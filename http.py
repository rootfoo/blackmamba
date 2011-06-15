from blackmamba import *
import sys, time

def save(response):
    import hashlib
    name = '/tmp/out/' + hashlib.sha1(response).hexdigest()
    if DEBUG: print "%s bytes read:" % len(response), name
    with open(name, 'wb') as fh: 
        fh.write(response)


stats = {}

def increment(name):
	stats[name] = stats.get(name,0) + 1

class HTTP(object):
	""" 
	An implimentation of the HTTP protocol as an example of using this library and coroutines
	"""

	def __init__(self, host, path='/', port=80):
		self.host = host
		self.port = port
		self.path = path
		self.message = "GET %s HTTP/1.0\r\nHost: %s\r\n\r\n" % (self.path, self.host)

	def run(self):
		""" 
		A coroutine to define the read/write interation with the target host.
		Every read/write must yield.
		"""
		try:
			yield connect(self.host, self.port, 5)
			yield write(self.message)
			response = yield read()
			#print '>', response[:15]
			yield close()
			increment('Completed')

		except ResetError as ex:
			increment('ResetError')

		except ConnectError as ex:
			increment('ConnectError')

		except EpollError as ex:
			increment('EpollError')

		except DomainError as ex:
			increment('DomainError')

		except SockError as ex:
			increment('SockError')

		except TimeoutError as ex:
			increment('TimeoutError')

def httpgen(host, count):

	for i in xrange(count):
		yield HTTP(host).run()


if __name__=='__main__':

	host = sys.argv[1]
	count = int(sys.argv[2])
	
	start = time.time()
	run(httpgen(host, count))
	end = time.time()

	print '\n-- statistics --\n'
	for k,v in stats.items():
		print '%s : %s' % (k,v)

	completed = stats['Completed']
	print "%i connections completed in %.3f seconds (%.3f per sec)\n" % (completed, end-start, completed/(end-start)) 


