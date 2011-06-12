from client import connect, read, write, close, run
import sys

def save(response):
    import hashlib
    name = '/tmp/out/' + hashlib.sha1(response).hexdigest()
    if DEBUG: print "%s bytes read:" % len(response), name
    with open(name, 'wb') as fh: 
        fh.write(response)



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
			yield close()
			print '>', response[:15]

		except Exception as ex: 
			print '>>', ex


def httpgen(host, count):

	for i in xrange(count):
		yield HTTP(host).run()


if __name__=='__main__':

	host = sys.argv[1]
	count = int(sys.argv[2])

	run(httpgen(host, count))

