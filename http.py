from blackmamba import *
from foolib.parse import *
import sys


class HTTP(object):
	""" 
	A simple object for creating HTTP reqeusts.

		http = HTTP('example.com', '/login.html', 'POST', 80)
		http.cookies['settings'] = 'XXX'
		http.headers['User-Agent'] = 'Mozilla'
		http.data = "user=alice&pass=insecuredefault"
		print http
	"""

	def __init__(self, host, path='/', verb='GET', port=80):
		self.host = host
		self.port = port
		self.path = path
		self.verb = verb
		self.headers = {}
		self.cookies = {}
		self.data = ""

	def __str__(self):
		self.headers['Host'] = self.host
		if self.cookies:
			self.headers['Cookie'] = djoin(self.cookies, '; ', '=')
		if self.data:
			self.headers['Content-Length'] = str(len(self.data))
		request = []
		request.append("%s %s HTTP/1.1" % (self.verb, self.path))
		request.append(djoin(self.headers, '\r\n', ': '))
		request.append("")
		request.append(self.data)
		return "\r\n".join(request)
		
	
	def run(self):
		"""Coroutine to send the request and get response, then call handle()."""
		yield connect(self.host, self.port, 20)
		yield write(self.__str__())
		response = yield read()
		yield close()
		self.handle(response)

	def handle(self, response):
		"""Virtual method to handle the response."""
		pass

def httpgen(host, count):

	for i in xrange(count):
		yield HTTP(host).run()


if __name__=='__main__':

	host = sys.argv[1]
	count = int(sys.argv[2])

	#run(httpgen(host, count))
	debug(httpgen(host, count))

