from blackmamba import *
from urlparse import urlparse
import sys

class HttpGet:
	headers = [
		"GET %(path)s HTTP/1.1",
		"Host: %(host)s",
		"User-Agent: Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)",
		"\r\n" ]

	def __init__(self, url):
		# parse the URL into a form we can create a GET request with
		p = urlparse(url)
		self.host = p.hostname.strip()
		self.port = 443 if p.scheme == 'https' else 80
		self.port = p.port if p.port else self.port
		self.path = p.path + '?' + p.query if p.query else p.path
		self.path = '/' if not p.path else self.path

	def __str__(self):
		# populate all the header format strings and join everything together
		return '\r\n'.join([h % self.__dict__ for h in self.headers])


def get(url):

	try:
		# create the HTTP GET request from the URL
		request = HttpGet(url)

		# to resolve DNS asynchronously, call resolve() prior to connect()
		yield resolve(request.host)
		yield connect(request.host, request.port)
		yield write(str(request))
		response = yield read()
		
		# close the connection
		yield close()

	except SockError as e:
		print e

if __name__=='__main__':
	
	# to change the blackmamba configuration just set the config values
	config.maxcons = 5
	config.verbose = True

	# get a list of URLs from stdin
	print 'Reading list of URLs from stdin'
	print 'E.g. echo "http://rootfoo.org/" | python', sys.argv[0]
	lines = sys.stdin.readlines()

	# Create a generator. List comprehension syntax is nice
	taskgen = (get(url) for url in lines)
	
	# the debug() is a wrapper for run() which provides verbose error handling	
	#run(taskgen)
	debug(taskgen)


