import socket, select
import sys, struct
import time
from Queue import Queue
import errno

DEBUG = False

# system calls
WRITE = 0
READ = 1
CLOSE = 2

# globals
#max_concurrent = 100
#pending = [] # will be a generator later 

dns_cache = {}		# {host : ip}
connections = {}	# {fileno : (sock, task)}
connecting = {}		# {fileno : (sock, address, task)}
requests = {}		# {fileno : request}
responses = {}		# {fileno : response}
taskfile = {}		# {task : fileno}
timeouts = {}		# {fileno : timeout, start}

global bytes_read
global bytes_written

epoll = select.epoll()
tasks = Queue()
connect_count = 0
timeout_poll = 5 	# timeout pulling frequency in seconds
maxcons = 1000
bytes_read = 0
bytes_written = 0
global completed 
completed = 0
global errored 
errored = 0

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
			yield connect(self.host, self.port, 1000)
			yield write(self.message)
			response = yield read()
			yield close()
			global completed
			completed += 1
			#print '>', response[:15]

		except Exception as ex:
			#print '>>', ex
			global errored
			errored += 1

class connect:
	"""
	connect system call. 
	Queue the task for connection but doesn't actuall call connect() which blocks
	"""
	def __init__(self, host, port, timeout=5):
		self.host = host
		self.port = port
		self.timeout = timeout

	def __call__(self, task):
		""" Create a non-blocking socket and enqueue it for connection. """
		# resolve the hostname only if not in cache
		# TODO: DNS lookups block. Impliment the DNS protocol and do this concurrently as well. 
		try:
			if self.host in dns_cache:
				ip = dns_cache.setdefault(self.host)
			else:
				ip = socket.gethostbyname(self.host)
				dns_cache[self.host] = ip
			
			# connect a socket
			sock = socket.socket()
			# do not set sock.timeout() it modifies blocking behavior
			sock.setblocking(0)
			# absolutely do not settimeout()! This also set's blocking.
			# do not linger on close, kernel will try to gracefully close in background
			sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 0, 0))

			# add socket to list of pending connections
			address = (ip, self.port) 
			fileno = sock.fileno()
			connecting[fileno] = (sock, address, task)
			taskfile[task] = fileno
			timeouts[fileno] = (self.timeout, time.time())
			# we dont actually call connect() here, just queue it
		
		# except DNS errors
		except socket.gaierror as ex:
			global connect_count
			print "DNS lookup failed. %i connections made before fail.", connect_count
			print repr(self.host), dns_cache
			sys.exit(1)



def connect_ex(sock, address, task):
	"""try to connect a non-blocking socket. Handle expected errors"""
	
	# Expect socket.error: 115, 'Operation now in progress'
	try:
		#err = sock.connect_ex(address)
		sock.connect(address)
	
	except socket.error, socket.msg:
		(errorno, errmsg) = socket.msg.args
		
		# connected successfully
		if errorno == 0 or errorno == errno.EISCONN:
			if DEBUG: print "connection successful"
			fileno = sock.fileno()
			# move to active connections
			connections[fileno] = (sock, task)
			# remove from pending
			connecting.pop(fileno, None)
			# get and call next syscall to register with epoll
			syscall = task.send(None)
			syscall(task)
			
			# debug statistics
			global connect_count
			connect_count += 1

		# connection already in progress or would block
		elif errorno == errno.EINPROGRESS or errorno == errno.EWOULDBLOCK:
			if DEBUG: print "EINPROGRESS / EWOULDBLOCK"
			pass
	
		# previous connection attempt has not completed
		elif errorno == errno.EALREADY: 
			if DEBUG: print "EALREADY"
			pass

		# not sure what to do with this yet
		elif errorno == errno.ETIMEDOUT:
			fileno = sock.fileno()
			connecting.pop(fileno, None)
			timeouts.pop(fileno, None)
			msg = "%s %s - %s" % (errorno, errno.errorcode[errorno], errmsg)
			if DEBUG: print msg
			# then maybe throw to task
			task.throw(Exception(msg))

		else:
			msg = "%s %s - %s" % (errorno, errno.errorcode[errorno], errmsg)
			if DEBUG: print msg
			task.throw(Exception(msg))

"""		## Windows errors ###

		# Connection already made
		#elif errorno == errno.EISCONN:
		#	pass

		#elif hassattr(errno, 'WSAEINVAL') and errorno == errno.WSAEINVAL:
			# WSAEINVAL is windows equivelent of EALREADY
			#pass

		else:
			raise Exception("unhandled connection error: %s" % errmsg)
			sys.exit(-1)
"""
class read:
	"""read system call"""
	def __init__(self, nbytes=8196):
		self.nbytes = nbytes # to be used as max amount to read, diff from read block size
	
	def __call__(self, task):
		fileno = taskfile[task]
		try:
			epoll.modify(fileno, select.EPOLLIN)
		except IOError as ex:
			epoll.register(fileno, select.EPOLLIN)
			responses[fileno] = ''
		finally:
			if DEBUG: print fileno, 'set epoll read'

class write:
	"""write system call"""
	def __init__(self, data):
		self.data = data
	
	def __call__(self, task):
		fileno = taskfile[task]
		requests[fileno] = self.data
		try:
			epoll.modify(fileno, select.EPOLLOUT)
		except IOError as ex:
			epoll.register(fileno, select.EPOLLOUT)
		finally:
			if DEBUG: print fileno, 'set epoll write'
			
class close:
	"""close system call"""
	def __call__(self, task):
		fileno = taskfile[task]
		sock,task = connections[fileno]
		sock.shutdown(socket.SHUT_RDWR)
		try:
			epoll.modify(fileno, 0)
		except IOError as ex:
			epoll.register(fileno, 0) # may never be reached?
		finally:
			if DEBUG: print fileno, 'set epoll close'
	


def run():
	"""
	The asyncronous loop. 
	"""
	last_timeout_check = time.time()
	global bytes_read
	global bytes_written
	
	while connections or connecting or not tasks.empty():
	
		try:
			#### ADD TASK ####
			if DEBUG: print "--loop--"
			
			# connect new tasks if workload under max and tasks remain
			if len(connections)+len(connecting) < maxcons:
				if not tasks.empty():
					# replace with generator someday 
					task = tasks.get()	
					# prime the coroutine and get first syscall
					syscall = task.send(None)
					# now call syscall; should be "connect"
					syscall(task)

			#### CONNECT ####
			
			for sock,address,task in connecting.values():
				connect_ex(sock, address, task)

			#### READ, WRITE, CLOSE ####

			# get epoll events
			events = epoll.poll(1)
			for fileno, event in events:
				sock, task = connections[fileno]

				try:
					# read response
					if event & select.EPOLLIN:
						response = sock.recv(8192)
						bytes_read += len(response)
						if DEBUG: print fileno, "bytes read", len(response)
						
						# len zero read means EOF
						if len(response) == 0:

							# send response, get new opp
							syscall = task.send(responses.get(fileno, None))
							syscall(task)

						else:
							responses[fileno] = responses.get(fileno, '') +  response
							#if DEBUG: print fileno, ":", len(response), "bytes read"
					
					# send request
					elif event & select.EPOLLOUT:
						if DEBUG: print fileno, "bytes written", byteswritten
						byteswritten = sock.send(requests[fileno])
						bytes_written += byteswritten
						syscall = task.send(byteswritten)
						syscall(task)

					# connection closed
					elif event & select.EPOLLHUP:
						if DEBUG: print fileno, "connection closed"
						epoll.unregister(fileno)
						sock.close()
						connections.pop(fileno, None)
						timeouts.pop(fileno, None)
						# advance the coroutine
						syscall = task.send(None)
						syscall(task)
			
					# throw unhandled states to task
					else:
						# includes select.EPOLLERR
						task.throw(Exception('Unhandled EPOLL event [%s]' % event))
						sys.exit(1)

				# throw any socket/epoll exceptions not handled by other methods
				except socket.error, socket.msg:
					(errorno, errmsg) = socket.msg.args
					
					# remote host closed connection
					if errorno == errno.ECONNRESET or errorno == errno.ENOTCONN:
						connections.pop(fileno, None)
						epoll.unregister(fileno)
						timeouts.pop(fileno, None)
						# dont close already closed connections
					
					task.throw(Exception('socket.error [%s] %s' % (errorno, errmsg)))

				# end epoll loop

			#### TIMEOUT ####
		
			# check for stale connections periodically	
			now = time.time()
			if now - last_timeout_check > timeout_poll:
				last_timeout_check = now
				# check each connecting or connected object
				for fileno, (duration, start) in timeouts.items():
					if DEBUG: print fileno, "timeout check"
					if now - start > duration:
						if DEBUG: print fileno, "timeout, closing"
						# close connection
						sock, task = connections[fileno]
						task.throw(Exception('timeout'))
						close()(task)
			
		# coroutine exited without closing connection
		except StopIteration as ex:
			if DEBUG: print "StopIteration"
			
def save(response):
	import hashlib
	name = '/tmp/out/' + hashlib.sha1(response).hexdigest()
	if DEBUG: print "%s bytes read:" % len(response), name
	with open(name, 'wb') as fh:
		fh.write(response)


if __name__=='__main__':

	host = sys.argv[1]
	count = int(sys.argv[2])

	for i in xrange(count):
		tasks.put(HTTP(host).run())

	start = time.time()
	run()
	end = time.time()

	# print statistics
	sys.stderr.write('-- statistics --\n')
	mb = float(bytes_read + bytes_written) / pow(1024,2)
	sys.stderr.write("%i connections completed in %.2f seconds (%.2f per sec)\n" % (completed, end-start, completed/(end-start)) )
	#sys.stderr.write("%i bytes read, %i bytes written\n" % (bytes_read, bytes_written) )
	sys.stderr.write("%.4f MB data transferred (%.4f per sec)\n" % (mb, mb / (end-start)) )
	sys.stderr.write("%i connections errored\n" % errored)

"""
Notes


# select.PIPE_BUF
# 512 on posix, this is the number of bytes which epoll guarantees wont block for write/read.
# may be depreciated in python 2.6+

# there is a limit on the nmber of open connections (default 1024)
to check: ulimit -n
edit /etc/security/limits.conf
*                soft    nofile          65535
*                hard    nofile          65535
then restart the shell


"""

