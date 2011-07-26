import socket, select
import sys, struct
import time
import errno
import ssl

VERBOSE = False

dns_cache = {}		# {host : ip}
connections = {}	# {fileno : context}
statistics = {}		# {Error : count}
epoll = select.epoll()
maxcons = 1000
global current
current = None


class Context:
	def __init__(self, task):
		"""
		A Context associates a task with a socket, request, response, ...
		task is the only required attribute. All others are set dynamically.
		"""
		self.task = task
		self.request = None
		self.tracelog = []

	def throw(self, error):
		"""A convenience method to pass exceptions to a task"""
		try:
			self.log(error)
			self.task.throw(error)

		except StopIteration as ex:
			self.log('StopIteration')
			pass
		
		except ConnectionError as ex: 
			# task didnt except error, add to statistics and ignore
			name = ex.__class__.__name__
			statistics[name] = statistics.get(name,0) + 1

	def send(self, sendval=None):
		"""A convenience method to advance a task (coroutine) to its next state"""
		try:
			syscall = self.task.send(sendval)
			syscall(self)

		except StopIteration as ex:
			name = "Completed"
			statistics[name] = statistics.get(name,0) + 1
			self.log(name)

	def log(self, msg):
		"""method to log debugging messages associated with a fileno"""
		self.tracelog.append(msg)
		if VERBOSE: print "[%i] %s" % (self.fileno, msg)

class ConnectionError(Exception):
	pass

class TimeoutError(ConnectionError):
	pass

class ConnectError(ConnectionError):
	pass

class DomainError(ConnectionError):
	pass

class ResetError(ConnectionError):
	pass
	
class SockError(ConnectionError):
	pass

class ClosedError(ConnectionError):
	pass

class EpollError(ConnectionError):
	pass


class connect:
	"""
	connect system call. 
	Queue the task for connection but doesn't actuall call connect() which blocks
	"""
	def __init__(self, host, port, timeout=30, ssl=False):
		self.host = host
		self.port = port
		self.timeout = timeout
		self.ssl = ssl

	def __call__(self, context):
		""" Create a non-blocking socket and enqueue it for connection. """
		# do not settimeout() it also set's blocking.
		sock = socket.socket()

		if self.ssl:
			sock = ssl.wrap_socket(s)
		
		sock.setblocking(0)
		# do not linger on close, kernel will try to gracefully close in background
		#sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 0, 0))
		sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
		# add socket to list of pending connections
		# don't actually call connect() here, just enqueue it
		context.fileno = sock.fileno()
		context.sock = sock 
		context.timeout = self.timeout
		context.atime = time.time()

		# resolve the hostname only if not in cache
		# TODO: DNS lookups block. Implement the DNS protocol and do this concurrently as well. 
		try:
			if self.host in dns_cache:
				ip = dns_cache.setdefault(self.host)
			else:
				ip = socket.gethostbyname(self.host)
				dns_cache[self.host] = ip
			
			# connect but expect EINPROGRESS
			context.address = (ip, self.port)
			err = sock.connect_ex(context.address)
			
			# for debugging
			context.log("connecting... [%s]" % errno.errorcode[err])

			if err != errno.EINPROGRESS:
				msg = "ConnectError [%s]" % (errno.errorcode[err])
				context.throw(ConnectError(msg))
			else:
				# register with epoll for write. 
				# when it has connected epoll will report a ready-to-write event
				connections[context.fileno] = context
				epoll.register(context.fileno, select.EPOLLOUT)
	
		# throw DNS errors
		except socket.gaierror as ex:
			msg = "DomainError [%s] %s" % (ex.args)
			context.throw(DomainError(msg))


class read:
	"""read system call"""
	def __init__(self):
		pass

	def __call__(self, context):
		context.response = ""
		
		try:
			context.log('read, setting epoll for EPOLLIN')
			#epoll.modify(fileno, select.EPOLLIN | select.EPOLLET)
			epoll.modify(context.fileno, select.EPOLLIN)
		
		except IOError as ex:
			msg = "ClosedError: Read failed; socket already closed"
			context.throw(ClosedError(msg)) 


class write:
	"""write system call"""
	def __init__(self, data):
		self.data = data
	
	def __call__(self, context):
		context.request = self.data
		
		try:
			context.log('write, seting epoll for EPOLLOUT')
			#epoll.modify(fileno, select.EPOLLOUT | select.EPOLLET)
			epoll.modify(context.fileno, select.EPOLLOUT)

		except IOError as ex:
			msg = "ClosedError: Write failed; Socket already closed"
			context.throw(ClosedError(msg)) 
		


class close:
	"""close system call"""
	def __call__(self, context):
		
		try:
			context.log("close, setting epoll to 0")
			context.sock.shutdown(socket.SHUT_RDWR)
			# set as 0 for level or EPOLLET for edge triggered
			#epoll.modify(fileno, select.EPOLLET)
			epoll.modify(context.fileno, 0)
		
		except socket.error, socket.msg:
			(err, errmsg) = socket.msg.args
			msg = "ConnectionError during close [%s] %s" % (err, errmsg)
			context.throw(ConnectionError(msg))



def run(taskgen):
	"""
	The asyncronous loop. taskgen is a generator that produces tasks.
	"""
	done = False

	while connections or not done:

		if VERBOSE: print "-- loop(%i) --" % len(connections)

		#### ADD TASK ####
		
		# connect new tasks if workload under max and tasks remain
		while not done and len(connections) < maxcons:
			try:
				# get task; prime coroutine; call syscall
				task = taskgen.next()
				context = Context(task)
				context.send(None)
			except StopIteration as ex:
				# taskgen.next() threw StopIteration (not context.send)
				done = True

		#### READ, WRITE, CLOSE ####

		# get epoll events
		events = epoll.poll(1)
		for fileno, event in events:
			
			if fileno not in connections:
				print "fileno %s not in connections. epoll event %x" % (fileno, event)
				sys.exit(1)

			# get context
			context = connections[fileno]
			sock = context.sock
			task = context.task
			context.atime = time.time()
			global current
			current = context
			
			try:
		
				# read response
				if event & select.EPOLLIN:
					blocksize = 8096
					response = sock.recv(blocksize)
					context.log("%i bytes read" % len(response))
					
					context.response += response

					# len zero read means EOF
					if len(response) < blocksize:
						# send response, get new opp
						context.send(context.response)
				
				# send request
				elif event & select.EPOLLOUT:
					# first check that connect() completed successfully
					err = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
					context.log('EPOLLOUT')
					if err:
						context.throw(ConnectError("ConnectError [%s] connection failed" % errno.errorcode[err]))
						# epoll automatically modifies failed connections to EPOLLHUP; no need to epoll.unregister here
					elif not context.request:
						context.log("connection successful")
						context.send(None)
					else:
						byteswritten = sock.send(context.request)
						context.log("%i bytes written" % byteswritten)
						context.send(byteswritten)

				# connection closed
				if event & select.EPOLLHUP:
					context.log("EPOLLHUP, connection closed")
					
					epoll.unregister(fileno)
					connections.pop(fileno, None)
					sock.close()
					# advance the coroutine
					context.send(None)
	
				if event & select.EPOLLERR:
					context.throw(EpollError("EpollError %s" % event))
				#	sys.exit(1)

			# throw any socket/epoll exceptions not handled by other methods
			except socket.error, socket.msg:
				(err, errmsg) = socket.msg.args
				context.log("socket.error %s %s" % (err, errmsg))	
				connections.pop(fileno, None)
				epoll.unregister(fileno)
			
				# remote host closed connection
				if err == errno.ECONNRESET or err == errno.ENOTCONN:
					# pop but dont close an already closed connection
					context.throw(ResetError("ResetError [%s] %s" % (err, errmsg)))
				else:
					context.throw(SockError("SockError [%s] %s" % (err, errmsg)))
			
				# advance to keep statistics accurate (throws StopIteration on purpose)
				context.send(None)


		# for-else epoll loop had no events. Let's check connection states.
		else:
			now = time.time()
			for fileno,context in connections.items():
				
				delta = now - context.atime
				err = context.sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
				
				if err:
					context.log("connection state %s" % errno.errorcode[err])
				
				elif delta > context.timeout:
					msg = "TimeoutError no connection activity for %.3f seconds" % delta
					close()(context)
					context.throw(TimeoutError(msg))


def debug(taskgen):
	"""A debugging wrapper for run()"""
	
	start = time.time()
	
	try:
		run(taskgen)

	except Exception as ex:
	
		print '\n-- context --\n'
		for t in current.tracelog:
			print t

		print '\n-- trace --\n'
		# also print stack trace
		import traceback
		traceback.print_exc()
		print ex

	finally:
		end = time.time()
		print '\n-- statistics --\n'
		for k,v in statistics.items():
			print '%s : %s' % (k,v)
		
		completed = statistics.get('Completed',0)
		print "%i connections completed in %.3f seconds (%.3f per sec)\n" % (completed, end-start, completed/(end-start)) 




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

# to limit the amount of kernel memory used by epoll set
/proc/sys/fs/epoll/max_user_watches


"""

