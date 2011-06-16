import socket, select
import sys, struct
import time
import errno

DEBUG = True

dns_cache = {}		# {host : ip}
connections = {}	# {fileno : context}
epoll = select.epoll()
maxcons = 1000

class Context:
	def __init__(self, task):
		"""
		A Context associates a task with a socket, request, response, ...
		task is the only required attribute. All others are set dynamically.
		"""
		self.task = task
		self.request = None

	def throw(self, error):
		"""A convenience method to pass exceptions to a task"""
		try:
			self.task.throw(error)
		except StopIteration as ex:
			pass


	def send(self, sendval=None):
		"""A convenience method to advance a task (coroutine) to it's next state"""
		try:
			syscall = self.task.send(sendval)
			syscall(self)

		except StopIteration as ex:
			pass



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


class connect:
	"""
	connect system call. 
	Queue the task for connection but doesn't actuall call connect() which blocks
	"""
	def __init__(self, host, port, timeout=5):
		self.host = host
		self.port = port
		self.timeout = timeout

	def __call__(self, context):
		""" Create a non-blocking socket and enqueue it for connection. """
		# do not set sock.timeout() it modifies blocking behavior
		# absolutely do not settimeout()! This also set's blocking.
		sock = socket.socket()
		sock.setblocking(0)
		# do not linger on close, kernel will try to gracefully close in background
		#sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 0, 0))
		sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
		# add socket to list of pending connections
		# dont actually call connect() here, just enqueue it
		context.fileno = sock.fileno()
		context.sock = sock 
		context.timeout = self.timeout
		context.atime = time.time()

		# resolve the hostname only if not in cache
		# TODO: DNS lookups block. Impliment the DNS protocol and do this concurrently as well. 
		try:
			if self.host in dns_cache:
				ip = dns_cache.setdefault(self.host)
			else:
				ip = socket.gethostbyname(self.host)
				dns_cache[self.host] = ip
			
			# connect but expect EINPROGRESS
			context.address = (ip, self.port)
			err = sock.connect_ex(context.address)
			msg = "connecting... [%s]" % errno.errorcode[err]
			if DEBUG: print context.fileno, msg
			
			if err != errno.EINPROGRESS:
				msg = "ConnectError [%s]" % (errno.errorcode[err])
				if DEBUG: print context.fileno, msg
				context.throw(ConnectError(msg))
			else:
				# register with epoll for write. 
				# when it has connected epoll will report a ready-to-write event
				connections[context.fileno] = context
				epoll.register(context.fileno, select.EPOLLOUT)
	
		# throw DNS errors
		except socket.gaierror as ex:
			msg = "DomainError [%s] %s" % (ex.args)
			if DEBUG: print context.fileno, msg
			context.throw(DomainError(msg))


class read:
	"""read system call"""
	def __init__(self):
		pass

	def __call__(self, context):
		fileno = context.fileno
		context.response = ""

		try:
			#epoll.modify(fileno, select.EPOLLIN | select.EPOLLET)
			epoll.modify(fileno, select.EPOLLIN)
			msg = 'set epoll read'

		except IOError as ex:
			msg = "ClosedError: Read failed; socket already closed"
			context.throw(ClosedError(msg)) 

		finally:
			if DEBUG: print fileno, msg


class write:
	"""write system call"""
	def __init__(self, data):
		self.data = data
	
	def __call__(self, context):
		fileno = context.fileno
		context.request = self.data
		
		try:
			#epoll.modify(fileno, select.EPOLLOUT | select.EPOLLET)
			epoll.modify(fileno, select.EPOLLOUT)
			msg = 'set epoll write'

		except IOError as ex:
			msg = "ClosedError: Write failed; Socket already closed"
			context.throw(ClosedError(msg)) 
		
		finally:
			if DEBUG: print fileno, msg


class close:
	"""close system call"""
	def __call__(self, context):
		fileno = context.fileno
		context.sock.shutdown(socket.SHUT_RDWR)
		
		try:
			# set as 0 for level or EPOLLET for edge triggered
			#epoll.modify(fileno, select.EPOLLET)
			epoll.modify(fileno, 0)
			msg = "set epoll close"
		
		except IOError as ex:
			#epoll.register(fileno, select.EPOLLET) # may never be reached?
			#epoll.register(fileno, 0) # may never be reached?
			msg = "ConnectionError: IOError while closing"
			context.throw(ConnectionError(msg))
		
		finally:
			if DEBUG: print fileno, msg
	


def run(taskgen):
	"""
	The asyncronous loop. taskgen is a generator that produces tasks.
	"""
	done = False

	while connections or not done:

		if DEBUG: print "-- loop(%i) --" % len(connections)

		#### ADD TASK ####
		
		# connect new tasks if workload under max and tasks remain
		if not done and len(connections) < maxcons:
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
		events = epoll.poll(2)
		for fileno, event in events:
			
			if fileno not in connections:
				print "fileno %s not in connections. epoll event %x" % (fileno, event)

			# get context
			context = connections[fileno]
			sock = context.sock
			task = context.task
			context.atime = time.time()
			
			try:
		
				# read response
				if event & select.EPOLLIN:
					response = sock.recv(8096)
					if DEBUG: print fileno, "bytes read", len(response)
					
					# len zero read means EOF
					if len(response) == 0:
						# send response, get new opp
						context.send(context.response)
					else:
						context.response += response
				
				# send request
				elif event & select.EPOLLOUT:
					# first check that connect() completed successfully
					err = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
					if err:
						if DEBUG: print fileno, errno.errorcode[err], "EPOLLOUT"
						context.throw(ConnectError("ConnectError [%s] connection failed" % errno.errorcode[err]))
						# epoll automatically modifies failed connections to EPOLLHUP; no need to unregister here
						#connections.pop(fileno, None)
						epoll.unregister(fileno)
					elif not context.request:
						if DEBUG: print fileno, "connection successful"
						context.send(None)
					else:
						byteswritten = sock.send(context.request)
						if DEBUG: print fileno, "bytes written", byteswritten
						context.send(byteswritten)

				# connection closed
				elif event & select.EPOLLHUP:
					if DEBUG: print fileno, "EPOLLHUP, connection closed"
					
					epoll.unregister(fileno)
					connections.pop(fileno, None)
					sock.close()
					# advance the coroutine
					context.send(None)
	
				# throw unhandled states to task
				#else:
					# includes select.EPOLLERR
				#	context.throw(EpollError("EpollError [%s]" % event))
				#	sys.exit(1)

			# throw any socket/epoll exceptions not handled by other methods
			except socket.error, socket.msg:
				(err, errmsg) = socket.msg.args

				if DEBUG: print fileno, err, errmsg
				connections.pop(fileno, None)
				epoll.unregister(fileno)
			
				# remote host closed connection
				if err == errno.ECONNRESET or err == errno.ENOTCONN:
					# pop but dont close an already closed connection
					context.throw(ResetError("ResetError [%s] %s" % (err, errmsg)))
				else:
					context.throw(SockError("SockError [%s] %s" % (err, errmsg)))

		# for/else
		# else epoll for-loop had no events. Let's check connection states.
		else:
			now = time.time()
			for fileno,context in connections.items():
				delta = now - context.atime
				if delta > context.timeout:
					msg = "TimeoutError no connection activity for %.3f seconds" % delta
					if DEBUG: print fileno, msg
					close()(context)
					context.throw(TimeoutError(msg))

				if DEBUG:
					err = context.sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
					if err:
						print fileno, "connection state", errno.errorcode[err]
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

