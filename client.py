import socket, select
import sys, struct
import time
import errno

DEBUG = True

dns_cache = {}		# {host : ip}
connecting = {}		# {fileno : context}
connections = {}	# {fileno : context}

epoll = select.epoll()
maxcons = 2000

class Context:
	def __init__(self, task):
		"""
		A Context associates a task with a socket, request, response, ...
		task is the only required attribute. All others are set dynamically.
		"""
		self.task = task

	def throw(self, error):
		"""A convenience method to pass exceptions to a task"""
		try:
			self.task.throw(error)
		except StopIteration as ex:
			self.close()

	def send(self, sendval=None):
		"""A convenience method to advance a task (coroutine) to it's next state"""
		try:
			syscall = self.task.send(sendval)
			syscall(self)
			return True

		except StopIteration as ex:
			self.close()
			return False

	def close(self):
		"""Convenience method to remove a task from the run loop"""
		if hasattr(self, 'fileno'):
			connections.pop(self.fileno, None)
			connecting.pop(self.fileno, None)

	

class TimeoutError(Exception):
	pass

class ConnectError(Exception):
	pass

class DomainError(Exception):
	pass

class ResetError(Exception):
	pass
	
class SockError(Exception):
	pass

class EpollError(Exception):
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
		sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 0, 0))

		# add socket to list of pending connections
		# dont actually call connect() here, just enqueue it
		context.fileno = sock.fileno()
		context.sock = sock 
		connecting[context.fileno] = context
		
		# resolve the hostname only if not in cache
		# TODO: DNS lookups block. Impliment the DNS protocol and do this concurrently as well. 
		try:
			if self.host in dns_cache:
				ip = dns_cache.setdefault(self.host)
			else:
				ip = socket.gethostbyname(self.host)
				dns_cache[self.host] = ip
			
			# set context	
			context.address = (ip, self.port)
	
		# throw DNS errors
		except socket.gaierror as ex:
			context.throw(DomainError("DomainError [%s] %s" % (ex.args)))


def connect_ex(context):
	"""try to connect a non-blocking socket. Handle expected errors"""

	sock = context.sock
	fileno = context.fileno
	task = context.task

	# Expect socket.error: 115, 'Operation now in progress'
	err = sock.connect_ex(context.address)
	
	# connected successfully
	if err == 0 or err == errno.EISCONN:
		if DEBUG: print "connection successful"
		# move from connecting to connections
		connections[fileno] = context
		connecting.pop(fileno, None)
		# advance the coroutine	
		context.send(None)

	# connection already in progress or would block
	elif err == errno.EINPROGRESS or err == errno.EWOULDBLOCK:
		if DEBUG: print "EINPROGRESS / EWOULDBLOCK"
		pass

	# previous connection attempt has not completed
	elif err == errno.EALREADY: 
		if DEBUG: print "EALREADY"
		pass

	# not sure what to do with this yet
	elif err == errno.ETIMEDOUT:
		connecting.pop(fileno, None)
		msg = "ConnectError [%s] Timeout while connecting" % (errno.errorcode[err])
		if DEBUG: print msg
		context.throw(ConnectError(msg))

	### Windows errors ###
	#elif hassattr(errno, 'WSAEINVAL') and err == errno.WSAEINVAL:
		# WSAEINVAL is windows equivelent of EALREADY
		#pass

	else:
		msg = "ConnectError [%s] Error while connecting" % (errno.errorcode[err])
		if DEBUG: print msg
		context.throw(ConnectError(msg))
	


class read:
	"""read system call"""
	def __init__(self, nbytes=8196):
		self.nbytes = nbytes # to be used as max amount to read, diff from read block size
	
	def __call__(self, context):
		fileno = context.fileno
		context.response = ""

		try:
			epoll.modify(fileno, select.EPOLLIN)
		except IOError as ex:
			epoll.register(fileno, select.EPOLLIN)
			#responses[fileno] = ''
		finally:
			if DEBUG: print fileno, 'set epoll read'

class write:
	"""write system call"""
	def __init__(self, data):
		self.data = data
	
	def __call__(self, context):
		fileno = context.fileno
		context.request = self.data
		
		try:
			epoll.modify(fileno, select.EPOLLOUT)
		except IOError as ex:
			epoll.register(fileno, select.EPOLLOUT)
		finally:
			if DEBUG: print fileno, 'set epoll write'
			
class close:
	"""close system call"""
	def __call__(self, context):
		fileno = context.fileno
		context.sock.shutdown(socket.SHUT_RDWR)
		
		try:
			epoll.modify(fileno, 0)
		except IOError as ex:
			epoll.register(fileno, 0) # may never be reached?
		finally:
			if DEBUG: print fileno, 'set epoll close'
	


def run(taskgen):
	"""
	The asyncronous loop. taskgen is a generator that produces tasks.
	"""
	done = False

	while connections or connecting or not done:

		if DEBUG: print "--loop--"
		
		#### ADD TASK ####
		
		# connect new tasks if workload under max and tasks remain
		if not done and (len(connections)+len(connecting) < maxcons):
			# get task; prime coroutine; call syscall
			try:	
				task = taskgen.next()
				context = Context(task)
				context.send(None)
			except StopIteration as ex:
				# taskgen.next() threw StopIteration
				done = True

		#### CONNECT ####
		
		for context in connecting.values():
			connect_ex(context)

		#### READ, WRITE, CLOSE ####

		# get epoll events
		events = epoll.poll(1)
		for fileno, event in events:
			
			if fileno not in connections:
				print "fileno %s not in connections. epoll event %s" % (fileno, event)

			# get context
			context = connections[fileno]
			sock = context.sock
			task = context.task

			try:
				# read response
				if event & select.EPOLLIN:
					response = sock.recv(8192)
					if DEBUG: print fileno, "bytes read", len(response)
					
					# len zero read means EOF
					if len(response) == 0:
						# send response, get new opp
						context.send(context.response)

					else:
						context.response += response
				
				# send request
				elif event & select.EPOLLOUT:
					byteswritten = sock.send(context.request)
					if DEBUG: print fileno, "bytes written", byteswritten
					context.send(byteswritten)

				# connection closed
				elif event & select.EPOLLHUP:
					if DEBUG: print fileno, "connection closed"
					epoll.unregister(fileno)
					sock.close()
					connections.pop(fileno, None)
					# advance the coroutine
					context.send(None)
		
				# throw unhandled states to task
				else:
					# includes select.EPOLLERR
					context.throw(EpollError("EpollError [%s]" % event))
					sys.exit(1)

			# throw any socket/epoll exceptions not handled by other methods
			except socket.error, socket.msg:
				(err, errmsg) = socket.msg.args
				
				# remote host closed connection
				if err == errno.ECONNRESET or err == errno.ENOTCONN:
					# pop but dont close an already closed connection
					connections.pop(fileno, None)
					epoll.unregister(fileno)
					context.throw(ResetError("ResetError [%s] %s" % (err, errmsg)))
				else:
					context.throw(SockError("SockError [%s] %s" % (err, errmsg)))


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

