import socket, select
import sys, struct
import time
import errno

DEBUG = False

dns_cache = {}		# {host : ip}
connecting = {}		# {fileno : context}
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
		#except Exception as ex:
		#	if DEBUG: print self.fileno, "catching unhandled exception"
		#	if DEBUG: print ex

	def send(self, sendval=None):
		"""A convenience method to advance a task (coroutine) to it's next state"""
		try:
			syscall = self.task.send(sendval)
			syscall(self)

		except StopIteration as ex:
			#self.close()
			pass

	def close(self):
		"""Convenience method to remove a task from the run loop"""
		if hasattr(self, 'fileno') and self.fileno in connections:
			close()(self)
	

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
		sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
		# add socket to list of pending connections
		# dont actually call connect() here, just enqueue it
		context.fileno = sock.fileno()
		context.sock = sock 
		#connecting[context.fileno] = context
		
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

		# connect but expect EINPROGRESS
		err = sock.connect_ex(context.address)
		if err != errno.EINPROGRESS:
			msg = "ConnectError [%s]" % (errno.errorcode[err])
			if DEBUG: print msg
			context.throw(ConnectError(msg))
		else:
			# register with epoll for write. 
			# when it has connected epoll will report a ready-to-write event
			connections[context.fileno] = context
			epoll.register(context.fileno, select.EPOLLOUT)



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
		except IOError as ex:
			#epoll.register(fileno, select.EPOLLIN | select.EPOLLET)
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
			#epoll.modify(fileno, select.EPOLLOUT | select.EPOLLET)
			epoll.modify(fileno, select.EPOLLOUT)
		except IOError as ex:
			#epoll.register(fileno, select.EPOLLOUT | select.EPOLLET)
			epoll.register(fileno, select.EPOLLOUT)
		finally:
			if DEBUG: print fileno, 'set epoll write'


class close:
	"""close system call"""
	def __call__(self, context):
		fileno = context.fileno
		context.sock.shutdown(socket.SHUT_RDWR)
		
		try:
			# set as 0 for level or EPOLLET for edge triggered
			#epoll.modify(fileno, select.EPOLLET)
			epoll.modify(fileno, 0)
		except IOError as ex:
			#epoll.register(fileno, select.EPOLLET) # may never be reached?
			epoll.register(fileno, 0) # may never be reached?
		finally:
			if DEBUG: print fileno, 'set epoll close'
	


def run(taskgen):
	"""
	The asyncronous loop. taskgen is a generator that produces tasks.
	"""
	done = False

	while connections or connecting or not done:

		if DEBUG: print "-- loop(%i) --" % len(connections)

		#### ADD TASK ####
		
		# connect new tasks if workload under max and tasks remain
		if not done and (len(connections)+len(connecting) < maxcons):
			try:
				# get task; prime coroutine; call syscall
				task = taskgen.next()
				context = Context(task)
				context.send(None)
			except StopIteration as ex:
				# taskgen.next() threw StopIteration
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
					elif not context.request:
						if DEBUG: print fileno, "connection successful"
						context.send(None)
					else:
						byteswritten = sock.send(context.request)
						if DEBUG: print fileno, "bytes written", byteswritten
						context.send(byteswritten)

				# connection closed
				if event & select.EPOLLHUP:
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
				
				connections.pop(fileno, None)
				epoll.unregister(fileno)
			
				# remote host closed connection
				if err == errno.ECONNRESET or err == errno.ENOTCONN:
					# pop but dont close an already closed connection
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

