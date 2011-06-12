import socket, select
import sys, struct
import time
from Queue import Queue
import errno

DEBUG = True

dns_cache = {}		# {host : ip}
connecting = {}		# {fileno : context}
connections = {}	# {fileno : context}
tasks = {}			# {task : context}

epoll = select.epoll()
timeout_poll = 5 	# timeout pulling frequency in seconds
maxcons = 1000

class Context:
	def __init__(self, task, sock, address, timeout):
		self.task = task
		self.sock = sock
		self.fileno = sock.fileno()
		self.address = address
		self.timeout = timeout
		self.last = time.time()
		self.request = None
		self.response = ""



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
			fileno = sock.fileno()
			address = (ip, self.port)
			context = Context(task, sock, address, self.timeout)
			connecting[fileno] = context
			tasks[task] = context

			#address = (ip, self.port) 
			#fileno = sock.fileno()
			#connecting[fileno] = (sock, address, task)
			#taskfile[task] = fileno
			#timeouts[fileno] = (self.timeout, time.time())
			# we dont actually call connect() here, just queue it
		
		# except DNS errors
		except socket.gaierror as ex:
			print "DNS lookup failed." 
			print repr(self.host), dns_cache
			sys.exit(1)



def connect_ex(context):
	"""try to connect a non-blocking socket. Handle expected errors"""

	sock = context.sock
	fileno = context.fileno
	task = context.task

	# Expect socket.error: 115, 'Operation now in progress'
	try:
		#sock.connect(context.address)
		err = sock.connect_ex(context.address)

	#except socket.error, socket.msg:
	#	(err, errmsg) = socket.msg.args

		# connected successfully
		if err == 0 or err == errno.EISCONN:
			if DEBUG: print "connection successful"
			# move from connecting to connections
			connections[fileno] = context
			connecting.pop(fileno, None)
			
			# get and call next syscall to register with epoll
			syscall = task.send(None)
			syscall(task)

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
			code = errno.errorcode[err]
			msg = "%s %s - %s" % (err, code, "Timeout while connecting")
			if DEBUG: print msg
			task.throw(Exception(msg))

		else:
			code = errno.errorcode[err]
			msg = "%s %s - %s" % (err, code, "Error while connecting")
			if DEBUG: print msg
			task.throw(Exception(msg))
	
	except StopIteration:
		connections.pop(fileno, None)
		connecting.pop(fileno, None)
		tasks.pop(task, None)

"""		## Windows errors ###

		# Connection already made
		#elif err == errno.EISCONN:
		#	pass

		#elif hassattr(errno, 'WSAEINVAL') and err == errno.WSAEINVAL:
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
		context = tasks[task]
		fileno = context.fileno
		
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
	
	def __call__(self, task):
		context = tasks[task]
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
	def __call__(self, task):
		context = tasks[task]
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
			try:
				# get task; prime coroutine; call syscall
				task = taskgen.next()
				syscall = task.send(None)
				syscall(task)

			except StopIteration:
				# task didnt yield syscall
				done = True

		#### CONNECT ####
		
		for context in connecting.values():
			connect_ex(context)

		#### READ, WRITE, CLOSE ####

		# get epoll events
		events = epoll.poll(1)
		for fileno, event in events:
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
						syscall = task.send(context.response)
						syscall(task)

					else:
						context.response += response
				
				# send request
				elif event & select.EPOLLOUT:
					byteswritten = sock.send(context.request)
					if DEBUG: print fileno, "bytes written", byteswritten
					syscall = task.send(byteswritten)
					syscall(task)

				# connection closed
				elif event & select.EPOLLHUP:
					if DEBUG: print fileno, "connection closed"
					epoll.unregister(fileno)
					sock.close()
					connections.pop(fileno, None)
					#timeouts.pop(fileno, None)
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
				(err, errmsg) = socket.msg.args
				
				# remote host closed connection
				if err == errno.ECONNRESET or err == errno.ENOTCONN:
					connections.pop(fileno, None)
					epoll.unregister(fileno)
					#timeouts.pop(fileno, None)
					# dont close already closed connections
				
				task.throw(Exception('socket.error [%s] %s' % (err, errmsg)))
			
			# coroutine exited without closing connection
			except StopIteration as ex:
				if DEBUG: print "StopIteration, removing task"
				tasks.pop(task, None)


			# end epoll loop

			#### TIMEOUT ####
		
			# check for stale connections periodically	
			#now = time.time()
			#if now - last_timeout_check > timeout_poll:
				#last_timeout_check = now
				# check each connecting or connected object
				#for fileno, (duration, start) in timeouts.items():
					#if DEBUG: print fileno, "timeout check"
					#if now - start > duration:
						#if DEBUG: print fileno, "timeout, closing"
						# close connection
						#sock, task = connections[fileno]
						#task.throw(Exception('timeout'))
						#close()(task)
		


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

