import socket, select
import sys, struct
import time
import errno
import ssl
from Queue import Queue
import adns

VERBOSE = True

dns_cache = {}		# {host : ip}
adns_queries = {}	# {query : context}
connections = {}	# {fileno : context}
statistics = {}		# {Error : count}
epoll = select.epoll()
maxcons = 1000
global current
current = None
queue = Queue()
timers = []


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

class EpollError(Exception):
	pass

class Timer(Exception):
	pass

resolver = adns.init(adns.iflags.noautosys)


class Context:
	def __init__(self, task):
		"""
		A Context associates a task with a socket, request, response, ...
		self.task is the only required attribute. All others are set dynamically.
		"""
		self.task = task
		self.request = None
		self.tracelog = []

	def throw(self, error):
		"""A convenience method to pass exceptions to a task"""
		try:
			self.log(error)
			syscall = self.task.throw(error)
		
			if isinstance(syscall, timer):
				syscall(self) # <-- this may be buggy! added for timer support
				# always advance timer syscalls to prevent the next throw from landing in an except clause
				self.task.send(None)
		
		except Timer as ex:
			self.log('Timer thrown to except clause; putting back in queue')
			timers.append(self)

		except StopIteration as ex:
			self.log('StopIteration caught after context.throw()')

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

	def log(self, msg, sockerror=None):
		"""method to log debugging messages associated with a fileno"""
		if sockerror:
			msg += " (Socket Error [%s] %s)" % (sockerror, errno.errorcode[sockerror])
		self.tracelog.append(msg)
		if VERBOSE: print "[%i] %s" % (self.fileno, msg)


class resolve:
	"""
	resolve system call.
	"""
	def __init__(self, host, record_type = adns.rr.A):
		self.host = host
		self.record_type = record_type
	
	def __call__(self, context):
		# be sure to keep a reference to the query, or adns-python gets very upset
		query = resolver.submit(self.host, self.record_type, 0)
		adns_queries[query] = context
		context.adns_host = self.host
		context.adns_record_type = self.record_type

class syncresolve:
	"""
	synchronous resolve system call.
	"""
	def __init__(self, host):
		self.host = host
	
	def __call__(self, context):
		try:
			ip = socket.gethostbyname(self.host)
			dns_cache[self.host] = ip
		# throw DNS errors
		except socket.gaierror as ex:
			msg = "DomainError [%s] %s" % (ex.args)
			context.throw(DomainError(msg))

class connect:
	"""
	Connect system call. Lookup hostname, connect non-blocking socket, and create a context.
	"""
	def __init__(self, host, port, timeout=30, ssl=False):
		self.host = host
		self.port = port
		self.timeout = timeout
		self.ssl = ssl

	def __call__(self, context):
		""" Create a non-blocking socket and enqueue it for connection. """
		# do not settimeout(); it also sets blocking.
		sock = socket.socket()

		# nee to use openssl probably
		#if self.ssl:
		#	sock = ssl.wrap_socket(s)
		
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
				context.log("DNS cache hit: [%s]" % self.host)
				ip = dns_cache.setdefault(self.host)
			else:
				context.log("DNS cache miss: [%s]" % self.host)
				ip = socket.gethostbyname(self.host)
				dns_cache[self.host] = ip
			
			# connect but expect EINPROGRESS
			context.address = (ip, self.port)
			err = sock.connect_ex(context.address)
			
			# for debugging
			context.log("Connecting [%s]" % errno.errorcode[err])

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

class timer:
	"""system call to throw an exception on a timer"""
	def __init__(self, duration):
		self.event_timeout = time.time() + duration

	def __call__(self, context):
		context.log("timer syscall")
		context.event_timeout = self.event_timeout 
		timers.append(context)


def add(task):
	"""convenience method to add task objects without a generator"""
	queue.put(task)
	print "adding task (%i)" % queue.qsize()


def run(taskgen):
	"""
	The asyncronous loop. taskgen is a generator that produces tasks.
	"""
	done = False

	while connections or resolver.allqueries() or not done:

		# use a blank line to separate messages in each loop
		if VERBOSE: print ""

		#### ADD TASK ####
		
		# we are no longer done if tasks were added to the queue at runtime
		qdone = queue.empty()

		# connect new tasks if workload under max and tasks remain
		#while not (done and qdone) and len(connections) < maxcons:
		while not done and len(connections) + len(resolver.allqueries()) < maxcons:
			try:
				# get task from queue first
				if not queue.empty():
					print "getting task from queue"
					task = queue.get()
					qdone = queue.empty()
				# get task from generator
				else:
					task = taskgen.next()
				# prime coroutine; call syscall
				context = Context(task)
				context.send(None)

			except StopIteration as ex:
				# taskgen.next() threw StopIteration (not context.send)
				done = True

		#### DNS ####

		# get adns events
		for adns_query in resolver.completed(0):
			context = adns_queries[adns_query]
			response = adns_query.check()

			host = context.adns_host
			record_type = context.adns_record_type
			del context.adns_host
			del context.adns_record_type
			del adns_queries[adns_query]
			if response[3]:
				if record_type == adns.rr.A:
					dns_cache[host] = response[3][0]
				context.send(response)
			else:
				context.throw(DomainError('adns unable to resolve: %s' % host))

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
					err = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
					context.log('EPOLLIN', err)
					blocksize = 8096
					response = sock.recv(blocksize)
					context.log("%i bytes read" % len(response))
					
					context.response += response

					# less than blocksize means EOF
					if len(response) < blocksize:
						if len(context.response) > 0:
							# send response, get new opp
							context.send(context.response)
				
				# send request
				elif event & select.EPOLLOUT:
					err = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
					context.log('EPOLLOUT', err)
					# first check that connect() completed successfully
					if err:
						# epoll automatically modifies failed connections to EPOLLHUP; no need to epoll.unregister here
						context.throw(ConnectError("ConnectError [%s] connection failed" % errno.errorcode[err]))
					# when connections succeed epoll uses EPOLLOUT. If nothing to send, just advance the coroutine
					elif not context.request:
						context.log("connection successful")
						context.send(None)
					# connection succeeded and there is data to write
					else:
						byteswritten = sock.send(context.request)
						context.log("%i bytes written" % byteswritten)
						context.send(byteswritten)

				# connection closed
				if event & select.EPOLLHUP:
					err = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
					context.log('EPOLLHUP', err)
					
					epoll.unregister(fileno)
					connections.pop(fileno, None)
					#sock.close()
					
					# advance the coroutine
					context.send(None)
			
					# remote host closed connection
					if err == errno.ECONNRESET or err == errno.ENOTCONN:
						context.throw(ResetError("ResetError [%s] %s" % (err, errno.errorcode[err])))
		
				#if event & select.EPOLLERR:
				#	context.throw(EpollError("EpollError %s" % event))
					#sys.exit(1)

			# throw any socket/epoll exceptions not handled by other methods
			except socket.error, socket.msg:
				(err, errmsg) = socket.msg.args
				context.log("socket.error [%s] caught in run(): %s" % (err, errmsg))	
				# pop but dont close an already closed connection
				connections.pop(fileno, None)
				epoll.unregister(fileno)
			
				# remote host closed connection
				if err == errno.ECONNRESET or err == errno.ENOTCONN:
					context.throw(ResetError("ResetError [%s] %s" % (err, errmsg)))
				else:
					context.throw(SockError("SockError [%s] %s" % (err, errmsg)))
			
				# advance to keep statistics accurate (throws StopIteration on purpose)
				context.send(None)


		# for-else: epoll loop had no events. Let's check connection states.
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

		#### EVENT TIMERS ####
		now = time.time()
		for context in filter(lambda c: now > c.event_timeout, timers):
			timers.remove(context)
			context.throw(Timer())

def debug(taskgen):
	"""A debugging wrapper for run()"""
	import traceback
	start = time.time()
	
	try:
		run(taskgen)

	except Exception as ex:
	
		print '\n-- context --\n'
		if current:
			for t in current.tracelog:
				print t

		# also print stack trace
		print '\n-- trace --\n'
		traceback.print_exc()
		print ex

	finally:
		end = time.time()
		print '\n-- statistics --\n'
		for k,v in statistics.items():
			print '%s : %s' % (k,v)
		
		completed = statistics.get('Completed',0)
		print "%i task completed in %.3f seconds (%.3f per sec)\n" % (completed, end-start, completed/(end-start)) 




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

