Blackmamba

Introduction
------------

Blackmamba is a new concurrent networking library for Python. Blackmamba was built from the ground up leveraging the power of epoll and coroutines. Not only does the library provide a very fast asyncrounous engine, it also makes concurrent programming a straitforward, easy to write and read process. Hence Blackmamba's speed includes development time. 

Blackmamba was designed with the following goals:

 * Straitforward Pythonic development
 * Fast concurrent execution
 * Robust error handling and debugging

Getting Started
-----------

Although developers must build coroutines to use the Blackmamba library, it is not necessary to actually understand thier intricacies. The coroutine requirement actually simplifies program flow, readibility, and ease of development. This tutorial should provide sufficient instruction to get started with the Blackmamba library; no prior knowlege of coroutines is assumed or required. However, for those interested how the library works behind the scenes, a primer on Python coroutines can be found [here].

To connect to a host using the Python socket library:

import socket
sock = socket.socket()
sock.connect((host,port))

To do the same thing with Blackmamba:

from blackmamba import *
yield connect(host,port)

A few things are different. First, the Blackmamba API is exposed as the Python equivelent of system calls. To connect to a host one simply calls connect(). There is no need to create or manage sockets objects or connection state; the library does that for you. Second, all Blackmamaba system calls have to be prefixed with "yield". The "yield" expressin is Python's way of building Generators and Coroutines. That's it! No need for threads, pools, queues, semaphores, callback chains, deferreds, or complicated interfaces. 

Example
-------

The following is an example of downloading a single webpage 100 times. For comparision this is first done using the standard Python socket library then with Blackmamba. Following the code is a line-by-line explanation. 


Downloading a webpage 100 times with the Python socket library:

1. 	import socket
2. 
3. 	def get(host, port=80):
4.		msg = "GET / HTTP/1.1\r\nHost: %s\r\n\r\n" % host
5.		sock = socket.socket()
6.		sock.connet((host, port))
7.		sock.write(msg)
8.		response = sock.read(4096)
9.		sock.close()
10.		print response
11.
12.	for i in range(100):
13.		get('example.com')



Downloading a webpage 100 times with Blackmamba:

1.	from blackmamba import *
2.
3.	def get(host, port=80):
4.		msg = "GET / HTTP/1.1\r\nHost: %s\r\n\r\n" % host
5.		yield connect(host, port)
6.		yield write(msg)
7.		response = yield read()
8.		yield close()
9.		print response
10.
11.	def generate(host, count=100):
12.		for i in range(count):
13.			yield get('example.com')
14.
15.	run(generate('example.com'))


Explanation
----------

 . The get() function is the implimentation of a protocol; it defines which network operations to perform and in what order. A Blackmamba protocol can be any coroutine that uses the blackmamba system calls.  
 . Blackmamba's connect system call establishes a connection to the host. The Python "yield" expression suspends execution until the connection is established and then resumes automatically; it is the Python magic that makes code non-blocking. Anytime yield is used in this manner the function is technically a coroutine. However, so long as each Blackmamba system call is preceeded by the yield keyword, that detail can be overlooked.
 . The the write system call is equivelent to socket.write or socket.send. Execution is suspended by yield until the write has completed. Then execution continues with the next line. While execution is suspended other functions/protocols may be resumed if thier pending network operations have completed. 
 . Read data from the connection and store it in the response variable. Again, yield suspends execution until the response has been fully read. At the moment read() returns all data availible and the amount to read cannot be specified. This behavior may change in later versions.
 . Close the connection and resume upon completion. 
 . There is no gain in using a concurrent networking library if there is only one network operation to perform; typical usage will involve multiple silmaltaneous network operation. To accomplish this, Blackmamba uses Python Generators. The generate() function in this example is a Generator that yields protocols. Remember, for the sake of Blackmamba programming, a protocol is any function that uses the the "yield syscall()" syntax.
 . The run() method is what peforms all the work. It expects a generator as the first argument and pulls protocols from it and executes them thousands at a time. Because the run() method blocks until there are no more items, it should be the last thing called by the application.

For comparison, using Blackmamba instead of sockets required a total increase of one line of code and executed XXX times faster. 


Project Status
--------------

 * The Blackmamba project is very Beta. It still undergoing development and debugging. It is not ready for production.
 * SSL support has not yet been implimented but is high on the todo list.
 * DNS lookups still block. This is also high on the todo list. This means that the library is not yet suitable for making thousands of concurrent connections do different hosts. It is, however, already useful for thousands of connections to a limmited number of hosts. 


Download
--------



