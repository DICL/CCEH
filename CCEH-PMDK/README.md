## Introduction

This "Hello" example explores the transactional object store, memory
allocation, and transactions in the libpmemobj library.  Developers new to
persistent memory programming may want to start with this library.  The goal is
to demonstrate how to create a "Hello Persistent Memory!!!" program using the
these functions to access the persistent memory and then reading back before
displaying it to the stdout. 

## Build Instructions

To build this sample, download/clone the pmdk-examples repository. A Makefile
is provided. 

## How to Run

After building the binary, the code sample can be run with a statement such as
the following:

	$./manpage test 

Below is an example of the result output:

	$ ./hello_libpmemobj -w t

Write the (Hello Persistent Memory!!!) string to persistent-memory.

	$ ./hello_libpmemobj -r t

Read the (Hello Persistent Memory!!!) string from persistent-memory.

	$ ./hello_libpmemobj -q t

Usage: ./hello_libpmemobj <-w/-r> <filename>
	
	$ ./hello_libpmemobj -r t

Read the (Hello Persistent Memory!!!) string from persistent-memory.
$ 
