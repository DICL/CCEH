.PHONY: all clean
.DEFAULT_GOAL := all

CC=g++
LIBS=-lrt -lm -lpmemobj
CFLAGS=-O3 -std=c++11 -w 
INCLUDES=-I./ 

all: main

main: 
	$(CC) $(CFLAGS) -c src/CCEH_LSB.cpp -o src/CCEH_LSB.o $(LIBS) $(INCLUDES)

clean: 
	rm -rf src/*.o
