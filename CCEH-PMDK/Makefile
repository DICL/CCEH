.PHONY: all clean

CXX := g++ -O3 -std=c++17
CFLAGS := -I./ -lrt

all: test

test: src/test.cpp CCEH
	$(CXX) $(CFLAGS) -o bin/single_threaded_cceh src/test.cpp src/CCEH.o -lpmemobj -lpmem
	$(CXX) $(CFLAGS) -o bin/multi_threaded_cceh src/test.cpp src/CCEH.o -lpmemobj -lpmem -lpthread -DMULTITHREAD
	$(CXX) $(CFLAGS) -o bin/single_threaded_ccehCoW src/test.cpp src/CCEH_CoW.o -lpmemobj -lpmem
	$(CXX) $(CFLAGS) -o bin/multi_threaded_ccehCoW src/test.cpp src/CCEH_CoW.o -lpmemobj -lpmem -lpthread -DMULTITHREAD

CCEH: src/CCEH.h src/CCEH.cpp
	$(CXX) $(CFLAGS) -c -o src/CCEH.o src/CCEH.cpp -DINPLACE -lpmemobj -lpmem
	$(CXX) $(CFLAGS) -c -o src/CCEH_CoW.o src/CCEH.cpp -lpmemobj -lpmem
	
clean:
	rm -rf src/*.o bin/* 
