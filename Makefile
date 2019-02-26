CXX := g++
CFLAGS := -std=c++17 -I./ -lrt -lpthread -O3

all:
	echo "make ALL_CCEH"

clean:
	rm -rf src/*.o bin/* util/*.o

ALL_CCEH: CCEH_MSB CCEH_LSB

CCEH_MSB: src/CCEH.h src/CCEH_MSB.cpp
	$(CXX) $(CFLAGS) -c -o src/CCEH_MSB.o src/CCEH_MSB.cpp -DINPLACE
	$(CXX) $(CFLAGS) -c -o src/CCEH_MSB_CoW.o src/CCEH_MSB.cpp
	
CCEH_LSB: src/CCEH.h src/CCEH_LSB.cpp
	$(CXX) $(CFLAGS) -c -o src/CCEH_LSB.o src/CCEH_LSB.cpp -DINPLACE
	$(CXX) $(CFLAGS) -c -o src/CCEH_LSB_CoW.o src/CCEH_LSB.cpp
