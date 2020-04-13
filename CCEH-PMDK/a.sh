LD_LIBRARY_PATH=$LD_LIBRARY_PATH:./lib/pmdk
export LD_LIBRARY_PATH

make
g++ -std=c++17 -I./ -lrt -lpthread -O3 -o test test.cpp src/CCEH_LSB.cpp -lpmemobj
