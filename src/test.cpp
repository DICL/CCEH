#include "src/CCEH.h"
#include <unistd.h>
#include <cstdlib>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <thread>
#include <vector>
#include <ctime>

using namespace std;

void clear_cache(){
    int* dummy = new int[1024*1024*256];
    for(int i=0; i<1024*1024*256; i++){
	dummy[i] = i;
    }

    for(int i=100; i<1024*1024*256-100; i++){
	dummy[i] = dummy[i-rand()%100] + dummy[i+rand()%100];
    }

    delete[] dummy;
}


int main(int argc, char* argv[]){
    const size_t initialTableSize = 16*1024;
    size_t numData = atoi(argv[1]);
#ifdef MULTITHREAD
    size_t numThreads = atoi(argv[2]);
#endif

    struct timespec start, end;
    uint64_t* keys = (uint64_t*)malloc(sizeof(uint64_t)*numData);

    ifstream ifs;
    string dataset = "/home/chahg0129/dataset/input_rand.txt";
    ifs.open(dataset);
    if(!ifs){
	cerr << "no file" << endl;
	return 0;
    }

    cout << dataset << " is used" << endl;
    for(int i=0; i<numData; i++){
	ifs >> keys[i];
    }
    cout << "Reading dataset Completed" << endl;

    
    Hash* table = new CCEH(initialTableSize/Segment::kNumSlot);
    cout << "Hashtable Initialized" << endl;

#ifndef MULTITHREAD
    cout << "Start Insertion" << endl;
    clear_cache();
    clock_gettime(CLOCK_MONOTONIC, &start);
    for(int i=0; i<numData; i++){
	table->Insert(keys[i], reinterpret_cast<Value_t>(keys[i]));
    }
    clock_gettime(CLOCK_MONOTONIC, &end);

    cout << "NumData(" << numData << ")" << endl;
    uint64_t elapsed = end.tv_nsec - start.tv_nsec + (end.tv_sec - start.tv_sec)*1000000000;
    cout << "Insertion: " << elapsed/1000 << " usec\t" << (uint64_t)(1000000*(numData/(elapsed/1000.0))) << " ops/sec" << endl;

    cout << "Start Search" << endl;
    clear_cache();
    int failedSearch = 0;
    clock_gettime(CLOCK_MONOTONIC, &start);
    for(int i=0; i<numData; i++){
	auto ret = table->Get(keys[i]);
	if(ret != reinterpret_cast<Value_t>(keys[i]))
	    failedSearch++;
    }
    clock_gettime(CLOCK_MONOTONIC, &end);

    elapsed = end.tv_nsec - start.tv_nsec + (end.tv_sec - start.tv_sec)*1000000000;
    cout << "Search: " << elapsed/1000 << " usec\t" << (uint64_t)(1000000*(numData/(elapsed/1000.0))) << " ops/sec" << endl;
    cout << "failedSearch: " << failedSearch << endl;
#else
    vector<thread> insertingThreads;
    vector<thread> searchingThreads;
    vector<int> failed(numThreads);

    auto insert = [&table, &keys](int from, int to){
	for(int i=from; i<to; i++){
	    table->Insert(keys[i], reinterpret_cast<Value_t>(keys[i]));
	}
    };
    
    auto search = [&table, &keys, &failed](int from, int to, int tid){
	int fail = 0;
	for(int i=from; i<to; i++){
	    auto ret = table->Get(keys[i]);
	    if(ret != reinterpret_cast<Value_t>(keys[i])){
		fail++;
	    }
	}
	failed[tid] = fail;
    };

    cout << "Start Insertion" << endl;
    clear_cache();
    const size_t chunk = numData/numThreads;
    clock_gettime(CLOCK_MONOTONIC, &start);
    for(int i=0; i<numThreads; i++){
	if(i != numThreads-1)
	    insertingThreads.emplace_back(thread(insert, chunk*i, chunk*(i+1)));
	else
	    insertingThreads.emplace_back(thread(insert, chunk*i, numData));
    }

    for(auto& t: insertingThreads) t.join();
    clock_gettime(CLOCK_MONOTONIC, &end);
    cout << "NumData(" << numData << "), numThreads(" << numThreads << ")" << endl;
    uint64_t elapsed = end.tv_nsec - start.tv_nsec + (end.tv_sec - start.tv_sec)*1000000000;
    cout << "Insertion: " << elapsed/1000 << " usec\t" << (uint64_t)(1000000*(numData/(elapsed/1000.0))) << " ops/sec" << endl;

    cout << "Start Search" << endl;
    clear_cache();
    clock_gettime(CLOCK_MONOTONIC, &start);
    for(int i=0; i<numThreads; i++){
	if(i != numThreads-1)
	    searchingThreads.emplace_back(thread(search, chunk*i, chunk*(i+1), i));
	else
	    searchingThreads.emplace_back(thread(search, chunk*i, numData, i));
    }

    for(auto& t: searchingThreads) t.join();
    clock_gettime(CLOCK_MONOTONIC, &end);
    elapsed = end.tv_nsec - start.tv_nsec + (end.tv_sec - start.tv_sec)*1000000000;
    cout << "Search: " << elapsed/1000 << " usec\t" << (uint64_t)(1000000*(numData/(elapsed/1000.0))) << " ops/sec" << endl;


    int failedSearch = 0;
    for(auto& v: failed) failedSearch += v;
    cout << failedSearch << " failedSearch" << endl;
#endif

    auto util = table->Utilization();
    auto cap = table->Capacity();

    cout << "Util( " << util << " ), Capacity( " << cap << " )" << endl;
    return 0;
}

