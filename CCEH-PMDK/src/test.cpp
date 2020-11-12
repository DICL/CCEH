#include <cstdio>
#include <ctime>
#include <cstdlib>
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <algorithm>
#include <thread>
#include <vector>
#include <bitset>

#include "src/CCEH.h"
using namespace std;


#define POOL_SIZE (10737418240) // 10GB

void clear_cache() {
    int* dummy = new int[1024*1024*256];
    for (int i=0; i<1024*1024*256; i++) {
	dummy[i] = i;
    }

    for (int i=100;i<1024*1024*256;i++) {
	dummy[i] = dummy[i-rand()%100] + dummy[i+rand()%100];
    }

    delete[] dummy;
}


int main (int argc, char* argv[])
{
    if(argc < 3){
	cerr << "Usage: " << argv[0] << "path numData" << endl;
	exit(1);
    }
    const size_t initialSize = 1024*16;
    char path[32];
    strcpy(path, argv[1]);
    int numData = atoi(argv[2]);
#ifdef MULTITHREAD
    int numThreads = atoi(argv[3]);
#endif
    struct timespec start, end;
    uint64_t elapsed;
    PMEMobjpool* pop;
    bool exists = false;
    TOID(CCEH) HashTable = OID_NULL;

    if(access(path, 0) != 0){
	pop = pmemobj_create(path, "CCEH", POOL_SIZE, 0666);
	if(!pop){
	    perror("pmemoj_create");
	    exit(1);
	}
	HashTable = POBJ_ROOT(pop, CCEH);
	D_RW(HashTable)->initCCEH(pop, initialSize);
    }
    else{
	pop = pmemobj_open(path, "CCEH");
	if(pop == NULL){
	    perror("pmemobj_open");
	    exit(1);
	}
	HashTable = POBJ_ROOT(pop, CCEH);
	if(D_RO(HashTable)->crashed){
	    D_RW(HashTable)->Recovery(pop);
	}
	exists = true;
    }

#ifdef MULTITHREAD
    cout << "Params: numData(" << numData << "), numThreads(" << numThreads << ")" << endl;
#else
    cout << "Params: numData(" << numData << ")" << endl;
#endif
    uint64_t* keys = new uint64_t[numData];

    ifstream ifs;
    string dataset = "/home/chahg0129/dataset/input_rand.txt";
    ifs.open(dataset);
    if (!ifs){
	cerr << "No file." << endl;
	exit(1);
    }
    else{
	for(int i=0; i<numData; i++)
	    ifs >> keys[i];
	ifs.close();
	cout << dataset << " is used." << endl;
    }
#ifndef MULTITHREAD // single-threaded
    if(!exists){
	{ // INSERT
	    cout << "Start Insertion" << endl;
	    clear_cache();
	    clock_gettime(CLOCK_MONOTONIC, &start);
	    for(int i=0; i<numData; i++){
		D_RW(HashTable)->Insert(pop, keys[i], reinterpret_cast<Value_t>(keys[i]));
	    }
	    clock_gettime(CLOCK_MONOTONIC, &end);

	    elapsed = (end.tv_sec - start.tv_sec)*1000000000 + (end.tv_nsec - start.tv_nsec);
	    cout << elapsed/1000 << "\tusec\t" << (uint64_t)(1000000*(numData/(elapsed/1000.0))) << "\tOps/sec\tInsertion" << endl;
	}
    }

    { // SEARCH
	cout << "Start Searching" << endl;
	clear_cache();
	int failedSearch = 0;
	clock_gettime(CLOCK_MONOTONIC, &start);
	for(int i=0; i<numData; i++){
	    auto ret = D_RW(HashTable)->Get(keys[i]);
	    if(ret != reinterpret_cast<Value_t>(keys[i])){
		failedSearch++;
	    }
	}
	clock_gettime(CLOCK_MONOTONIC, &end);
	elapsed = (end.tv_sec - start.tv_sec)*1000000000 + (end.tv_nsec - start.tv_nsec);
	cout << elapsed/1000 << "\tusec\t" << (uint64_t)(1000000*(numData/(elapsed/1000.0))) << "\tOps/sec\tSearch" << endl;
	cout << "Failed Search: " << failedSearch << endl;
    }

#else // multi-threaded
    vector<thread> insertingThreads;
    vector<thread> searchingThreads;
    int chunk_size = numData/numThreads;

    if(!exists){
	{ // INSERT
	    auto insert = [&pop, &HashTable, &keys](int from, int to){
		for(int i=from; i<to; i++){
		    D_RW(HashTable)->Insert(pop, keys[i], reinterpret_cast<Value_t>(keys[i]));
		}
	    };

	    cout << "Start Insertion" << endl;
	    clear_cache();
	    clock_gettime(CLOCK_MONOTONIC, &start);
	    for(int i=0; i<numThreads; i++){
		if(i != numThreads-1)
		    insertingThreads.emplace_back(thread(insert, chunk_size*i, chunk_size*(i+1)));
		else
		    insertingThreads.emplace_back(thread(insert, chunk_size*i, numData));
	    }

	    for(auto& t: insertingThreads) t.join();
	    clock_gettime(CLOCK_MONOTONIC, &end);

	    elapsed = (end.tv_sec - start.tv_sec)*1000000000 + (end.tv_nsec - start.tv_nsec);
	    cout << elapsed/1000 << "\tusec\t" << (uint64_t)(1000000*(numData/(elapsed/1000.0))) << "\tOps/sec\tInsertion" << endl;
	}
    }

    { // SEARCH
	int failedSearch = 0;
	vector<int> searchFailed(numThreads);

	auto search = [&pop, &HashTable, &keys, &searchFailed](int from, int to, int tid){
	    int fail_cnt = 0;
	    for(int i=from; i<to; i++){
		auto ret = D_RW(HashTable)->Get(keys[i]);
		if(ret != reinterpret_cast<Value_t>(keys[i])){
		    fail_cnt++;
		}
	    }
	    searchFailed[tid] = fail_cnt;
	};

	cout << "Start Search" << endl;
	clear_cache();
	clock_gettime(CLOCK_MONOTONIC, &start);
	for(int i=0; i<numThreads; i++){
	    if(i != numThreads-1)
		searchingThreads.emplace_back(thread(search, chunk_size*i, chunk_size*(i+1), i));
	    else
		searchingThreads.emplace_back(thread(search, chunk_size*i, numData, i));
	}

	for(auto& t: searchingThreads) t.join();
	clock_gettime(CLOCK_MONOTONIC, &end);

	elapsed = (end.tv_sec - start.tv_sec)*1000000000 + (end.tv_nsec - start.tv_nsec);
	cout << elapsed/1000 << "\tusec\t" << (uint64_t)(1000000*(numData/(elapsed/1000.0))) << "\tOps/sec\tSearch" << endl;

	for(auto& v: searchFailed) failedSearch += v;
	cout << "Search Failed: " << failedSearch << endl;
    }
#endif

    auto util = D_RW(HashTable)->Utilization();
    cout << "Utilization: " << util << " %" << endl;

    D_RW(HashTable)->crashed = false;
    pmemobj_persist(pop, (char*)&D_RO(HashTable)->crashed, sizeof(bool));
    pmemobj_close(pop);
    return 0;
} 
