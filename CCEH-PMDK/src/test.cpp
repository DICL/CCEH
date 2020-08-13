#include <cstdio>
#include <ctime>
#include <cstdlib>
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <algorithm>
#include <thread>
#include <vector>


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
    char path[32];
    strcpy(path, argv[1]);
    int numData = atoi(argv[2]);
    const size_t initialSize = 1024*16*4;

    TOID(CCEH) HashTable = OID_NULL;
    PMEMobjpool* pop;
    if(access(path, 0) != 0){
	pop = pmemobj_create(path, "CCEH", POOL_SIZE, 0666);
	if(pop == NULL){
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
    }

    struct timespec start, end;
    uint64_t elapsed;

    cout << "Params: numData(" << numData << ")" << endl;

    uint64_t* keys = (uint64_t*)malloc(sizeof(uint64_t)*numData);

    ifstream ifs;

    string dataset = "../data.txt";
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

#ifdef INSERT
    {
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
#endif

#ifdef SEARCH
    {
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
#endif
    pmemobj_close(pop);
    return 0;
} 


