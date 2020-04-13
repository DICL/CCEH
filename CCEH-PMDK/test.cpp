#include <cstdio>
#include <ctime>
#include <unistd.h>
#include <cstdlib>
#include <cstring>
#include "util/pair.h"
#include "src/CCEH.h"




int main(int argc, char* argv[]){
	const size_t initialSize = 1024*16*4;
	const size_t from = argc>=4? atoi(argv[3]) : 1024; 
	const size_t to = argc==5? atoi(argv[4]) : from+1; 
	int failSearch = 0;
	printf("Hashtable");fflush(stdout);
	//HashTable->init_pmem(argv[1]);
	uint64_t* keys = new uint64_t[to];
	uint64_t* values = new uint64_t[to];
	printf("from %d  to %d\n",from,to);
	for(unsigned i=from; i<to; i++){
		keys[i] = i;
		values[i] = i*1931+1;
	}
	struct timespec start, end;
	uint64_t elapsed;
	clock_gettime(CLOCK_MONOTONIC, &start);
	CCEH* HashTable = new CCEH(argv[2]);

	printf("!");fflush(stdout);
	if(!strcmp(argv[1], "-r")){
		for(int i=from;i<to;i++){
			auto ret = HashTable->Get(keys[i]);
			if(!ret || ret!=values[i]){
				printf("Fail key: %d, value: %d, ans: %d\n",keys[i], ret, values[i]);
				failSearch++;
			}
		}
		printf("Fail Search: %d",failSearch);
	}else if(!strcmp(argv[1], "-w")){
		for(int i=from;i<to;i++)
			HashTable->Insert(keys[i],values[i]);

		int failInsert=0;
		printf("Check\n");fflush(stdout);
		for(int i=from;i<to;i++){
			auto ret= HashTable->Get(keys[i]);
			if(!ret || ret!=values[i]) failInsert++;
		}
		printf("Fail Insert : %d\n",failInsert);
		//for(unsigned i=0; i<insertSize; i++){
		//	HashTable->Insert(keys[i], values[i]);
		//}
	}else{
		for(int i=from;i<to;i++)
			HashTable->Delete(keys[i]);
		int failDelete = 0;
		for(int i=from; i<to; i++){
			auto ret= HashTable->Get(keys[i]);
			if(ret) failDelete ++;
		}
		printf("Fail Delete : %d\n",failDelete);

	}
	return 0;
}


