#include <cstdio>
#include <ctime>
#include <unistd.h>
#include <cstdlib>
#include <cstring>
#include "util/pair.h"
#include "src/CCEH.h"

void clear_cache(){
	const size_t size = 1024*1024*512;
	int* dummy = new int[size];
	for(int i=100; i<size; i++){
		dummy[i] = i;
	}
	for(int i=100; i<size-(1024*1024); i++){
		dummy[i] = dummy[i-rand()%100] + dummy[i+rand()%100];
	}

	delete[] dummy;
}



int main(int argc, char* argv[]){
	const size_t initialSize = 1024*16*4;
	const size_t insertSize = argc==4? atoi(argv[3]) : 1024; 
	int failSearch = 0;
	printf("Hashtable");fflush(stdout);
	//HashTable->init_pmem(argv[1]);
		uint64_t* keys = new uint64_t[insertSize*2];
		uint64_t* values = new uint64_t[insertSize*2];
		for(unsigned i=0; i<insertSize; i++){
			keys[i] = i+1;
			values[i] = i*1931+1;
		}
	if(!strcmp(argv[1],"-r")){
		CCEH* HashTable = new CCEH(argv[2], 'r');
		for(unsigned i=0;i<insertSize;i++){
			auto ret= HashTable->Get( keys[i]);
			if(!ret || ret!=values[i]){
				printf("FAIL : key %d, value : %d, ans : %d\n", keys[i], ret, values[i]);
				failSearch++;
			}
		}
		printf("fail search : %d\n", failSearch);
		for(unsigned i=insertSize; i<insertSize*2; i++){
			keys[i] = i+1;
			values[i] = (i)*1931+1;
		}
		for(unsigned i=insertSize; i<insertSize*2; i++){
			HashTable->Insert(keys[i], values[i]);
		}
		for(unsigned i=0; i<insertSize; i++){
			HashTable->Delete(keys[i]);
		}
		for(unsigned i=insertSize; i<insertSize*2; i++){
			auto ret = HashTable->Get(keys[i]);
			if(!ret || ret!=values[i]){
				printf("FAIL : key %d, value : %d, ans : %d\n", keys[i], ret, values[i]);
				failSearch++;
			}
		}
		printf("fail search : %d\n",failSearch);
		
		for(unsigned i=0; i<3; i++){
			auto ret = HashTable->Get(keys[i]);
			if(!ret || ret!=values[i]){
				printf("FAIL : key %d, value : %d, ans : %d\n", keys[i], ret, values[i]);
				failSearch++;
			}
		}
	}else{
		struct timespec start, end;
		uint64_t elapsed;
		clock_gettime(CLOCK_MONOTONIC, &start);

		CCEH* HashTable = new CCEH(argv[2],'w');

		for(int i=0;i<insertSize;i++)
			HashTable->Insert(keys[i],values[i]);

		int failInsert=0;
		for(int i=0;i<insertSize;i++){
			auto ret= HashTable->Get(keys[i]);
			if(!ret || ret!=values[i]) failInsert++;
		}
		printf("Fail Insert : %d\n",failInsert);
		//for(unsigned i=0; i<insertSize; i++){
		//	HashTable->Insert(keys[i], values[i]);
		//}
	}
	return 0;
}


