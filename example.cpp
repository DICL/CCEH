#include <cstdio>
#include <ctime>
#include <unistd.h>
#include <cstdlib>
#include <cstring>
#include "util/pair.h"
#include "src/CCEH.h"

int main(){
    const size_t initialSize = 1024*16*4;
    const size_t insertSize = 1024*1024;

    CCEH* HashTable = new CCEH();
    u_int64_t* keys = new u_int64_t[insertSize];
    for(unsigned i=0; i<insertSize; i++){
	keys[i] = i+1;
    }
    for(unsigned i=0; i<insertSize; i++){
	HashTable->Insert(keys[i], reinterpret_cast<Value_t>(&keys[i]));
    }

    int failSearch = 0;
    for(unsigned i=0; i<insertSize; i++){
	auto ret = HashTable->Get(keys[i]);
	if(!ret){
	    failSearch++;
	}
    }
    printf("failedSearch: %d\n", failSearch);

    return 0;
}

	
