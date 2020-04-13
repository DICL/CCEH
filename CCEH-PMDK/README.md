## Introduction
CCEH PMDK (Persistent Memory Development Kit) Version


## Build Instructions

To build this sample, download/clone this directory. run ./a.sh 

## How to Run
usage:

./test [-w|-d|-r] <PM save file> <hash_key_start_num> <hash_key_end_num>


example:

To write hash key from 0 to 1000 on File1

./test -w ./File1 0 1000


To read hash key from 0 to 1000 in File1

./test -r ./File1 0 1000


To delete hash key from 0 to 500 in File1

./test -d ./File1 0 500  
