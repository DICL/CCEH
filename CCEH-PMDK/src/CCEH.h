#ifndef CCEH_H_
#define CCEH_H_

#include <cstring>
#include <iostream>
#include <stdlib.h>
#include <string.h>
#include <libpmemobj.h>
#include <unistd.h>
#include <cmath>
#include <vector>
#include "../util/pair.h"
#include "../src/hash.h"

#define LAYOUT "CCEH"
#define TOID_ARRAY(x) TOID(x)

constexpr size_t kSegmentBits = 8;
constexpr size_t kMask = (1 << kSegmentBits)-1;
constexpr size_t kShift = kSegmentBits;
constexpr size_t kSegmentSize = (1 << kSegmentBits) * 16 * 4;
constexpr size_t kNumPairPerCacheLine = 4;
constexpr size_t kNumCacheLine = 4;

POBJ_LAYOUT_BEGIN(CCEH_LAYOUT);
POBJ_LAYOUT_ROOT(CCEH_LAYOUT, struct CCEH_pmem);
POBJ_LAYOUT_TOID(CCEH_LAYOUT, struct Directory_pmem);
POBJ_LAYOUT_TOID(CCEH_LAYOUT, struct Segment_pmem);
POBJ_LAYOUT_TOID(CCEH_LAYOUT, TOID(struct Segment_pmem));
POBJ_LAYOUT_TOID(CCEH_LAYOUT, Pair);
POBJ_LAYOUT_END(CCEH_LAYOUT);

struct Segment_pmem{
	size_t pair_size;
	size_t local_depth;
	int64_t sema;
	size_t pattern;
	TOID(Pair) pairs;
};
struct Directory_pmem{
	size_t capacity;
	size_t depth;
	bool lock;
	int sema = 0;

	TOID_ARRAY(TOID(struct Segment_pmem)) segments;
};
struct CCEH_pmem{
	size_t global_depth;
	TOID(struct Directory_pmem) directories;	
};
struct Segment {
  static const size_t kNumSlot = kSegmentSize/sizeof(Pair);

  void* operator new(size_t size) {
    void* ret;
    posix_memalign(&ret, 64, size);
    return ret;
  }

  void* operator new[](size_t size) {
    void* ret;
    posix_memalign(&ret, 64, size);
    return ret;
  }

  int Insert(Key_t&, Value_t, size_t, size_t);
  void Insert4split(Key_t&, Value_t, size_t);
  bool Put(Key_t&, Value_t, size_t);
  int Delete(Key_t& key, size_t loc, size_t key_hash);
  Segment** Split(PMEMobjpool* pop);

//  Pair _[kNumSlot];
  size_t local_depth;
  int64_t sema = 0;
  size_t pattern = 0;
  
  TOID(struct Segment_pmem) seg_pmem;
  TOID(Pair) pairs;


  
  Segment(PMEMobjpool *pop, size_t depth)
  :local_depth{depth}
  {
    POBJ_ALLOC(pop, &seg_pmem, struct Segment_pmem, sizeof(struct Segment_pmem),NULL, NULL);
    D_RW(seg_pmem)->local_depth = local_depth;
    D_RW(seg_pmem)->sema = sema;
    D_RW(seg_pmem)->pattern = pattern;
    D_RW(seg_pmem)->pair_size = kNumSlot;

    POBJ_ALLOC(pop, &pairs, Pair, sizeof(Pair)*kNumSlot, NULL, NULL);
    D_RW(seg_pmem)->pairs = pairs; 
    for(int i=0;i<kNumSlot;i++){
	D_RW(pairs)[i].key = -1;
    }
  }
  Segment(){
  }
  void set_pattern_pmem(size_t pattern){
    D_RW(seg_pmem)->pattern = pattern;
  }
  Key_t get_key(size_t y){
    return D_RO(D_RO(seg_pmem)->pairs)[y].key;
  }
  Value_t get_value(size_t y){
    return D_RO(D_RO(seg_pmem)->pairs)[y].value;
  }
  void pair_insert_pmem(size_t y, Key_t key, Value_t value){
	D_RW(D_RW(seg_pmem)->pairs)[y].key = key;	
	D_RW(D_RW(seg_pmem)->pairs)[y].value = value;
  }
  void load_pmem(PMEMobjpool* pop,TOID(struct Segment_pmem)  seg_pmem_){
	seg_pmem = seg_pmem_;
	pairs = D_RO(seg_pmem)->pairs;
	sema = D_RO(seg_pmem)->sema;
	pattern = D_RO(seg_pmem)->pattern;
	local_depth = D_RO(seg_pmem)->local_depth;
//	kNumSlot = D_RO(seg_pmem)->pair_size;
	
  }
  ~Segment(void) {
  }
  size_t numElem(void); 
};

struct Directory {
public:
  static const size_t kDefaultDepth = 10;
  Segment** _;
  size_t capacity;
  size_t depth;
  int sema = 0 ;
  bool lock;
  PMEMobjpool *pop;
  TOID(struct Directory_pmem) dir_pmem;
  TOID_ARRAY(TOID(struct Segment_pmem)) segments;
  
  Directory(PMEMobjpool *pop_, size_t depth_, TOID(struct Directory_pmem) dir_pmem_){
    dir_pmem = dir_pmem_;
    capacity = pow(2,depth_);
    depth = depth_;
    sema = 0;
    lock = false;
    pop=pop_;
    _ = new Segment*[capacity];
    D_RW(dir_pmem)->depth = depth;
    D_RW(dir_pmem)->capacity = capacity;
    D_RW(dir_pmem)->lock = lock;
    D_RW(dir_pmem)->sema = sema;

    POBJ_ALLOC(pop, &segments, TOID(struct Segment_pmem), sizeof(TOID(struct Segment_pmem))*capacity, NULL,NULL);  
    D_RW(dir_pmem)->segments = segments;
  }
   Directory(){

   }
  ~Directory(void){

  }
  void doubling_pmem(){
    D_RW(dir_pmem)->capacity = capacity;
    printf("Doubling to %d\n",capacity); fflush(stdout);
    POBJ_REALLOC(pop, &segments, TOID(struct Segment_pmem),capacity*sizeof(TOID(struct Segment_pmem)));
    D_RW(dir_pmem)->segments = segments;
    for(size_t i=capacity/2; i<capacity; i++){
	D_RW(D_RW(dir_pmem)->segments)[i] = D_RO(D_RO(dir_pmem)->segments)[i-capacity/2];
    }
  } 
  void segment_bind_pmem(size_t dir_index, struct Segment* s){
    D_RW(D_RW(dir_pmem)->segments)[dir_index] = s->seg_pmem;
  }
  void load_pmem(PMEMobjpool* pop_, TOID(struct Directory_pmem) dir_pmem_){
    pop = pop_;
    dir_pmem = dir_pmem_;
    capacity = D_RO(dir_pmem)->capacity;
    sema = D_RO(dir_pmem)->sema;
    lock = D_RO(dir_pmem)->lock;
    depth = D_RO(dir_pmem)->depth;
    _ = new Segment*[capacity];
    segments = D_RO(dir_pmem)->segments;
    for(size_t i=0;i<capacity;i++){
	TOID(struct Segment_pmem) seg_pmem = D_RO(segments)[i];
        if(i == D_RO(seg_pmem)->pattern){
	  _[i] = new Segment();
	  _[i]->load_pmem(pop, seg_pmem);
	}else{
	  _[i] = _[D_RO(seg_pmem)->pattern];
	}
    }
  } 
  bool Acquire(void) {
    bool unlocked = false;
    return CAS(&lock, &unlocked, true);
  }

  bool Release(void) {
    bool locked = true;
    return CAS(&lock, &locked, false);
  }
  
  void SanityCheck(void*);
  void LSBUpdate(int, int, int, int, Segment**);
};

class CCEH {
  public:
    CCEH(const char*);
    CCEH(size_t, const char*);
    ~CCEH(void);
    void Insert(Key_t&, Value_t);
    Value_t Get(Key_t&);
    int Delete(Key_t&);
    /*bool InsertOnly(Key_t&, Value_t);
    bool Delete(Key_t&);
    Value_t FindAnyway(Key_t&);
    double Utilization(void);
    size_t Capacity(void);
    bool Recovery(void);
*/
    TOID(struct CCEH_pmem) cceh_pmem;
    TOID(struct Directory_pmem) dir_pmem;
    PMEMobjpool *pop;
    Directory* dir;
    size_t global_depth;
    int init_pmem(const char* path){

	if(access(path, F_OK) != 0){
          pop = pmemobj_create(path, LAYOUT, PMEMOBJ_MIN_POOL*10, 0666);
          if(pop==NULL){
   		perror(path);
   		exit(-1);
          }
  	  cceh_pmem = POBJ_ROOT(pop, struct CCEH_pmem);
	  POBJ_ALLOC(pop, &dir_pmem, struct Directory_pmem, sizeof(struct Directory_pmem), NULL,NULL);
	  D_RW(cceh_pmem)->directories = dir_pmem;
	  return 1;
	}else{
   	  pop = pmemobj_open(path, LAYOUT);
	  if(pop==NULL){
	    perror(path);
	    exit(-1);	
	  }
	  cceh_pmem = POBJ_ROOT(pop, struct CCEH_pmem);
	  global_depth = D_RO(cceh_pmem)->global_depth;
	  dir_pmem = D_RO(cceh_pmem)->directories;
          dir = new Directory();
	  dir->load_pmem(pop, dir_pmem);
	  return 0;
	}
    }

    void constructor(size_t global_depth_){
	global_depth = global_depth_;
	D_RW(cceh_pmem)->global_depth = global_depth;
	dir = new Directory(pop,global_depth, dir_pmem);
    }
    void set_global_depth_pmem(size_t global_depth){
	D_RW(cceh_pmem)->global_depth = global_depth;
    }
    void* operator new(size_t size) {
      void *ret;
      posix_memalign(&ret, 64, size);
      return ret;
    } 
};

#endif  // EXTENDIBLE_PTR_H_
