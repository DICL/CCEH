#include <iostream>
#include <cmath>
#include <thread>
#include <bitset>
#include <cassert>
#include <unordered_map>
#include "util/persist.h"
#include "util/hash.h"
#include "src/CCEH.h"

extern size_t perfCounter;
/*

// This function does not allow resizing
bool CCEH::InsertOnly(Key_t& key, Value_t value) {
  auto key_hash = h(&key, sizeof(key));
  auto x = (key_hash % dir->capacity);
  auto y = (key_hash >> (sizeof(key_hash)*8-kShift)) * kNumPairPerCacheLine;

  auto ret = dir->_[x]->Insert(key, value, y, key_hash);
  if (ret == 0) {
    clflush((char*)&dir->_[x]->_[y], 64);
    return true;
  }

  return false;
}

// TODO
bool CCEH::Delete(Key_t& key) {
  return false;
}



double CCEH::Utilization(void) {
  size_t sum = 0;
  std::unordered_map<Segment*, bool> set;
  for (size_t i = 0; i < dir->capacity; ++i) {
    set[dir->_[i]] = true;
  }
  for (auto& elem: set) {
    for (unsigned i = 0; i < Segment::kNumSlot; ++i) {
      if (elem.first->_[i].key != INVALID) sum++;
    }
  }
  return ((double)sum)/((double)set.size()*Segment::kNumSlot)*100.0;
}

size_t CCEH::Capacity(void) {
  std::unordered_map<Segment*, bool> set;
  for (size_t i = 0; i < dir->capacity; ++i) {
    set[dir->_[i]] = true;
  }
  return set.size() * Segment::kNumSlot;
}

size_t Segment::numElem(void) {
  size_t sum = 0;
  for (unsigned i = 0; i < kNumSlot; ++i) {
    if (_[i].key != INVALID) {
      sum++;
    }
  }
  return sum;
}

bool CCEH::Recovery(void) {
  return false;
}

// for debugging
Value_t CCEH::FindAnyway(Key_t& key) {
  using namespace std;
  for (size_t i = 0; i < dir->capacity; ++i) {
     for (size_t j = 0; j < Segment::kNumSlot; ++j) {
       if (dir->_[i]->_[j].key == key) {
         auto key_hash = h(&key, sizeof(key));
         auto x = (key_hash >> (8*sizeof(key_hash)-global_depth));
         auto y = (key_hash & kMask) * kNumPairPerCacheLine;
         cout << bitset<32>(i) << endl << bitset<32>((x>>1)) << endl << bitset<32>(x) << endl;
         return dir->_[i]->_[j].value;
       }
     }
  }
  return NONE;
}

void Directory::SanityCheck(void* addr) {
  using namespace std;
  for (unsigned i = 0; i < capacity; ++i) {
    if (_[i] == addr) {
      cout << i << " " << _[i]->sema << endl;
      exit(1);
    }
  }
}*/
int Segment::Delete(Key_t& key, size_t loc, size_t key_hash){
  if(sema==-1) return 2;
  if((key_hash & (size_t)pow(2, local_depth)-1) != pattern) return 2;
  auto lock = sema;
  int ret = 1;
  while(!CAS(&sema, &lock, lock+1)){
    lock = sema;
  }
   Key_t LOCK = key;
  for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
    auto slot = (loc + i) % kNumSlot;
    Key_t key_ = get_key(slot);
    if (CAS(&key_, &LOCK, SENTINEL)) {
      pair_insert_pmem(slot,-1,-1); 
      ret = 0;
      break;
    } else {
      LOCK = key;
    }
  }
  lock = sema;
  while (!CAS(&sema, &lock, lock-1)) {
    lock = sema;
  }
  return ret;

}
int CCEH::Delete(Key_t& key) {
STARTOVER:
  auto key_hash = h(&key, sizeof(key));
  auto y = (key_hash >> (sizeof(key_hash)*8-kShift)) * kNumPairPerCacheLine;

RETRY:
  auto x = (key_hash % dir->capacity);
  auto target = dir->_[x];
  //printf("{{{%d %d}}}\n",key,key_hash);
  auto ret = target->Delete(key, y, key_hash);

  if (ret == 1) {
    return -1; //DATA NOT FOUND 
  } else if (ret == 2) {
    goto STARTOVER;
  } else {
    return 0;
  }
}

int Segment::Insert(Key_t& key, Value_t value, size_t loc, size_t key_hash) {
  if (sema == -1) return 2;
  if ((key_hash & (size_t)pow(2, local_depth)-1) != pattern) return 2;
  auto lock = sema;
  int ret = 1;
  while (!CAS(&sema, &lock, lock+1)) {
    lock = sema;
  }
  Key_t LOCK = INVALID;
  for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
    auto slot = (loc + i) % kNumSlot;
    Key_t key_ = get_key(slot);
    if (CAS(&key_, &LOCK, SENTINEL)) {
      pair_insert_pmem(slot,key,value); 
     
// _[slot].value = value;      
//      mfence();
//      _[slot].key = key;
      ret = 0;
      break;
    } else {
      LOCK = INVALID;
    }
  }
  lock = sema;
  while (!CAS(&sema, &lock, lock-1)) {
    lock = sema;
  }
  return ret;
}


void Segment::Insert4split(Key_t& key, Value_t value, size_t loc) {
  for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
    auto slot = (loc+i) % kNumSlot;
    Key_t key_ = get_key(slot);
    if (key_ == INVALID) {
      pair_insert_pmem(slot,key,value);
      return;
    }
  }
}

Segment** Segment::Split(PMEMobjpool *pop) {
  using namespace std;
  int64_t lock = 0;
  if (!CAS(&sema, &lock, -1)) return nullptr;

 Segment** split = new Segment*[2];
  split[0] = new Segment(pop, local_depth+1);
  split[1] = new Segment(pop, local_depth+1);
  printf("Split\n");
  for (unsigned i = 0; i < kNumSlot; ++i) {
    //auto key_hash = h(&_[i].key, sizeof(Key_t));
   Key_t key_ = get_key(i);
   Value_t value_ = get_value(i);
    auto key_hash = h(&key_, sizeof(Key_t));
    if (key_hash & ((size_t) 1 << (local_depth))) {
      split[1]->Insert4split
        (key_, value_, (key_hash >> (8*sizeof(key_hash)-kShift))*kNumPairPerCacheLine);
    } else {
      split[0]->Insert4split
        (key_, value_, (key_hash >> (8*sizeof(key_hash)-kShift))*kNumPairPerCacheLine);
    }
  }

  clflush((char*)split[0], sizeof(Segment));
  clflush((char*)split[1], sizeof(Segment));

  return split;
}


void Directory::LSBUpdate(int local_depth, int global_depth, int dir_cap, int x, Segment** s) {
  int depth_diff = global_depth - local_depth;
  if (depth_diff == 0) {
    if ((x % dir_cap) >= dir_cap/2) {
      _[x-dir_cap/2] = s[0];
      segment_bind_pmem(x-dir_cap/2, s[0]);
      clflush((char*)&_[x-dir_cap/2], sizeof(Segment*));
      _[x] = s[1];
      segment_bind_pmem(x, s[1]);
      clflush((char*)&_[x], sizeof(Segment*));
      printf("lsb update : %d %d\n",x-dir_cap/2, x);
    } else {
      _[x] = s[0];
      segment_bind_pmem(x, s[0]);
      clflush((char*)&_[x], sizeof(Segment*));
      _[x+dir_cap/2] = s[1];
      segment_bind_pmem(x+dir_cap/2, s[1]);
      clflush((char*)&_[x+dir_cap/2], sizeof(Segment*));
      printf("lsb update : %d %d\n",x, x+dir_cap/2);
    }
  } else {
    if ((x%dir_cap) >= dir_cap/2) {
      LSBUpdate(local_depth+1, global_depth, dir_cap/2, x-dir_cap/2, s);
      LSBUpdate(local_depth+1, global_depth, dir_cap/2, x, s);
    } else {
      LSBUpdate(local_depth+1, global_depth, dir_cap/2, x, s);
      LSBUpdate(local_depth+1, global_depth, dir_cap/2, x+dir_cap/2, s);
    }
  }
  return;
}

void CCEH::Insert(Key_t& key, Value_t value) {
STARTOVER:
  auto key_hash = h(&key, sizeof(key));
  auto y = (key_hash >> (sizeof(key_hash)*8-kShift)) * kNumPairPerCacheLine;

RETRY:
  auto x = (key_hash % dir->capacity);
  auto target = dir->_[x];
  //printf("{{{%d %d}}}\n",key,key_hash);
  auto ret = target->Insert(key, value, y, key_hash);

  if (ret == 1) {
    Segment** s = target->Split(pop);
    if (s == nullptr) {
      goto RETRY;
    }

    s[0]->pattern = (key_hash % (size_t)pow(2, s[0]->local_depth-1));
    s[1]->pattern = s[0]->pattern + (1 << (s[0]->local_depth-1));
    s[0]->set_pattern_pmem(s[0]->pattern);
    s[1]->set_pattern_pmem(s[1]->pattern);
    // Directory management
    while (!dir->Acquire()) {
      asm("nop");
    }
    { // CRITICAL SECTION - directory update
     x = (key_hash % dir->capacity);
      if (dir->_[x]->local_depth < global_depth) {  // normal split
        dir->LSBUpdate(s[0]->local_depth, global_depth, dir->capacity, x, s);
      } else {  // directory doubling
        auto d = dir->_;
        auto _dir = new Segment*[dir->capacity*2];
        memcpy(_dir, d, sizeof(Segment*)*dir->capacity);
        memcpy(_dir+dir->capacity, d, sizeof(Segment*)*dir->capacity);
        _dir[x] = s[0];
        _dir[x+dir->capacity] = s[1];
        clflush((char*)&dir->_[0], sizeof(Segment*)*dir->capacity);
        dir->_ = _dir;
        clflush((char*)&dir->_, sizeof(void*));
        dir->capacity *= 2;
        clflush((char*)&dir->capacity, sizeof(size_t));
        global_depth += 1;
        clflush((char*)&global_depth, sizeof(global_depth));

	dir->doubling_pmem();
	dir->segment_bind_pmem(x, s[0]);
	dir->segment_bind_pmem(x+dir->capacity/2, s[1]);
	set_global_depth_pmem(global_depth);	
        delete d;
        // TODO: requiered to do this atomically
      }
    }  // End of critical section
    while (!dir->Release()) {
      asm("nop");
    }
    goto RETRY;
  } else if (ret == 2) {
    // Insert(key, value);
    goto STARTOVER;
  } else {
    //clflush((char*)&dir->_[x]->_[y], 64);
  }
}


CCEH::CCEH(const char* path, char flag)
{
  if(flag=='w'){
  init_pmem(path);
  constructor(0);
  size_t capacity = dir->capacity;
  for(unsigned i=0;i<capacity;i++){
     dir->_[i] = new Segment(pop, global_depth);
     dir->_[i]->pattern = i;    
  }
  }else{
   load_pmem(path);
  }
}

CCEH::CCEH(size_t initCap, const char* path, char flag)
{
  if(flag=='w'){
  init_pmem(path);
  constructor(log2(initCap));
  size_t capacity = dir->capacity;
  for(unsigned i=0;i<capacity;i++){
     dir->_[i] = new Segment(pop, global_depth);
     dir->_[i]->pattern = i;    
  }
  }else{
    load_pmem(path);
  }
}

CCEH::~CCEH(void)
{ }

Value_t CCEH::Get(Key_t& key) {
  auto key_hash = h(&key, sizeof(key));
  const size_t mask = dir->capacity-1;
  auto x = (key_hash & mask);
  auto y = (key_hash >> (sizeof(key_hash)*8-kShift)) * kNumPairPerCacheLine;

  auto dir_ = dir->_[x];

  for (unsigned i = 0; i < kNumPairPerCacheLine * kNumCacheLine; ++i) {
    auto slot = (y+i) % Segment::kNumSlot;
    Key_t key_ = dir->_[x]->get_key(slot);
    if (key_ == key) {
     return dir->_[x]->get_value(slot);
    }
  }
 return NONE;
}
