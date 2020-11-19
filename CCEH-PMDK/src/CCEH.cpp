#include <iostream>
#include <thread>
#include <bitset>
#include <cassert>
#include <unordered_map>
#include <stdio.h>
#include <vector>
#include "src/CCEH.h"
#include "src/hash.h"
#include "src/util.h"

#define f_seed 0xc70697UL
#define s_seed 0xc70697UL
//#define f_seed 0xc70f6907UL
//#define s_seed 0xc70f6907UL

using namespace std;

void Segment::execute_path(PMEMobjpool* pop, vector<pair<size_t, size_t>>& path, Key_t& key, Value_t value){
    for(int i=path.size()-1; i>0; --i){
	bucket[path[i].first] = bucket[path[i-1].first];
	pmemobj_persist(pop, (char*)&bucket[path[i].first], sizeof(Pair));
    }
    bucket[path[0].first].value = value;
    mfence();
    bucket[path[0].first].key = key;
    pmemobj_persist(pop, (char*)&bucket[path[0].first], sizeof(Pair));
}

void Segment::execute_path(vector<pair<size_t, size_t>>& path, Pair _bucket){
    int i = 0;
    int j = (i+1) % 2;

    Pair temp[2];
    temp[0] = _bucket;
    for(auto p: path){
	temp[j] = bucket[p.first];
	bucket[p.first] = temp[i];
	i = (i+1) % 2;
	j = (i+1) % 2;
    }
}
	
vector<pair<size_t, size_t>> Segment::find_path(size_t target, size_t pattern){
    vector<pair<size_t, size_t>> path;
    path.reserve(kCuckooThreshold);
    path.emplace_back(target, bucket[target].key);

    auto cur = target;
    int i = 0;

    do{
	Key_t* key = &bucket[cur].key;
	auto f_hash = hash_funcs[0](key, sizeof(Key_t), f_seed);
	auto s_hash = hash_funcs[2](key, sizeof(Key_t), s_seed);

	if((f_hash >> (8*sizeof(f_hash) - local_depth)) != pattern || *key == INVALID){
	    break;
	}

	for(int j=0; j<kNumPairPerCacheLine*kNumCacheLine; ++j){
	    auto f_idx = (((f_hash & kMask) * kNumPairPerCacheLine) + j) % kNumSlot;
	    auto s_idx = (((s_hash & kMask) * kNumPairPerCacheLine) + j) % kNumSlot;

	    if(f_idx == cur){
		path.emplace_back(s_idx, bucket[s_idx].key);
		cur = s_idx;
		break;
	    }
	    else if(s_idx == cur){
		path.emplace_back(f_idx, bucket[f_idx].key);
		cur = f_idx;
		break;
	    }
	}
	++i;
    }while(i < kCuckooThreshold);

    if(i == kCuckooThreshold){
	path.resize(0);
    }

    return move(path);
}


bool Segment::Insert4split(Key_t& key, Value_t value, size_t loc){
    for(int i=0; i<kNumPairPerCacheLine*kNumCacheLine; ++i){
	auto slot = (loc+i) % kNumSlot;
	if(bucket[slot].key == INVALID){
	    bucket[slot].key = key;
	    bucket[slot].value = value;
	    return 1;
	}
    }
    return 0;
}

TOID(struct Segment)* Segment::Split(PMEMobjpool* pop){
#ifdef INPLACE
    TOID(struct Segment)* split = new TOID(struct Segment)[2];
    split[0] = pmemobj_oid(this);
    POBJ_ALLOC(pop, &split[1], struct Segment, sizeof(struct Segment), NULL, NULL);
    D_RW(split[1])->initSegment(local_depth+1);

    auto pattern = ((size_t)1 << (sizeof(Key_t)*8 - local_depth - 1));
    for(int i=0; i<kNumSlot; ++i){
	auto f_hash = hash_funcs[0](&bucket[i].key, sizeof(Key_t), f_seed);
	if(f_hash & pattern){
	    if(!D_RW(split[1])->Insert4split(bucket[i].key, bucket[i].value, (f_hash & kMask)*kNumPairPerCacheLine)){
		auto s_hash = hash_funcs[2](&bucket[i].key, sizeof(Key_t), s_seed);
		if(!D_RW(split[1])->Insert4split(bucket[i].key, bucket[i].value, (s_hash & kMask)*kNumPairPerCacheLine)){
#ifdef CUCKOO
		    auto path1 = find_path((f_hash & kMask)*kNumPairPerCacheLine, pattern);
		    auto path2 = find_path((s_hash & kMask)*kNumPairPerCacheLine, pattern);
		    if(path1.size() == 0 && path2.size() == 0){
			cerr << "[" << __func__ << "]: something wrong -- need to adjust probing distance" << endl;
		    }
		    else{
			if(path1.size() == 0){
			    execute_path(path2, bucket[i]);
			}
			else if(path2.size() == 0){
			    execute_path(path1, bucket[i]);
			}
			else if(path1.size() < path2.size()){
			    execute_path(path1, bucket[i]);
			}
			else{
			    execute_path(path2, bucket[i]);
			}
		    }
#endif
		}
	    }
	}
    }

    pmemobj_persist(pop, (char*)D_RO(split[1]), sizeof(struct Segment));
    return split;
#else
    TOID(struct Segment)* split = new TOID(struct Segment)[2];
    POBJ_ALLOC(pop, &split[0], struct Segment, sizeof(struct Segment), NULL, NULL);
    POBJ_ALLOC(pop, &split[1], struct Segment, sizeof(struct Segment), NULL, NULL);
    D_RW(split[0])->initSegment(local_depth+1);
    D_RW(split[1])->initSegment(local_depth+1);

    auto pattern = ((size_t)1 << (sizeof(Key_t)*8 - local_depth - 1));
    for(int i=0; i<kNumSlot; ++i){
	auto f_hash = h(&bucket[i].key, sizeof(Key_t));
	if(f_hash & pattern){
	    D_RW(split[1])->Insert4split(bucket[i].key, bucket[i].value, (f_hash & kMask)*kNumPairPerCacheLine);
	}
	else{
	    D_RW(split[0])->Insert4split(bucket[i].key, bucket[i].value, (f_hash & kMask)*kNumPairPerCacheLine);
	}
    }

    pmemobj_persist(pop, (char*)D_RO(split[0]), sizeof(struct Segment));
    pmemobj_persist(pop, (char*)D_RO(split[1]), sizeof(struct Segment));

    return split;
#endif
}


void CCEH::initCCEH(PMEMobjpool* pop){
    crashed = true;
    POBJ_ALLOC(pop, &dir, struct Directory, sizeof(struct Directory), NULL, NULL);
    D_RW(dir)->initDirectory();
    POBJ_ALLOC(pop, &D_RW(dir)->segment, TOID(struct Segment), sizeof(TOID(struct Segment))*D_RO(dir)->capacity, NULL, NULL);

    for(int i=0; i<D_RO(dir)->capacity; ++i){
	POBJ_ALLOC(pop, &D_RO(D_RO(dir)->segment)[i], struct Segment, sizeof(struct Segment), NULL, NULL);
	D_RW(D_RW(D_RW(dir)->segment)[i])->initSegment();
    }
}

void CCEH::initCCEH(PMEMobjpool* pop, size_t initCap){
    crashed = true;
    POBJ_ALLOC(pop, &dir, struct Directory, sizeof(struct Directory), NULL, NULL);
    D_RW(dir)->initDirectory(static_cast<size_t>(log2(initCap)));
    POBJ_ALLOC(pop, &D_RW(dir)->segment, TOID(struct Segment), sizeof(TOID(struct Segment))*D_RO(dir)->capacity, NULL, NULL);

    for(int i=0; i<D_RO(dir)->capacity; ++i){
	POBJ_ALLOC(pop, &D_RO(D_RO(dir)->segment)[i], struct Segment, sizeof(struct Segment), NULL, NULL);
	D_RW(D_RW(D_RW(dir)->segment)[i])->initSegment(static_cast<size_t>(log2(initCap)));
    }
}
 
void CCEH::Insert(PMEMobjpool* pop, Key_t& key, Value_t value){

    auto f_hash = hash_funcs[0](&key, sizeof(Key_t), f_seed);
    auto f_idx = (f_hash & kMask) * kNumPairPerCacheLine;

RETRY:
    auto x = (f_hash >> (8*sizeof(f_hash) - D_RO(dir)->depth));
    auto target = D_RO(D_RO(dir)->segment)[x];

    if(!D_RO(target)){
	std::this_thread::yield();
	goto RETRY;
    }
    
    /* acquire segment exclusive lock */
    if(!D_RW(target)->lock()){
	std::this_thread::yield();
	goto RETRY;
    }

    auto target_check = (f_hash >> (8*sizeof(f_hash) - D_RO(dir)->depth));
    if(D_RO(target) != D_RO(D_RO(D_RO(dir)->segment)[target_check])){
	D_RW(target)->unlock();
	std::this_thread::yield();
	goto RETRY;
    }

    auto pattern = (f_hash >> (8*sizeof(f_hash) - D_RO(target)->local_depth));
    for(unsigned i=0; i<kNumPairPerCacheLine * kNumCacheLine; ++i){
	auto loc = (f_idx + i) % Segment::kNumSlot;
	auto _key = D_RO(target)->bucket[loc].key;
	/* validity check for entry keys */
	if((((hash_funcs[0](&D_RO(target)->bucket[loc].key, sizeof(Key_t), f_seed) >> (8*sizeof(f_hash)-D_RO(target)->local_depth)) != pattern) || (D_RO(target)->bucket[loc].key == INVALID)) && (D_RO(target)->bucket[loc].key != SENTINEL)){
	    if(CAS(&D_RW(target)->bucket[loc].key, &_key, SENTINEL)){
		D_RW(target)->bucket[loc].value = value;
		mfence();
		D_RW(target)->bucket[loc].key = key;
		pmemobj_persist(pop, (char*)&D_RO(target)->bucket[loc], sizeof(Pair));
		/* release segment exclusive lock */
		D_RW(target)->unlock();
		return;
	    }
	}
    }

    auto s_hash = hash_funcs[2](&key, sizeof(Key_t), s_seed);
    auto s_idx = (s_hash & kMask) * kNumPairPerCacheLine;

    for(unsigned i=0; i<kNumPairPerCacheLine * kNumCacheLine; ++i){
	auto loc = (s_idx + i) % Segment::kNumSlot;
	auto _key = D_RO(target)->bucket[loc].key;
	if((((hash_funcs[0](&D_RO(target)->bucket[loc].key, sizeof(Key_t), f_seed) >> (8*sizeof(s_hash)-D_RO(target)->local_depth)) != pattern) || (D_RO(target)->bucket[loc].key == INVALID)) && (D_RO(target)->bucket[loc].key != SENTINEL)){
	    if(CAS(&D_RW(target)->bucket[loc].key, &_key, SENTINEL)){
		D_RW(target)->bucket[loc].value = value;
		mfence();
		D_RW(target)->bucket[loc].key = key;
		pmemobj_persist(pop, (char*)&D_RO(target)->bucket[loc], sizeof(Pair));
		D_RW(target)->unlock();
		return;
	    }
	}
    }

    auto target_local_depth = D_RO(target)->local_depth;
    // COLLISION !!
    /* need to split segment but release the exclusive lock first to avoid deadlock */
    D_RW(target)->unlock();

    if(!D_RW(target)->suspend()){
	std::this_thread::yield();
	goto RETRY;
    }

    /* need to check whether the target segment has been split */
#ifdef INPLACE
    if(target_local_depth != D_RO(target)->local_depth){
	D_RW(target)->sema = 0;
	std::this_thread::yield();
	goto RETRY;
    }
#else
    if(target_local_depth != D_RO(D_RO(D_RO(dir)->segment)[x])->local_depth){
	D_RW(target)->sema = 0;
	std::this_thread::yield();
	goto RETRY;
    }
#endif

#ifdef CUCKOO
    auto path1 = D_RW(target)->find_path(f_idx, pattern);
    auto path2 = D_RW(target)->find_path(s_idx, pattern);
    if(path1.size() != 0 || path2.size() != 0){
	auto path = &path1;
	if(path1.size() == 0 || (path2.size() != 0 && path2.size() < path1.size()) || (path2.size() != 0 && path1[0].second == INVALID)){
	    path = &path2;
	}
	D_RW(target)->execute_path(pop, *path, key, value);
	D_RW(target)->sema = 0;
	return;
    }
#endif

    TOID(struct Segment)* s = D_RW(target)->Split(pop);
DIR_RETRY:
    /* need to double the directory */
    if(D_RO(target)->local_depth == D_RO(dir)->depth){
	if(!D_RW(dir)->suspend()){
	    std::this_thread::yield();
	    goto DIR_RETRY;
	}

	x = (f_hash >> (8*sizeof(f_hash) - D_RO(dir)->depth));
	auto dir_old = dir;
	TOID_ARRAY(TOID(struct Segment)) d = D_RO(dir)->segment;
	TOID(struct Directory) _dir;
	POBJ_ALLOC(pop, &_dir, struct Directory, sizeof(struct Directory), NULL, NULL);
	POBJ_ALLOC(pop, &D_RO(_dir)->segment, TOID(struct Segment), sizeof(TOID(struct Segment))*D_RO(dir)->capacity*2, NULL, NULL);
	D_RW(_dir)->initDirectory(D_RO(dir)->depth+1);

	for(int i=0; i<D_RO(dir)->capacity; ++i){
	    if(i == x){
		D_RW(D_RW(_dir)->segment)[2*i] = s[0];
		D_RW(D_RW(_dir)->segment)[2*i+1] = s[1];
	    }
	    else{
		D_RW(D_RW(_dir)->segment)[2*i] = D_RO(d)[i];
		D_RW(D_RW(_dir)->segment)[2*i+1] = D_RO(d)[i];
	    }
	}

	pmemobj_persist(pop, (char*)&D_RO(D_RO(_dir)->segment)[0], sizeof(TOID(struct Segment))*D_RO(_dir)->capacity);
	pmemobj_persist(pop, (char*)&_dir, sizeof(struct Directory));
	dir = _dir;
	pmemobj_persist(pop, (char*)&dir, sizeof(TOID(struct Directory)));
#ifdef INPLACE
	D_RW(s[0])->local_depth++;
	pmemobj_persist(pop, (char*)&D_RO(s[0])->local_depth, sizeof(size_t));
	/* release segment exclusive lock */
	D_RW(s[0])->sema = 0;
#endif
	/* TBD */
	// POBJ_FREE(&dir_old);

    }
    else{ // normal split
	while(!D_RW(dir)->lock()){
	    asm("nop");
	}
	x = (f_hash >> (8*sizeof(f_hash) - D_RO(dir)->depth));
	if(D_RO(dir)->depth == D_RO(target)->local_depth + 1){
	    if(x%2 == 0){
		D_RW(D_RW(dir)->segment)[x+1] = s[1];
#ifdef INPLACE
		pmemobj_persist(pop, (char*)&D_RO(D_RO(dir)->segment)[x+1], sizeof(TOID(struct Segment)));
#else
		mfence();
		D_RW(D_RW(dir)->segment)[x] = s[0];
		pmemobj_persist(pop, (char*)&D_RO(D_RO(dir)->segment)[x], sizeof(TOID(struct Segment))*2);
#endif
	    }
	    else{
		D_RW(D_RW(dir)->segment)[x] = s[1];
#ifdef INPLACE
		pmemobj_persist(pop, (char*)&D_RO(D_RO(dir)->segment)[x], sizeof(TOID(struct Segment)));
#else
		mfence();
		D_RW(D_RW(dir)->segment)[x-1] = s[0];
		pmemobj_persist(pop, (char*)&D_RO(D_RO(dir)->segment)[x-1], sizeof(TOID(struct Segment))*2);
#endif
	    }
	    D_RW(dir)->unlock();

#ifdef INPLACE
	    D_RW(s[0])->local_depth++;
	    pmemobj_persist(pop, (char*)&D_RO(s[0])->local_depth, sizeof(size_t));
	    /* release target segment exclusive lock */
	    D_RW(s[0])->sema = 0;
#endif
	}
	else{
	    int stride = pow(2, D_RO(dir)->depth - target_local_depth);
	    auto loc = x - (x%stride);
	    for(int i=0; i<stride/2; ++i){
		D_RW(D_RW(dir)->segment)[loc+stride/2+i] = s[1];
	    }
#ifdef INPLACE
	    pmemobj_persist(pop, (char*)&D_RO(D_RO(dir)->segment)[loc+stride/2], sizeof(TOID(struct Segment))*stride/2);
#else
	    for(int i=0; i<stride/2; ++i){
		D_RW(D_RW(dir)->segment)[loc+i] = s[0];
	    }
	    pmemobj_persist(pop, (char*)&D_RO(D_RO(dir)->segment)[loc], sizeof(TOID(struct Segment))*stride);
#endif
	    D_RW(dir)->unlock();
#ifdef INPLACE
	    D_RW(s[0])->local_depth++;
	    pmemobj_persist(pop, (char*)&D_RO(s[0])->local_depth, sizeof(size_t));
	    /* release target segment exclusive lock */
	    D_RW(s[0])->sema = 0;
#endif
	}
    }
    std::this_thread::yield();
    goto RETRY;
}

bool CCEH::Delete(Key_t& key){
    return false;
}

Value_t CCEH::Get(Key_t& key){
    auto f_hash = hash_funcs[0](&key, sizeof(Key_t), f_seed);
    auto f_idx = (f_hash & kMask) * kNumPairPerCacheLine;

RETRY:
    while(D_RO(dir)->sema < 0){
	asm("nop");
    }

    auto x = (f_hash >> (8*sizeof(f_hash) - D_RO(dir)->depth));
    auto target = D_RO(D_RO(dir)->segment)[x];

    if(!D_RO(target)){
	std::this_thread::yield();
	goto RETRY;
    }

#ifdef INPLACE
    /* acquire segment shared lock */
    if(!D_RW(target)->lock()){
	std::this_thread::yield();
	goto RETRY;
    }
#endif

    auto target_check = (f_hash >> (8*sizeof(f_hash) - D_RO(dir)->depth));
    if(D_RO(target) != D_RO(D_RO(D_RO(dir)->segment)[target_check])){
	D_RW(target)->unlock();
	std::this_thread::yield();
	goto RETRY;
    }
    
    for(int i=0; i<kNumPairPerCacheLine*kNumCacheLine; ++i){
	auto loc = (f_idx+i) % Segment::kNumSlot;
	if(D_RO(target)->bucket[loc].key == key){
	    Value_t v = D_RO(target)->bucket[loc].value;
#ifdef INPLACE
	    /* key found, release segment shared lock */
	    D_RW(target)->unlock();
#endif
	    return v;
	}
    }

    auto s_hash = hash_funcs[2](&key, sizeof(Key_t), s_seed);
    auto s_idx = (s_hash & kMask) * kNumPairPerCacheLine;
    for(int i=0; i<kNumPairPerCacheLine*kNumCacheLine; ++i){
	auto loc = (s_idx+i) % Segment::kNumSlot;
	if(D_RO(target)->bucket[loc].key == key){
	    Value_t v = D_RO(target)->bucket[loc].value;
#ifdef INPLACE
	    D_RW(target)->unlock();
#endif
	    return v;
	}
    }

#ifdef INPLACE
    /* key not found, release segment shared lock */ 
    D_RW(target)->unlock();
#endif
    return NONE;
}

void CCEH::Recovery(PMEMobjpool* pop){
    size_t i = 0;
    while(i < D_RO(dir)->capacity){
	size_t depth_cur = D_RO(D_RO(D_RO(dir)->segment)[i])->local_depth;
	size_t stride = pow(2, D_RO(dir)->depth - depth_cur);
	size_t buddy = i + stride;
	if(buddy == D_RO(dir)->capacity) break;
	for(int j=buddy-1; i<j; j--){
	    if(D_RO(D_RO(D_RO(dir)->segment)[j])->local_depth != depth_cur){
		D_RW(D_RW(dir)->segment)[j] = D_RO(D_RO(dir)->segment)[i];
		pmemobj_persist(pop, (char*)&D_RO(D_RO(dir)->segment)[i], sizeof(TOID(struct Segment)));
	    }
	}
	i += stride;
    }
    pmemobj_persist(pop, (char*)&D_RO(D_RO(dir)->segment)[0], sizeof(TOID(struct Segment))*D_RO(dir)->capacity);
}


double CCEH::Utilization(void){
    size_t sum = 0;
    size_t cnt = 0;
    for(int i=0; i<D_RO(dir)->capacity; ++cnt){
	auto target = D_RO(D_RO(dir)->segment)[i];
	int stride = pow(2, D_RO(dir)->depth - D_RO(target)->local_depth);
	auto pattern = (i >> (D_RO(dir)->depth - D_RO(target)->local_depth));
	for(unsigned j=0; j<Segment::kNumSlot; ++j){
	    auto f_hash = h(&D_RO(target)->bucket[j].key, sizeof(Key_t));
	    if(((f_hash >> (8*sizeof(f_hash)-D_RO(target)->local_depth)) == pattern) && (D_RO(target)->bucket[j].key != INVALID)){
		sum++;
	    }
	}
	i += stride;
    }
    return ((double)sum) / ((double)cnt * Segment::kNumSlot)*100.0;
}

size_t CCEH::Capacity(void){
    size_t cnt = 0;
    for(int i=0; i<D_RO(dir)->capacity; cnt++){
	auto target = D_RO(D_RO(dir)->segment)[i];
	int stride = pow(2, D_RO(dir)->depth - D_RO(target)->local_depth);
	i += stride;
    }

    return cnt * Segment::kNumSlot;
}

// for debugging
Value_t CCEH::FindAnyway(Key_t& key){
    for(size_t i=0; i<D_RO(dir)->capacity; ++i){
	for(size_t j=0; j<Segment::kNumSlot; ++j){
	    if(D_RO(D_RO(D_RO(dir)->segment)[i])->bucket[j].key == key){
		cout << "segment(" << i << ")" << endl;
		cout << "global_depth(" << D_RO(dir)->depth << "), local_depth(" << D_RO(D_RO(D_RO(dir)->segment)[i])->local_depth << ")" << endl;
		cout << "pattern: " << bitset<sizeof(int64_t)>(i >> (D_RO(dir)->depth - D_RO(D_RO(D_RO(dir)->segment)[i])->local_depth)) << endl;
		cout << "Key MSB: " << bitset<sizeof(int64_t)>(h(&key, sizeof(key)) >> (8*sizeof(key) - D_RO(D_RO(D_RO(dir)->segment)[i])->local_depth)) << endl;
		return D_RO(D_RO(D_RO(dir)->segment)[i])->bucket[j].value;
	    }
	}
    }
    return NONE;
}
