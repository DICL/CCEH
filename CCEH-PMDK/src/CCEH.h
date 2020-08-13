#ifndef CCEH_H_
#define CCEH_H_

#include <cstring>
#include <cmath>
#include <vector>
#include <cstdlib>
#include <libpmemobj.h>

#define CAS(_p, _u, _v) (__atomic_compare_exchange_n (_p, _u, _v, false, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE))
#define kCacheLineSize (64)

#define TOID_ARRAY(x) TOID(x)

typedef size_t Key_t;
typedef const char* Value_t;

const Key_t SENTINEL = -2;
const Key_t INVALID = -1;
const Value_t NONE = 0x0;

struct Pair{
    Key_t key;
    Value_t value;
};

class CCEH;
struct Directory;
struct Segment;
POBJ_LAYOUT_BEGIN(HashTable);
POBJ_LAYOUT_ROOT(HashTable, CCEH);
POBJ_LAYOUT_TOID(HashTable, struct Directory);
POBJ_LAYOUT_TOID(HashTable, struct Segment);
POBJ_LAYOUT_TOID(HashTable, TOID(struct Segment));
POBJ_LAYOUT_END(HashTable);

constexpr size_t kSegmentBits = 8;
constexpr size_t kMask = (1 << kSegmentBits)-1;
constexpr size_t kShift = kSegmentBits;
constexpr size_t kSegmentSize = (1 << kSegmentBits) * 16 * 4;
constexpr size_t kNumPairPerCacheLine = 4;
constexpr size_t kNumCacheLine = 4;

struct Segment{
    static const size_t kNumSlot = kSegmentSize/sizeof(Pair);

    Segment(void){ }
    ~Segment(void){ }

    void initSegment(void){
	for(int i=0; i<kNumSlot; ++i){
	    pair[i].key = INVALID;
	}
	local_depth = 0;
	sema = 0;
	//printf("[%s] called\n", __func__);
    }

    void initSegment(size_t depth){
	for(int i=0; i<kNumSlot; ++i){
	    pair[i].key = INVALID;
	}
	local_depth = depth;
	sema = 0;
	//printf("[%s] called with %lld depth\n", __func__, depth);
    }

    int Insert(PMEMobjpool*, Key_t&, Value_t, size_t, size_t);
    void Insert4split(Key_t&, Value_t, size_t);
    TOID(struct Segment)* Split(PMEMobjpool*);

    size_t numElement(void);

    Pair pair[kNumSlot];
    size_t local_depth;
    int64_t sema = 0;
    size_t pattern = 0;
    
};

struct Directory{
    static const size_t kDefaultDepth = 10;
    TOID_ARRAY(TOID(struct Segment)) segment;
    //TOID(struct Segment)* segment;
    //Segment** segment;
    //TOID(void*) segment;
    size_t capacity;
    size_t depth;
    bool lock;
    int sema = 0;

    Directory(void){ }
    ~Directory(void){ }

    void initDirectory(void){
	depth = kDefaultDepth;
	capacity = pow(2, depth);
	lock = false;
	sema = 0;
	printf("[%s] called\n", __func__);
    }

    void initDirectory(size_t _depth){
	depth = _depth;
	capacity = pow(2, _depth);
	lock = false;
	sema = 0;
	printf("[%s] called with %lld depth\n", __func__, _depth);
    }

    bool Acquire(void){
	bool unlocked = false;
	return CAS(&lock, &unlocked, true);
    }

    bool Release(void){
	bool locked = true;
	return CAS(&lock, &locked, false);
    }

    void LSBUpdate(PMEMobjpool*, int, int, int, int, TOID(struct Segment)*);
};

class CCEH{
    public:
	CCEH(void){ }
	~CCEH(void){ }
	void initCCEH(PMEMobjpool*);
	void initCCEH(PMEMobjpool*, size_t);

	void Insert(PMEMobjpool*, Key_t&, Value_t);
	bool Delete(Key_t&);
	Value_t Get(Key_t&);

	size_t Capacity(void);

    private:
	size_t global_depth;
	//PMEMobjpool* pop;
	TOID(struct Directory) dir;
};

#endif

