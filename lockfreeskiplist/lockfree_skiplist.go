package lockfreeskiplist

import (
	"bytes"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	// Suitable for math.Floor(math.Pow(math.E, 15)) == 3269017 elements in list
	DefaultMaxLevel    int     = 15
	DefaultProbability float64 = 1 / math.E
)

type node struct {
	nexts []unsafe.Pointer
	key   []byte
	value interface{}
}

func newNode(level int, key []byte, value interface{}) *node {
	nd := &node{
		key:   key,
		value: value,
		nexts: make([]unsafe.Pointer, level),
	}
	for i := 0; i < level; i++ {
		nd.storeNext(i, nil)
	}
	return nd
}

// Key allows retrieval of the key for a given Element
func (nd *node) Key() []byte {
	return nd.key
}

// Value allows retrieval of the value for a given Element
func (nd *node) Value() interface{} {
	return nd.value
}

func (nd *node) Level() int {
	return len(nd.nexts)
}

func (nd *node) loadNext(level int) *node {
	return (*node)(atomic.LoadPointer(&nd.nexts[level]))
}

func (nd *node) storeNext(level int, next *node) {
	atomic.StorePointer(&nd.nexts[level], unsafe.Pointer(next))
}

func (nd *node) casNext(level int, expected *node, desire *node) bool {
	return atomic.CompareAndSwapPointer(&nd.nexts[level], unsafe.Pointer(expected), unsafe.Pointer(desire))
}

// LockFreeSkipList define
type LockFreeSkipList struct {
	head     *node
	tail     *node
	size     int32
	maxLevel int
	// rand.Source does not guarantee concurrency security,
	// so we need to assign it a mutex here.
	// See https://github.com/golang/go/issues/3611
	randlk      sync.Mutex
	randSource  rand.Source
	probability float64
	probTable   []float64
}

// NewWithMaxLevel creates a new skip list with MaxLevel set to the provided number.
// maxLevel has to be int(math.Ceil(math.Log(N))) for DefaultProbability (where N is an upper bound on the
// number of elements in a skip list). See http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.17.524
// Returns a pointer to the new list.
func NewWithMaxLevel(maxLevel int) *LockFreeSkipList {
	if maxLevel < 1 || maxLevel > 64 {
		panic("maxLevel for a SkipList must be a positive integer <= 64")
	}
	sl := &LockFreeSkipList{
		head:        newNode(maxLevel, []byte{}, nil),
		tail:        newNode(maxLevel, []byte{}, nil),
		size:        0,
		maxLevel:    maxLevel,
		randSource:  rand.New(rand.NewSource(time.Now().UnixNano())),
		probability: DefaultProbability,
		probTable:   probabilityTable(DefaultProbability, maxLevel),
	}

	for level := 0; level < maxLevel; level++ {
		sl.head.storeNext(level, sl.tail)
	}

	return sl
}

// New creates a new skip list with default parameters. Returns a pointer to the new list.
func NewLockFreeSkipList() *LockFreeSkipList {
	return NewWithMaxLevel(DefaultMaxLevel)
}

// Add a kv pair to skiplist, do not support update value
func (sl *LockFreeSkipList) Set(key []byte, value interface{}) bool {
	// TODO: Optimize how memory is allocated here.
	prevs := make([]*node, sl.maxLevel)
	nexts := make([]*node, sl.maxLevel)
	for {
		if sl.find(key, &prevs, &nexts) {
			return false
		}
		topLevel := sl.randLevel()
		newNode := newNode(topLevel, key, value)
		for level := 0; level < topLevel; level++ {
			newNode.storeNext(level, nexts[level])
		}
		if prev, next := prevs[0], nexts[0]; !prev.casNext(0, next, newNode) {
			// The successor of prev is not next, we should try again.
			continue
		}
		for level := 1; level < topLevel; level++ {
			for {
				if prev, next := prevs[level], nexts[level]; prev.casNext(level, next, newNode) {
					break
				}
				// The successor of prev is not next,
				// we should call find to update the prevs and nexts.
				sl.find(key, &prevs, &nexts)
			}
		}
		break
	}
	atomic.AddInt32(&sl.size, 1)
	return true
}

func (sl *LockFreeSkipList) Get(key []byte) *node {
	// TODO: Optimize how memory is allocated here.
	prevs := make([]*node, sl.maxLevel)
	nexts := make([]*node, sl.maxLevel)
	found := sl.find(key, &prevs, &nexts)
	if found {
		return nexts[0]
	}
	return nil
}

// Remove a value from skiplist.
// Important: This function does not support concurrency!
// Cache deletions are rare in InfluxDB, so it is not necessary to make the skiplist support lock-free deletions
// When running this function, you need to ensure that the upper layer keeps a global lock
func (sl *LockFreeSkipList) Remove(key []byte) bool {
	// TODO: Optimize how memory is allocated here.
	prevs := make([]*node, sl.maxLevel)
	nexts := make([]*node, sl.maxLevel)
	if !sl.find(key, &prevs, &nexts) {
		return false
	}
	removeNode := nexts[0]
	for level := len(removeNode.nexts) - 1; level >= 0; level-- {
		next := removeNode.loadNext(level)
		prevs[level].storeNext(level, next)
	}
	atomic.AddInt32(&sl.size, -1)
	return true
}

// Contains check if skiplist contains a value.
func (sl *LockFreeSkipList) Contains(key []byte) bool {
	// TODO: Optimize how memory is allocated here.
	prevs := make([]*node, sl.maxLevel)
	nexts := make([]*node, sl.maxLevel)
	return sl.find(key, &prevs, &nexts)
}

// GetSize get the element size of skiplist.
func (sl *LockFreeSkipList) GetSize() int32 {
	return atomic.LoadInt32(&sl.size)
}

// SetProbability changes the current P value of the list.
// It doesn't alter any existing data, only changes how future insert heights are calculated.
func (sl *LockFreeSkipList) SetProbability(newProbability float64) {
	sl.probability = newProbability
	sl.probTable = probabilityTable(sl.probability, sl.maxLevel)
}

func (list *LockFreeSkipList) randLevel() (level int) {
	// Our random number source only has Int63(), so we have to produce a float64 from it
	// Reference: https://golang.org/src/math/rand/rand.go#L150
	list.randlk.Lock()
	r := float64(list.randSource.Int63()) / (1 << 63)
	list.randlk.Unlock()

	level = 1
	for level < list.maxLevel && r < list.probTable[level] {
		level++
	}
	return
}

// probabilityTable calculates in advance the probability of a new node having a given level.
// probability is in [0, 1], MaxLevel is (0, 64]
// Returns a table of floating point probabilities that each level should be included during an insert.
func probabilityTable(probability float64, MaxLevel int) (table []float64) {
	for i := 1; i <= MaxLevel; i++ {
		prob := math.Pow(probability, float64(i-1))
		table = append(table, prob)
	}
	return table
}

func (sl *LockFreeSkipList) less(nd *node, key []byte) bool {
	if sl.head == nd {
		return true
	}
	if sl.tail == nd {
		return false
	}
	return bytes.Compare(nd.key, key) < 0
}

func (sl *LockFreeSkipList) equals(nd *node, key []byte) bool {
	if sl.head == nd || sl.tail == nd {
		return false
	}
	return bytes.Equal(nd.key, key)
}

func (sl *LockFreeSkipList) find(key []byte, prevs *[]*node, nexts *[]*node) bool {
	var prev *node
	var cur *node
	var next *node
	prev = sl.head
	for level := sl.maxLevel - 1; level >= 0; level-- {
		cur = prev.loadNext(level)
		for {
			next = cur.loadNext(level)
			if !sl.less(cur, key) {
				break
			}
			prev = cur
			cur = next
		}
		(*prevs)[level] = prev
		(*nexts)[level] = cur
	}
	return sl.equals(cur, key)
}

type Iterator interface {
	Next() bool
	ToBegin()
	AtEnd() bool
	Pair() ([]byte, interface{})
}

func (sl *LockFreeSkipList) Iterator() Iterator {
	si := &SkipListIterator{
		head: sl.head,
		tail: sl.tail,
	}
	si.ToBegin()
	return si
}

type SkipListIterator struct {
	head    *node
	tail    *node
	current *node
}

func (si *SkipListIterator) Next() bool {
	if si.AtEnd() {
		return false
	}
	si.current = si.current.loadNext(0)
	return true
}

func (si *SkipListIterator) ToBegin() {
	si.current = si.head.loadNext(0)
}

func (si *SkipListIterator) AtEnd() bool {
	return si.current == si.tail
}

func (si *SkipListIterator) Pair() ([]byte, interface{}) {
	key := si.current.key
	value := si.current.value
	return key, value
}
