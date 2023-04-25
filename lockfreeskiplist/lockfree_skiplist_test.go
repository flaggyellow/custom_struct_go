package lockfreeskiplist

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
	// "unsafe"
)

var testStringLength int = 100

func GenerateRandomString(length int) []byte {
	rand.Seed(time.Now().UnixNano())
	letters := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	b := make([]byte, length)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return b
}

// This test function is not designed for conccurency environment.
func checkSanity(sl *LockFreeSkipList, t *testing.T) {
	// each level must be correctly ordered
	fmt.Println("-----------------")
	fmt.Println("CheckSanity:")
	for level := 0; level < sl.maxLevel; level++ {
		cur := sl.head.loadNext(level)
		if cur == sl.tail {
			continue
		}
		if level > cur.Level() {
			t.Fatal("first node's level must be no less than current level")
		}

		cnt := 1
		next := cur.loadNext(level)

		for next != sl.tail {

			if !(bytes.Compare(next.key, cur.key) >= 0) {
				t.Fatalf("next key value must be greater than prev key value. [next:%v] [prev:%v]", next.key, cur.key)
			}

			if level > next.Level() {
				t.Fatalf("node's level must be no less than current level. [cur:%v] [node:%v]", level, next)
			}

			cur = next
			next = next.loadNext(level)
			cnt++
		}

		if level == 0 {
			if cnt != int(sl.GetSize()) {
				t.Fatalf("list len must match the level 0 nodes count. [cur:%v] [level0:%v]", cnt, sl.GetSize())
			}
		}
		percent := float64(cnt) / float64(sl.GetSize())
		fmt.Printf("level%d: %d elements, %f ratio\n", level, cnt, percent)
	}
	fmt.Println("CheckSanity Passed.")
}

func TestBasicBytesCRUD(t *testing.T) {

	var list *LockFreeSkipList = NewLockFreeSkipList()

	list.Set([]byte("10"), 1)
	list.Set([]byte("60"), 2)
	list.Set([]byte("30"), 3)
	list.Set([]byte("20"), 4)
	list.Set([]byte("90"), 5)
	checkSanity(list, t)

	list.Set([]byte("30"), 9)
	checkSanity(list, t)

	list.Remove([]byte("0"))
	list.Remove([]byte("20"))
	checkSanity(list, t)

	v1 := list.Get([]byte("10"))
	v2 := list.Get([]byte("60"))
	v3 := list.Get([]byte("30"))
	v4 := list.Get([]byte("20"))
	v5 := list.Get([]byte("90"))
	v6 := list.Get([]byte("0"))

	if v1 == nil || v1.value.(int) != 1 || !bytes.Equal(v1.key, []byte("10")) {
		t.Fatal(`wrong "10" value (expected "1")`, v1)
	}

	if v2 == nil || v2.value.(int) != 2 {
		t.Fatal(`wrong "60" value (expected "2")`)
	}

	// this skiplist does not support
	if v3 == nil || v3.value.(int) != 3 {
		t.Fatal(`wrong "30" value (expected "3")`)
	}

	if v4 != nil {
		t.Fatal(`found value for key "20", which should have been deleted`)
	}

	if v5 == nil || v5.value.(int) != 5 {
		t.Fatal(`wrong "90" value`)
	}

	if v6 != nil {
		t.Fatal(`found value for key "0", which should have been deleted`)
	}
}

func TestConcurrency(t *testing.T) {
	fmt.Println("4")
	list := NewLockFreeSkipList()
	amount = 3000000
	testBytes := make([][]byte, amount)
	checkDiff := make(map[string]struct{})
	for i := 0; i < amount; i++ {
		str := GenerateRandomString(testStringLength)
		for {
			checkDiff[string(str)] = struct{}{}
			if len(checkDiff) == i+1 {
				break
			}
			str = GenerateRandomString(testStringLength)
		}
		testBytes[i] = str
	}

	wg := &sync.WaitGroup{}
	wg.Add(6)
	go func() {
		for i := 0; i < amount; i++ {
			list.Set(testBytes[i], i)
		}
		wg.Done()
	}()

	go func() {
		for i := 0; i < amount; i++ {
			list.Set(testBytes[i], i)
		}
		wg.Done()
	}()

	go func() {
		for i := amount - 1; i >= 0; i-- {
			list.Set(testBytes[i], i)
		}
		wg.Done()
	}()

	go func() {
		for i := amount - 1; i >= 0; i-- {
			list.Set(testBytes[i], i)
		}
		wg.Done()
	}()

	go func() {
		for i := 0; i < amount; i++ {
			list.Get(testBytes[i])
		}
		wg.Done()
	}()

	go func() {
		for i := amount - 1; i >= 0; i-- {
			list.Get(testBytes[i])
		}
		wg.Done()
	}()

	wg.Wait()
	if int(list.GetSize()) != amount {
		fmt.Printf("SkipList length %d, expect %d.\n", list.GetSize(), amount)
		t.Fail()
	}

	checkSanity(list, t)
}

// skiplist benchmark

var benchList *LockFreeSkipList
var testStr [][]byte
var amount int

func BenchmarkSkipListSet(b *testing.B) {
	amount = 1000000
	testStr = make([][]byte, amount)
	for i := 0; i < amount; i++ {
		testStr[i] = GenerateRandomString(testStringLength)
	}

	b.ResetTimer()

	b.ReportAllocs()
	list := NewLockFreeSkipList()

	for i := 0; i < b.N; i++ {
		list.Set(testStr[i], [1]byte{})
	}

	b.SetBytes(int64(b.N))
}

func BenchmarkSkipListGet(b *testing.B) {
	amount = 1000000
	benchList = NewLockFreeSkipList()
	testStr = make([][]byte, amount)
	for i := 0; i < amount; i++ {
		testStr[i] = GenerateRandomString(testStringLength)
		benchList.Set(testStr[i], [1]byte{})
	}

	b.ResetTimer()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		res := benchList.Get(testStr[i])
		if res == nil {
			b.Fatal("failed to Get an element that should exist")
		}
	}

	b.SetBytes(int64(b.N))
}

func TestCompareSkiplistWithMapSet(t *testing.T) {
	amount = 1600000
	testStr = make([][]byte, amount)
	for i := 0; i < amount; i++ {
		testStr[i] = GenerateRandomString(testStringLength)
	}

	fmt.Printf("测试开始:\n")

	// test map[string]value

	start := time.Now()

	// 测试的代码块
	testMap := make(map[string]int, amount)
	for i := 0; i < amount; i++ {
		testMap[string(testStr[i])] = 1
	}

	elapsed := time.Since(start)
	fmt.Printf("map[string]*entry Set 执行时间: %s\n", elapsed)

	// test skiplist

	start = time.Now()

	// 测试的代码块
	testSkipList := NewLockFreeSkipList()
	for i := 0; i < amount; i++ {
		testSkipList.Set(testStr[i], 1)
	}

	elapsed = time.Since(start)
	fmt.Printf("skiplist Set 执行时间: %s\n", elapsed)
}

func TestCompareSkiplistWithMapGet(t *testing.T) {
	amount = 1600000
	testStr = make([][]byte, amount)
	for i := 0; i < amount; i++ {
		testStr[i] = GenerateRandomString(testStringLength)
	}

	testMap := make(map[string]int, amount)
	for i := 0; i < amount; i++ {
		testMap[string(testStr[i])] = 1
	}

	testSkipList := NewLockFreeSkipList()
	for i := 0; i < amount; i++ {
		testSkipList.Set(testStr[i], 1)
	}

	fmt.Printf("测试开始:\n")

	// test map[string]value

	start := time.Now()

	// 测试的代码块
	for i := 0; i < amount; i++ {
		_ = testMap[string(testStr[i])]
	}

	elapsed := time.Since(start)
	fmt.Printf("map[string]*entry Get 执行时间: %s\n", elapsed)

	// test skiplist

	start = time.Now()

	// 测试的代码块
	for i := 0; i < amount; i++ {
		_ = testSkipList.Get(testStr[i]).Value()
	}

	elapsed = time.Since(start)
	fmt.Printf("skiplist Get 执行时间: %s\n", elapsed)
}

func TestConccurentSet(t *testing.T) {
	fmt.Println("1")
	list := NewLockFreeSkipList()
	amount = 3200000
	task_n := 16
	testBytes := make([][]byte, amount)
	checkDiff := make(map[string]struct{})
	for i := 0; i < amount; i++ {
		str := GenerateRandomString(testStringLength)
		for {
			checkDiff[string(str)] = struct{}{}
			if len(checkDiff) == i+1 {
				break
			}
			str = GenerateRandomString(testStringLength)
		}
		testBytes[i] = str
	}

	fmt.Printf("skiplist test:\n")

	start := time.Now()

	// 测试的代码块

	wg := &sync.WaitGroup{}
	for task := 0; task < task_n; task++ {
		wg.Add(1)
		go func(task int) {
			fmt.Printf("task%d: %d~%d\n", task, task*amount/task_n, (task+1)*amount/task_n)
			for i := task * amount / task_n; i < (task+1)*amount/task_n; i++ {
				list.Set(testBytes[i], i)
			}
			wg.Done()
		}(task)
	}
	wg.Wait()

	elapsed := time.Since(start)
	fmt.Printf("skiplist Set 执行时间: %s\n", elapsed)

	if int(list.GetSize()) != amount {
		fmt.Printf("SkipList length %d, expect %d.\n", list.GetSize(), amount)
		t.Fail()
	}

	fmt.Printf("--------------\n")
	fmt.Printf("map test:\n")

	testMap := make(map[string]int, amount)
	var lk sync.Mutex

	start = time.Now()

	// 测试的代码块

	wg = &sync.WaitGroup{}
	for task := 0; task < task_n; task++ {
		wg.Add(1)
		go func(task int) {
			fmt.Printf("task%d: %d~%d\n", task, task*amount/task_n, (task+1)*amount/task_n)
			for i := task * amount / task_n; i < (task+1)*amount/task_n; i++ {
				lk.Lock()
				testMap[string(testBytes[i])] = 1
				lk.Unlock()
			}
			wg.Done()
		}(task)
	}
	wg.Wait()

	elapsed = time.Since(start)
	fmt.Printf("Map Set 执行时间: %s\n", elapsed)

	if len(testMap) != amount {
		fmt.Printf("SkipList length %d, expect %d.\n", list.GetSize(), amount)
		t.Fail()
	}
}

// func TestCompareKeyScan(t *testing.T) {
// 	amount = 1000000
// 	benchList = NewLockFreeSkipList()
// 	testStr = make([][]byte, amount)
// 	testMap := make(map[string]int, amount)
// 	for i := 0; i < amount; i++ {
// 		testStr[i] = GenerateRandomString(testStringLength)
// 		benchList.Set(testStr[i], 1)
// 		testMap[string(testStr[i])] = 1
// 	}

// 	// skiplist scan

// 	fmt.Printf("测试开始:\n")
// 	start := time.Now()

// 	keys := make([][]byte, 0, benchList.GetSize())
// 	for iter := benchList.Range(); !iter.End(); iter.Next() {
// 		k, _ := iter.Pair()
// 		keys = append(keys, k)
// 	}

// 	elapsed := time.Since(start)
// 	fmt.Printf("skiplist scan 执行时间: %s\n", elapsed)

// 	// map scan

// 	start = time.Now()

// 	keys = make([][]byte, 0, len(testMap))
// 	for k, _ := range testMap {
// 		keys = append(keys, []byte(k))
// 	}

// 	elapsed = time.Since(start)
// 	fmt.Printf("map scan 执行时间: %s\n", elapsed)
// }

func TestLockCost(t *testing.T) {

	var mu sync.RWMutex

	fmt.Printf("测试开始:\n")
	start := time.Now()

	for i := 0; i < 10000000; i++ {
		mu.RLock()
		mu.RUnlock()
	}

	elapsed := time.Since(start)
	fmt.Printf("lock 执行时间: %s\n", elapsed)
}
