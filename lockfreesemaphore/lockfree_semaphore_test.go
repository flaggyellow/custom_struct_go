package lockfreesemaphore

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var cardi int = 2e6

func runTestWithParam(ptime int, ctime int, capacity int32, have int32, watermark int32, acquire int32) {
	concurrency := runtime.GOMAXPROCS(0)
	chans := make([]chan struct{}, cardi)
	for i := 0; i < cardi; i++ {
		chans[i] = make(chan struct{}, 1)
	}
	sem := NewLockfreeSem(capacity, have, watermark, concurrency)
	idx := uint64(0)
	chSent := uint64(0)
	Acquires := uint64(0)
	Releases := uint64(0)
	var wg sync.WaitGroup
	wg.Add(concurrency + 1)
	// create consumer
	for i := 0; i < concurrency; i++ {
		go func() {
			for {
				err := sem.Acquire(acquire)
				if err != nil {
					fmt.Printf("Error: %v\n", err)
				}
				atomic.AddUint64(&Acquires, 1)
				for x := 0; x < int(acquire); x++ {
					j := int(atomic.AddUint64(&idx, uint64(1))) - 1
					if j >= cardi {
						goto end
					}
					if ctime > 0 {
						time.Sleep(time.Nanosecond * time.Duration(ctime))
					}
					chans[j] <- struct{}{}
					atomic.AddUint64(&chSent, 1)
				}
			}
		end:
			wg.Done()
		}()
	}

	// create producer
	go func() {
		for i := 0; i < cardi; i++ {
			<-chans[i]
			sem.Release(1)
			atomic.AddUint64(&Releases, 1)
			if ptime > 0 {
				time.Sleep(time.Nanosecond * time.Duration(ptime))
			}
		}
		wg.Done()
	}()
	wg.Wait()
	fmt.Printf("Chsent: %d, Acquires: %d, Releases: %d\n", chSent, Acquires, Releases)
}

func TestConcurrency(t *testing.T) {
	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()
	rounds := 10
	fmt.Println("TEST1: no limit")
	for i := 0; i < rounds; i++ {
		fmt.Printf("round %d --- ", i)
		runTestWithParam(0, 0, 2000, 2000, 1800, 5)
	}
	fmt.Println("TEST2: producer slower")
	for i := 0; i < rounds; i++ {
		fmt.Printf("round %d --- ", i)
		runTestWithParam(100, 0, 2000, 2000, 1800, 5)
	}
	fmt.Println("TEST3: customer slower")
	for i := 0; i < rounds; i++ {
		fmt.Printf("round %d --- ", i)
		runTestWithParam(0, 1000, 2000, 2000, 1800, 5)
	}
	fmt.Println("TEST4: Larger step")
	for i := 0; i < rounds; i++ {
		fmt.Printf("round %d --- ", i)
		runTestWithParam(0, 0, 20000, 20000, 18000, 50)
	}
}
