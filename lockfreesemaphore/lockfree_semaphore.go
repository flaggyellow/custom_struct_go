package lockfreesemaphore

import (
	"sync/atomic"
)

/*
	This code is referenced from semaphore.Weighted, and it is a single-producer,
	multi-consumer specialized semaphore.
	NOTE THAT:
		* The number of consumers is determined in advance, do not let more than
		  the specified number of consumers or multiple producers use this
		  semaphore.
		* The usage scenario of this semaphore guarantees that the consumer can
		  get enough resources to not be blocked on this semaphore when the producer
		  finishes it work and exits, so this semaphore does not provide a wake-up
		  function. Special attention should be paid to using this semaphore in
		  other scenarios.
*/

type waiter struct {
	n     int32
	ready chan struct{}
}

type waiter_queue struct {
	capacity uint32
	ehead    uint32
	etail    uint32
	dhead    uint32
	waiters  []waiter
}

// should never Enqueue when the queue is full
func (q *waiter_queue) Enqueue(n int32, ready chan struct{}) *waiter {
	newehead := atomic.AddUint32(&q.ehead, 1)
	// the capacity of queue is multiple of 2, so ehead % q.capacity should be correct
	curIndex := (newehead - 1) % q.capacity
	w := &q.waiters[curIndex]
	w.n = n
	w.ready = ready
	// it seems to be too much competition here
	for !atomic.CompareAndSwapUint32(&q.etail, newehead-1, newehead) {
	}
	// fmt.Printf("Info: Enqueue into slot %d \n", curIndex)
	return w
}

// designed for single producer
func (q *waiter_queue) Dequeue() *waiter {
	etail := atomic.LoadUint32(&q.etail)
	if q.dhead == etail%q.capacity {
		return nil
	}
	waiter := &q.waiters[q.dhead]
	// fmt.Printf("Info: Dequeue from slot %d \n", q.dhead)
	q.dhead = (q.dhead + 1) % q.capacity
	return waiter
}

// designed for single producer
func (q *waiter_queue) Front() *waiter {
	etail := atomic.LoadUint32(&q.etail)
	if q.dhead == etail%q.capacity {
		return nil
	}
	waiter := &q.waiters[q.dhead]
	return waiter
}

func (q *waiter_queue) Size() int {
	head := atomic.LoadUint32(&q.dhead)
	tail := atomic.LoadUint32(&q.etail)
	size := (tail - head + q.capacity) % q.capacity
	return int(size)
}

type LockfreeSem struct {
	capacity  int32
	cur       int32
	watermark int32
	wq        *waiter_queue
	notifying bool
}

// NewLockfreeSem create a new lockfree semaphore, capacity means how many resources it has,
// have means how many resource it has. The waiting goroutines will only be awaken when
// the amount of resource is beyond watermark. consumer_amount means how many consumers will
// use this semaphore, do not use consumers more than this amount.
func NewLockfreeSem(capacity int32, have int32, watermark int32, consumer_amount int) *LockfreeSem {
	if capacity <= 0 || have < 0 || have > capacity || watermark > capacity || consumer_amount <= 0 {
		return nil
	}
	// queue size should be multiple of 2
	var q_size uint32 = 1
	for q_size < uint32(consumer_amount)+1 {
		q_size *= 2
	}
	return &LockfreeSem{
		capacity:  capacity,
		cur:       capacity - have,
		watermark: watermark,
		wq: &waiter_queue{
			capacity: q_size,
			dhead:    0,
			ehead:    0,
			etail:    0,
			waiters:  make([]waiter, q_size),
		},
		notifying: false,
	}
}

func (s *LockfreeSem) Acquire(n int32) error {
	if n > s.capacity {
		// Don't make other Acquire calls block on one that's doomed to fail.
		return SemError{"Acquired more than capacity"}
	}

	if n <= 0 {
		return nil
	}

	if s.wq.Size() <= 0 {
		newCur := atomic.AddInt32(&s.cur, n)
		if newCur <= s.capacity {
			return nil
		}
		atomic.AddInt32(&s.cur, -1*n)
	}

	ready := make(chan struct{}, 1)
	s.wq.Enqueue(n, ready)

	defer close(ready)

	<-ready
	return nil
}

// designed for single producer.
func (s *LockfreeSem) Release(n int32) {
	atomic.AddInt32(&s.cur, -1*n)
	if s.cur < 0 {
		panic("semaphore: released more than held")
	}
	s.notifyWaiters()
}

// designed for single producer.
// keeps a watermark strategy, only notify waiters when the amount of resource is beyond watermark.
func (s *LockfreeSem) notifyWaiters() {
	if s.capacity-s.cur < s.watermark {
		return
	}
	if !s.notifying {
		s.notifying = true
		go func() {
			for {
				w := s.wq.Front()
				if w == nil {
					break // No more waiters blocked.
				}

				if s.capacity-s.cur < w.n {
					// Not enough tokens for the next waiter.  We could keep going (to try to
					// find a waiter with a smaller request), but under load that could cause
					// starvation for large requests; instead, we leave all remaining waiters
					// blocked.
					//
					// Consider a semaphore used as a read-write lock, with N tokens, N
					// readers, and one writer.  Each reader can Acquire(1) to obtain a read
					// lock.  The writer can Acquire(N) to obtain a write lock, excluding all
					// of the readers.  If we allow the readers to jump ahead in the queue,
					// the writer will starve — there is always one token available for every
					// reader.
					break
				}

				atomic.AddInt32(&s.cur, w.n)
				s.wq.Dequeue()
				w.ready <- struct{}{}
			}
			s.notifying = false
		}()
	}
}

type SemError struct {
	err string
}

func (e SemError) Error() string {
	return e.err
}
