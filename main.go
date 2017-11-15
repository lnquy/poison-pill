package main

import (
	"bytes"
	"context"
	"github.com/rs/xid"
	"log"
	"runtime"
	"strconv"
	"sync"
	"time"
)

// Special value used as notification value between parent goroutine and its child workers
const poisonPill = "POISON_PILL"

type worker struct {
	ctx          context.Context // Context to allow cancel worker at any time
	wg           *sync.WaitGroup // Global WaitGroup
	parentChan   chan string     // Channel for distributing jobs from parent to its workers
	workerChan   chan string     // Channel for handling new jobs created by workers itself
}

// Starts a child worker pool.
func startWorkerPool(ctx context.Context, wg *sync.WaitGroup, parentChan chan string, num int) {
	workerChan := make(chan string, 5000)
	w := &worker{
		ctx:        ctx,
		wg:         wg,
		parentChan: parentChan,
		workerChan: workerChan,
	}
	for i := 0; i < num; i++ {
		w.wg.Add(1)
		go w.do()
	}
}

// worker do actual job here.
func (w *worker) do() {
	defer w.wg.Done()
	gid := getGID() // Get go routine ID of current worker, for logging purpose only
	var isParentDone bool // Indicator for checking if parent go routine is finished or not

	// Receives jobs from both parentChan and workerChan.
	// If receives POISON_PILL from parentChan, then marks the internal isParentDone to true,
	// re-passing POISON_PILL to other workers and stop listening from parentChan.
	for {
		select {
		case job := <-w.parentChan:
			if job == poisonPill { // Received poison pill
				log.Printf("Worker #%d received %s", gid, poisonPill)
				isParentDone = true
				w.parentChan <- poisonPill // Pass the poison pill to other workers
				goto handleRemainingJobsInWorkerChan
			}

			w.doHeavyJob(gid) // Otherwise just do normal processing
		case job := <-w.workerChan:
			_ = job
			w.doHeavyJob(gid)
		case <-w.ctx.Done():
			log.Printf("Worker #%d terminated", gid)
			return
		}
	}

handleRemainingJobsInWorkerChan:
	// Simulate workerChan still has remaining jobs
	for i := 0; i < 5; i++ {
		w.workerChan <- xid.New().String()
	}

	// Check if workerChan is empty -> end the worker
	if isParentDone && len(w.workerChan) == 0 {
		log.Printf("Worker #%d finished", gid)
		return
	}

	// Only handle remaining jobs in workerChan, stop listening from parentChan
	for {
		select {
		case job := <-w.workerChan:
			_ = job
			w.doHeavyJob(gid)
			if isParentDone && len(w.workerChan) == 0 { // workerChan is now empty
				log.Printf("Worker #%d finished", gid)
				return
			}
		case <-w.ctx.Done():
			log.Printf("Worker #%d terminated", gid)
			return
		}
	}
}

// Simulate actual job runs in long duration.
func (w *worker) doHeavyJob(gid uint64) {
	log.Printf("worker #%d is doing the job...", gid)
	time.Sleep(1 * time.Second)
	// If this worker produces more jobs then put those jobs to workerChan
	// w.workerChan <- "new job"
}

func main() {
	jobChan := make(chan string, 1000)
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	log.Printf("parent: starting child workers")
	startWorkerPool(ctx, wg, jobChan, 10) // 10 child workers
	log.Printf("parent: all child workers started")

	// Parent can terminate all child workers at any time by calling cancel()
	_ = cancel

	log.Printf("parent: start sending job to child workers")
	for i := 0; i < 50; i++ { // Simulate some random jobs
		jobChan <- xid.New().String()
	}
	log.Printf("parent: all jobs sent to child workers")
	jobChan <- poisonPill // Send poison pill to notify child workers
	log.Printf("parent: poison pill sent")

	wg.Wait() // Wait child workers to finished its remaining jobs
	log.Printf("parent: all child workers ended. exit")
}

// getGID returns the id of the current goroutine.
func getGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}
