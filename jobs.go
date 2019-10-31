package SimpleJobQueue

import (
	"errors"
	"sync"
)

// Credit to https://riptutorial.com/go/example/18325/job-queue-with-worker-pool

/*
Implements a JobQueue with workers to perform these jobs. Jobs are sent to the queue using channels,
and the dispatcher then sends it to an available worker.

A job is any interface that impelemnts the methods: Process(), GetJobID() string, LinkQueue(*bool) and Done().

Process():
	The function the worker will execute to perform the job, should contain any necessary code to perform a desired job.

GetJobID() string:
	Returns the JobID of a job. A job interface must therefor implement a form of JobID. It is assumed as a string

LinkQueue(*bool):
	Links a pointer to a bool in the JobQueue struct to the job when a job is being processed. This is used for tracking
	the state of a job and marking it as done.

Done():
	Sets the done bool to true.


*/

// Job - interface for job processing
type Job interface {
	Process()
	GetJobID() string
	LinkQueue(*bool)
	Done()
}

// Worker - the worker threads that actually process the jobs
type Worker struct {
	done             sync.WaitGroup
	readyPool        chan chan Job
	assignedJobQueue chan Job

	quit chan bool
}

// JobQueue - a queue for enqueueing jobs to be processed
type JobQueue struct {
	InternalQueueJobID map[string]*bool // False indicate job is not complete
	internalQueue      chan Job
	readyPool          chan chan Job
	workers            []*Worker
	dispatcherStopped  sync.WaitGroup
	workersStopped     sync.WaitGroup
	quit               chan bool
}

// NewJobQueue - creates a new job queue
func NewJobQueue(maxWorkers int) *JobQueue {
	workersStopped := sync.WaitGroup{}
	readyPool := make(chan chan Job, maxWorkers)
	workers := make([]*Worker, maxWorkers, maxWorkers)
	for i := 0; i < maxWorkers; i++ {
		workers[i] = NewWorker(readyPool, workersStopped)
	}
	return &JobQueue{
		InternalQueueJobID: make(map[string]*bool),
		internalQueue:      make(chan Job),
		readyPool:          readyPool,
		workers:            workers,
		dispatcherStopped:  sync.WaitGroup{},
		workersStopped:     workersStopped,
		quit:               make(chan bool),
	}
}

// Start - starts the worker routines and dispatcher routine
func (q *JobQueue) Start() {
	for i := 0; i < len(q.workers); i++ {
		q.workers[i].Start()
	}
	go q.dispatch()
}

// Stop - stops the workers and dispatcher routine
func (q *JobQueue) Stop() {
	q.quit <- true
	q.dispatcherStopped.Wait()
}

// Gives a job to an available worker
func (q *JobQueue) dispatch() {
	q.dispatcherStopped.Add(1)
	for {
		select {
		case job := <-q.internalQueue: // We got something in on our queue
			workerChannel := <-q.readyPool // Check out an available worker
			workerChannel <- job           // Send the request to the channel
		case <-q.quit:
			for i := 0; i < len(q.workers); i++ {
				q.workers[i].Stop()
			}
			q.workersStopped.Wait()
			q.dispatcherStopped.Done()
			return
		}
	}
}

func (q *JobQueue) GetJobStatus(id string) bool {
	return *q.InternalQueueJobID[id]
}

// Submit - adds a new job to be processed
func (q *JobQueue) Submit(job Job) error {
	if q.InQueue(job.GetJobID()) {
		return errors.New("job with id " + job.GetJobID() + " already exists")
	}
	select {
	case q.internalQueue <- job:
		// If queue is not full, link job to queue and pass pointer to done bool
		q.InternalQueueJobID[job.GetJobID()] = new(bool)
		job.LinkQueue(q.InternalQueueJobID[job.GetJobID()])
		return nil
	default:
		return errors.New("queue full")
	}
}

func (q *JobQueue) IsDone(id string) bool {
	if q.InternalQueueJobID[id] == nil {
		return false
	}
	return *q.InternalQueueJobID[id]
}

func (q *JobQueue) InQueue(id string) bool {
	if v := q.InternalQueueJobID[id]; v == nil {
		return false
	}
	return true
}

func (q *JobQueue) RemoveFromQueue(id string) error {
	if q.InQueue(id) && q.IsDone(id) {
		delete(q.InternalQueueJobID, id)
		return nil
	} else if q.InQueue(id) && !q.IsDone(id) {
		return errors.New("job " + id + " is in queue but is not done.")
	} else {
		return errors.New("job " + id + " is not in queue")
	}
}

// NewWorker - creates a new worker
func NewWorker(readyPool chan chan Job, done sync.WaitGroup) *Worker {
	return &Worker{
		done:             done,
		readyPool:        readyPool,
		assignedJobQueue: make(chan Job),
		quit:             make(chan bool),
	}
}

// Start - begins the job processing loop for the worker
func (w *Worker) Start() {
	go func() {
		w.done.Add(1)
		for {
			w.readyPool <- w.assignedJobQueue // check the job queue in
			select {
			case job := <-w.assignedJobQueue: // see if anything has been assigned to the queue
				job.Process()
				job.Done()
			case <-w.quit:
				w.done.Done()
				return
			}
		}
	}()
}

// Stop - stops the worker
func (w *Worker) Stop() {
	w.quit <- true
}
