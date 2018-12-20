package pgpq

import (
	"time"
	"sync"
)

type BulkService struct {
	Client				*Client
	FlushInterval	time.Duration
	BulkJobs			int
	AfterEnqueue	func(*APIResult, error)

	workers				[]*BulkServiceWorker
	jobC					chan *Job
	wg						*sync.WaitGroup
}

func NewBulkService(client *Client) *BulkService {
	p := &BulkService{}
	p.Client = client
	p.FlushInterval = time.Second * 3
	p.BulkJobs = 500
	p.wg = &sync.WaitGroup{}
	return p
}

func (bp *BulkService) EnqueueJob(job *Job) error {
	bp.jobC <- job
	return nil
}

func (bp *BulkService) EnqueueJobs(jobs []*Job) error {
	for _, job := range jobs {
		bp.jobC <- job
	}
	return nil
}

func (bp *BulkService) Start() {
	bp.jobC = make(chan *Job, bp.BulkJobs)
	w := NewBulkServiceWorker(bp)
	bp.wg.Add(1)
	go w.Run(bp.wg, bp.jobC)
	bp.workers = append(bp.workers, w)
	log.Trace("PGPQ: Workers started.")
}

func (bp *BulkService) Stop() {
	log.Trace("PGPQ: Stopping workers...")
	bp.sendWorkerControl("done")
	bp.workers = make([]*BulkServiceWorker, 0)
	bp.wg.Wait()
	log.Trace("PGPQ: Workers stopped.")
}

func (bp *BulkService) Close() {
	bp.Stop()
}

func (bp *BulkService) Flush() {
	bp.sendWorkerControl("flush")
}

func (bp *BulkService) sendWorkerControl(cmd string) {
	for _, w := range bp.workers {
		log.Trace("Sending command to worker: ", cmd)
		w.ControlC <- cmd
	}
}

type BulkServiceWorker struct {
	Service			*BulkService
	ControlC		chan string

	waitingJobs []*Job
	ticker			*time.Ticker
}

func NewBulkServiceWorker(service *BulkService) *BulkServiceWorker {
	w := &BulkServiceWorker{}
	w.Service = service
	w.ControlC = make(chan string)
	w.waitingJobs = make([]*Job, 0, service.BulkJobs)
	return w
}

func (w *BulkServiceWorker) Run(wg *sync.WaitGroup, jobC chan *Job) {
	p := w.Service
	w.ticker = time.NewTicker(p.FlushInterval)
	log.Trace("PGPQ: Starting worker.")
	done := false
	for {
		select {
		case cmd, ok := <-w.ControlC:
			log.Trace("PGPQ: Worker received command: ", cmd)
			if !ok || cmd == "done" { w.Flush(); done = true; break }
			if cmd == "flush" {
				w.Flush()
			}
		case job, ok := <-jobC:
			if !ok { w.Flush(); done = true; break }
			w.waitingJobs = append(w.waitingJobs, job)
			if len(w.waitingJobs) >= p.BulkJobs {
				w.Flush()
			}
		case <- w.ticker.C:
			w.Flush()
		}
		if done { break }
	}
	log.Trace("PGPQ: Stopping worker.")
	wg.Done()
}

func (w *BulkServiceWorker) Flush() error {
	count := len(w.waitingJobs)
	log.Tracef("PGPQ: Flushing %v jobs", count)
	if count == 0 { return nil }
	p := w.Service
	result, err := p.Client.EnqueueJobs(w.waitingJobs)
	if p.AfterEnqueue != nil {
		p.AfterEnqueue(result, err)
	}
	w.waitingJobs = make([]*Job, 0, p.BulkJobs)
	return err
}

