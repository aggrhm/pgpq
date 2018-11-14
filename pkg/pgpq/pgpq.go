package pgpq

import (
	"fmt"
	"log"
	"time"
	"errors"
	"strconv"
	"net/http"
	"encoding/json"
	"database/sql/driver"
	"github.com/gorilla/mux"
)

type DataMap map[string]interface{}

func (dm DataMap) Value() (driver.Value, error) {
	j, err := json.Marshal(dm)
	return j, err
}

func (dm *DataMap) Scan(value interface{}) error {
	if value == nil {
		dm = nil
		return nil
	}
	bs, ok := value.([]byte)
	if !ok {
		return errors.New("Cannot convert to bytes.")
	}
	var m map[string]interface{}
	err := json.Unmarshal(bs, &m)
	if err != nil { return err }

	*dm = m
	return nil
}

type Queue struct {
	ID int
	Name string
	Capacity int
	JobsCount int
	IsLocked bool
	MinPriority *int64
	CreatedAt *time.Time
	UpdatedAt *time.Time
}

type Job struct {
	ID int64						`json:"id"`
	QueueName string		`json:"queue_name"`
	Quid string					`json:"quid"`
	Priority int64			`json:"priority"`
	Data DataMap				`json:"data"`
	State int						`json:"state,omitempty"`
	StateChangedAt *time.Time `json:"state_changed_at,omitempty"`
	CreatedAt *time.Time `json:"created_at,omitempty"`
}

func (j *Job) ParseDataJSON(ds string) error {
	var m map[string]interface{}
	err := json.Unmarshal([]byte(ds), &m)
	j.Data = m
	return err
}

type JobStore interface {
	EnqueueJob(job *Job) error
	PeekJobs(queue_name string, count int) ([]Job, error)
	DequeueJobs(queue_name string, count int) ([]Job, error)
	ReleaseJob(id int64) error
	GetQueues() ([]Queue, error)
	DeleteQueues() error
	ManageQueues() error
}

type APIResult struct {
	Success bool			`json:"success"`
	Data json.RawMessage	`json:"data"`
	Error *APIError		`json:"error,omitempty"`
}

func (r *APIResult) SetData(d interface{}) {
	r.Data, _ = json.Marshal(d)
}

type APIError struct {
	Message string		`json:"message,omitempty"`
	Type string				`json:"type,omitempty"`
}

func (err *APIError) Error() string {
	return err.Message;
}

var (
	store JobStore
)

func StartServer(port int, store_url string) {
	var err error

	fmt.Printf("Connecting to store.\n")
	store, err = NewPostgresJobStore(store_url)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Starting manager.\n")
	go store.ManageQueues()

	fmt.Printf("Starting server.\n")
	router := mux.NewRouter()
	router.HandleFunc("/enqueue", EnqueueJobHandler).Methods("POST")
	router.HandleFunc("/peek", PeekJobsHandler).Methods("GET")
	router.HandleFunc("/dequeue", DequeueJobsHandler).Methods("POST")
	router.HandleFunc("/release", ReleaseJobHandler).Methods("POST")
	router.HandleFunc("/queues", GetQueuesHandler).Methods("GET")
	router.HandleFunc("/queues", DeleteQueuesHandler).Methods("DELETE")
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%v", port), router))
}

func EnqueueJobHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	result := APIResult{Success: false}
	now := time.Now()
	defer func() {
		writeResultToResponse(w, &result)
	}()

	// build job
	job := Job{}
	job.QueueName = r.FormValue("queue_name")
	job.Quid = r.FormValue("quid")
	job.Priority, err = strconv.ParseInt(r.FormValue("priority"), 10, 64)
	if err != nil {
		job.Priority = 0
	}
	if ds := r.FormValue("data"); ds != "" {
		err := job.ParseDataJSON(ds)
		if err != nil {
			result.Error = &APIError{Message: "Could not parse data json.", Type: "InvalidParam"}
			return
		}
	}
	job.State = 10
	job.StateChangedAt = &now
	job.CreatedAt = &now

	// enqueue job
	err = store.EnqueueJob(&job)
	if err != nil {
		result.Error = err.(*APIError)
		return
	}
	result.SetData(job)
	result.Success = true
}

func PeekJobsHandler(w http.ResponseWriter, r *http.Request) {
	result := APIResult{Success: false}
	defer func() {
		writeResultToResponse(w, &result)
	}()

	queue_name := r.FormValue("queue_name")
	if queue_name == "" {
		result.Error = &APIError{Message: "Invalid queue_name parameter.", Type: "InvalidParam"}
	}
	count, err := strconv.Atoi(r.FormValue("count"))
	if err != nil {
		result.Error = &APIError{Message: "Invalid count parameter.", Type: "InvalidParam"}
		return
	}
	jobs, err := store.PeekJobs(queue_name, count)
	if (err != nil) {
		result.Error = err.(*APIError)
		return
	}
	result.SetData(jobs)
	result.Success = true
}

func DequeueJobsHandler(w http.ResponseWriter, r *http.Request) {
	result := APIResult{Success: false}
	defer func() {
		writeResultToResponse(w, &result)
	}()

	queue_name := r.FormValue("queue_name")
	count, err := strconv.Atoi(r.FormValue("count"))
	if err != nil {
		result.Error = &APIError{Message: "Invalid count parameter.", Type: "InvalidParam"}
		return
	}

	jobs, err := store.DequeueJobs(queue_name, count)
	if (err != nil) {
		result.Error = err.(*APIError)
		return
	}
	result.SetData(jobs)
	result.Success = true
}

func ReleaseJobHandler(w http.ResponseWriter, r *http.Request) {
	result := APIResult{Success: false}
	defer func() {
		writeResultToResponse(w, &result)
	}()

	jid, err := strconv.ParseInt(r.FormValue("job_id"), 10, 64)
	if err != nil {
		result.Error = &APIError{Message: "Invalid job id.", Type: "InvalidParam"}
		return
	}

	err = store.ReleaseJob(jid)
	if (err != nil) {
		result.Error = err.(*APIError)
		return
	}
	result.Success = true
}

func GetQueuesHandler(w http.ResponseWriter, r *http.Request) {
	result := APIResult{Success: false}
	defer func() {
		writeResultToResponse(w, &result)
	}()

	queues, err := store.GetQueues()
	if err != nil {
		result.Error = err.(*APIError)
		return
	}
	result.SetData(queues)
	result.Success = true
}

func DeleteQueuesHandler(w http.ResponseWriter, r *http.Request) {
	result := APIResult{Success: false}
	defer func() {
		writeResultToResponse(w, &result)
	}()

	if r.FormValue("force") != "true" {
		result.Error = &APIError{Message: "Please add force=true param.", Type: "InvalidParam"}
		return
	}
	err := store.DeleteQueues()
	if err != nil {
		result.Error = err.(*APIError)
		return
	}
	result.Success = true
}

func writeResultToResponse(w http.ResponseWriter, r *APIResult) {
	if !r.Success {
		w.WriteHeader(http.StatusBadRequest)
	}
	json.NewEncoder(w).Encode(r)
}
