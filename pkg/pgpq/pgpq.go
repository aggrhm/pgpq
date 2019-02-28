package pgpq

import (
	"fmt"
	"time"
	"errors"
	"strconv"
	"net/http"
	"encoding/json"
	"database/sql/driver"
	"github.com/gorilla/mux"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
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
	ID int							`json:"id"`
	Name string					`json:"name"`
	Capacity int				`json:"capacity"`
	JobsCount int				`json:"jobs_count"`
	IsLocked bool				`json:"is_locked"`
	MinPriority *int64	`json:"min_priority"`
	CreatedAt *time.Time	`json:"created_at"`
	UpdatedAt *time.Time	`json:"updated_at"`
}

func (q *Queue) IsFull() bool {
	return q.JobsCount >= q.Capacity
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

func NewJob() *Job {
	now := time.Now()
	j := &Job{}
	j.Quid = uuid.New().String()
	j.Priority = 0
	j.State = 10
	j.StateChangedAt = &now
	j.CreatedAt = &now
	return j
}

func (j *Job) ParseDataJSON(ds string) error {
	var m map[string]interface{}
	err := json.Unmarshal([]byte(ds), &m)
	j.Data = m
	return err
}

func (j *Job) SetDataValue(key string, val interface{}) {
	if j.Data == nil { j.Data = make(map[string]interface{}) }
	j.Data[key] = val
}

type JobStore interface {
	EnqueueJob(job *Job) error
	PeekJobs(queue_name string, count int) ([]Job, error)
	DequeueJobs(queue_name string, count int) ([]Job, error)
	ReleaseJob(id int64) error
	GetQueues() ([]Queue, error)
	GetQueue(name string, update bool) (*Queue, error)
	DeleteQueues() error
	ManageQueues() error
	ValidateDatabase() error
	PerformMigration() error
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
	log = logrus.New()
	store JobStore
)

func SetLogger(l *logrus.Logger) {
	log = l
}

func StartServer(port int, store_url string) {
	var err error

	fmt.Printf("Connecting to store.\n")
	store, err = NewPostgresJobStore(store_url)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Validating database.\n")
	err = store.ValidateDatabase()
	if err != nil {
		panic(err)
	}

	//fmt.Printf("Starting manager.\n")
	//go store.ManageQueues()

	fmt.Printf("Starting server.\n")
	router := mux.NewRouter()
	router.HandleFunc("/enqueue", EnqueueJobHandler).Methods("POST")
	router.HandleFunc("/enqueues", EnqueueJobsHandler).Methods("POST")
	router.HandleFunc("/peek", PeekJobsHandler).Methods("GET")
	router.HandleFunc("/dequeue", DequeueJobsHandler).Methods("POST")
	router.HandleFunc("/release", ReleaseJobHandler).Methods("POST")
	router.HandleFunc("/queues", GetQueuesHandler).Methods("GET")
	router.HandleFunc("/queue", GetQueueHandler).Methods("GET")
	router.HandleFunc("/queues", DeleteQueuesHandler).Methods("DELETE")

	srv := &http.Server{
		Handler: router,
		Addr: fmt.Sprintf(":%v", port),
		WriteTimeout: 10 * time.Second,
		ReadTimeout: 10 * time.Second,
	}
	log.Fatal(srv.ListenAndServe())
}

func PerformMigration(store_url string) {
	var err error

	fmt.Println("Connecting to store.")
	store, err = NewPostgresJobStore(store_url)
	if err != nil { panic(err) }

	fmt.Println("Performing migration.")
	err = store.PerformMigration()
	if err != nil {
		fmt.Println("Migration failed. ", err)
	}
	fmt.Println("Migration finished.")
}

func EnqueueJobHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	result := APIResult{Success: false}
	defer func() {
		writeResultToResponse(w, &result)
	}()

	// build job
	job := NewJob()
	job.QueueName = r.FormValue("queue_name")
	quid := r.FormValue("quid")
	if quid != "" { job.Quid = quid }
	job.Priority, err = strconv.ParseInt(r.FormValue("priority"), 10, 64)
	if ds := r.FormValue("data"); ds != "" {
		err := job.ParseDataJSON(ds)
		if err != nil {
			result.Error = &APIError{Message: "Could not parse data json.", Type: "InvalidParam"}
			return
		}
	}

	// enqueue job
	err = store.EnqueueJob(job)
	if err != nil {
		result.Error = err.(*APIError)
		return
	}
	result.SetData(job)
	result.Success = true
}

func EnqueueJobsHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	result := APIResult{Success: false}
	resdata := make([]interface{}, 0)
	defer func() {
		writeResultToResponse(w, &result)
	}()

	jobsds := r.FormValue("jobs")
	var jobsd []interface{}
	err = json.Unmarshal([]byte(jobsds), &jobsd)
	for _, jobd := range jobsd {
		jdm := jobd.(map[string]interface{})
		job := NewJob()
		job.QueueName, _ = jdm["queue_name"].(string)
		if quid, ok := jdm["quid"].(string); ok {
			job.Quid = quid
		}
		if pr, ok := jdm["priority"].(float64); ok {
			job.Priority = int64(pr)
		}
		if data, ok := jdm["data"].(map[string]interface{}); ok {
			job.Data = data
		}
		err = store.EnqueueJob(job)
		if err != nil {
			apierr := fmt.Errorf("Job error: %+v, %v", jdm, err)
			resdata = append(resdata, apierr)
			result.Error = err.(*APIError)
			continue
		}
		resdata = append(resdata, job)
	}
	result.SetData(resdata)
	result.Success = (result.Error == nil)
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

func GetQueueHandler(w http.ResponseWriter, r *http.Request) {
	result := APIResult{Success: false}
	defer func() {
		writeResultToResponse(w, &result)
	}()

	name := r.FormValue("name")
	update := r.FormValue("update") == "true"
	queue, err := store.GetQueue(name, update)
	if err != nil {
		result.Error = err.(*APIError)
		return
	}
	result.SetData(queue)
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
