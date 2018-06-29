package pgpq

import (
  "fmt"
  "log"
  "time"
  "strconv"
  "net/http"
  "encoding/json"
  "github.com/gorilla/mux"
)

type Queue struct {
  Id int
  Name string
  Capacity int
  JobsCount int
  IsLocked bool
  MinPriority int
}

type Job struct {
  Id int            `json:"id"`
  QueueName string  `json:"queue_name"`
  Quid string       `json:"quid"`
  Priority int      `json:"priority"`
  Data string       `json:"data"`
  State int         `json:"state,omitempty"`
  StateChangedAt *time.Time `json:"state_changed_at,omitempty"`
  CreatedAt *time.Time `json:"created_at,omitempty"`
}

type JobStore interface {
  EnqueueJob(job *Job) error
  PeekJobs(count int) ([]Job, error)
  DequeueJobs(count int) ([]Job, error)
  ReleaseJob(id int) error
  ManageQueues() error
}

type APIResult struct {
  Success bool      `json:"success"`
  Data interface{}  `json:"data"`
  Error *APIError   `json:"error,omitempty"`
}

type APIArrayResult struct {
  Success bool      `json:"success"`
  Data []interface{}  `json:"data"`
  Error *APIError   `json:"error,omitempty"`
}

type APIError struct {
  Message string    `json:"message,omitempty"`
  Type string       `json:"type,omitempty"`
}

func (err *APIError) Error() string {
  return err.Message;
}

var store JobStore

func StartServer() {
  var err error

  fmt.Printf("Connecting to store.\n")
  store, err = NewPostgresJobStore(notifs)
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
  log.Fatal(http.ListenAndServe(":8000", router))
}

func StartManager() {
  var err error
  store, err = NewPostgresJobStore()
  fmt.Printf("Connecting to store.\n")
  store, err = NewPostgresJobStore()
  if err != nil {
    panic(err)
  }
  store.ManageQueues()
}

func EnqueueJobHandler(w http.ResponseWriter, r *http.Request) {
  var err error
  result := APIResult{Success: false}
  now := time.Now()
  defer func() {
    json.NewEncoder(w).Encode(result)
  }()

  // build job
  job := Job{}
  job.QueueName = r.FormValue("queue_name")
  job.Quid = r.FormValue("quid")
  job.Priority, err = strconv.Atoi(r.FormValue("priority"))
  if err != nil {
    result.Error = &APIError{Message: "Invalid job priority.", Type: "InvalidParam"}
    return
  }
  job.Data = r.FormValue("data")
  job.State = 10
  job.StateChangedAt = &now
  job.CreatedAt = &now

  // enqueue job
  err = store.EnqueueJob(&job)
  if err != nil {
    result.Error = err.(*APIError)
    return
  }
  result.Data = &job
  result.Success = true
}

func PeekJobsHandler(w http.ResponseWriter, r *http.Request) {
  result := APIResult{Success: false}
  defer func() {
    json.NewEncoder(w).Encode(result)
  }()

  count, err := strconv.Atoi(r.FormValue("count"))
  if err != nil {
    result.Error = &APIError{Message: "Invalid count parameter.", Type: "InvalidParam"}
    return
  }
  jobs, err := store.PeekJobs(count)
  if (err != nil) {
    result.Error = err.(*APIError)
    return
  }
  result.Data = jobs
  result.Success = true
}

func DequeueJobsHandler(w http.ResponseWriter, r *http.Request) {
  result := APIResult{Success: false}
  defer func() {
    json.NewEncoder(w).Encode(result)
  }()

  count, err := strconv.Atoi(r.FormValue("count"))
  if err != nil {
    result.Error = &APIError{Message: "Invalid count parameter.", Type: "InvalidParam"}
    return
  }

  jobs, err := store.DequeueJobs(count)
  if (err != nil) {
    result.Error = err.(*APIError)
    return
  }
  result.Data = jobs
  result.Success = true
}

func ReleaseJobHandler(w http.ResponseWriter, r *http.Request) {
  result := APIResult{Success: false}
  defer func() {
    json.NewEncoder(w).Encode(result)
  }()

  jid, err := strconv.Atoi(r.FormValue("job_id"))
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
