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
  MinPriority int64
  CreatedAt *time.Time
  UpdatedAt *time.Time
}

type Job struct {
  Id int64            `json:"id"`
  QueueName string  `json:"queue_name"`
  Quid string       `json:"quid"`
  Priority int64      `json:"priority"`
  Data *string       `json:"data"`
  State int         `json:"state,omitempty"`
  StateChangedAt *time.Time `json:"state_changed_at,omitempty"`
  CreatedAt *time.Time `json:"created_at,omitempty"`
}

type JobStore interface {
  EnqueueJob(job *Job) error
  PeekJobs(count int) ([]Job, error)
  DequeueJobs(queue_name string, count int) ([]Job, error)
  ReleaseJob(id int64) error
  ManageQueues() error
}

type APIResult struct {
  Success bool      `json:"success"`
  Data json.RawMessage  `json:"data"`
  Error *APIError   `json:"error,omitempty"`
}

func (r *APIResult) SetData(d interface{}) {
  r.Data, _ = json.Marshal(d)
}

type APIError struct {
  Message string    `json:"message,omitempty"`
  Type string       `json:"type,omitempty"`
}

func (err *APIError) Error() string {
  return err.Message;
}

var (
  store JobStore
)

func StartServer(store_url string) {
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
  log.Fatal(http.ListenAndServe(":8000", router))
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
    job.Data = &ds
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

func writeResultToResponse(w http.ResponseWriter, r *APIResult) {
  if !r.Success {
    w.WriteHeader(http.StatusBadRequest)
  }
  json.NewEncoder(w).Encode(r)
}
