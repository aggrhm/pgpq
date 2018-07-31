package pgpq

import (
  "fmt"
  "time"
  "database/sql"
  "github.com/lib/pq"
)

type PostgresJobStore struct {
  ConnectUrl string
  db *sql.DB
  listener *pq.Listener
  notifs chan string
}

func NewPostgresJobStore(url string) (*PostgresJobStore,error) {
  store := PostgresJobStore{}
  store.notifs = make(chan string)
  store.ConnectUrl = url
  pgdb, err := sql.Open("postgres", store.ConnectUrl)
  if err != nil {
    return nil, err
  }
  store.db = pgdb
  return &store, nil
}

func PGToAPIError(err error, prefix string) *APIError {
  msg := fmt.Sprintf("%s%s", prefix, err.Error())
  return &APIError{Message: msg, Type: "StoreError"}
}

func (s *PostgresJobStore) EnqueueJob(job *Job) error {
  queue := Queue{}
  // check if queue empty
  err := s.db.QueryRow("SELECT is_locked, COALESCE(min_priority, 0) FROM queues WHERE name = $1 LIMIT 1", job.QueueName).Scan(&queue.IsLocked, &queue.MinPriority)
  if err != nil {
    if err == sql.ErrNoRows {
      // create new queue
      err = s.db.QueryRow("INSERT INTO queues(name, capacity, created_at, updated_at) VALUES($1, $2, $3, $3) RETURNING id, name, is_locked", job.QueueName, 100000, time.Now()).Scan(&queue.ID, &queue.Name, &queue.IsLocked)
      if err != nil {
        return PGToAPIError(err, "")
      }
    } else {
      return PGToAPIError(err, "")
    }
  }

  if queue.IsLocked == true && job.Priority <= queue.MinPriority {
    return &APIError{Message: "Queue is full and new job has too low priority.", Type: "QueueFull"}
  }

  // insert row
  job_updated := false
  err = s.db.QueryRow("INSERT INTO jobs(queue_name, quid, priority, data, state, created_at) VALUES($1, $2, $3, $4, $5, $6) RETURNING id", job.QueueName, job.Quid, job.Priority, job.Data, job.State, job.CreatedAt).Scan(&job.ID)
  if err != nil {
    pe := err.(*pq.Error)
    if pe.Code.Name() == "unique_violation" {
      // if already in queue, update
      err = s.db.QueryRow("UPDATE jobs SET priority=$1, data=$2 WHERE queue_name=$3 AND quid=$4 RETURNING id", job.Priority, job.Data, job.QueueName, job.Quid).Scan(&job.ID)
      if err != nil {
        return PGToAPIError(err, "")
      }
      job_updated = true
    } else {
      return PGToAPIError(err, "")
    }
  }

  // delete lower job if needed
  if queue.IsLocked == true && !job_updated {
    _, err := s.db.Exec("DELETE FROM jobs WHERE id IN (SELECT id FROM jobs WHERE jobs.queue_name=$1 ORDER BY priority ASC LIMIT 1)", queue.Name)
    if err != nil {
      return PGToAPIError(err, "")
    }
  }

  s.NotifyJobsUpdated(job.QueueName)

  return nil
}

func (s * PostgresJobStore) PeekJobs(count int) ([]Job, error) {
  var jobs = make([]Job, 0, count)
  rows, err := s.db.Query("SELECT id, queue_name, quid, priority, data, state, created_at FROM jobs ORDER BY priority DESC LIMIT $1", count)
  if err != nil {
    return nil, PGToAPIError(err, "")
  }
  defer rows.Close()
  for rows.Next() {
    j := Job{}
    err := rows.Scan(&j.ID, &j.QueueName, &j.Quid, &j.Priority, &j.Data, &j.State, &j.CreatedAt)
    if err != nil {
      return nil, PGToAPIError(err, "")
    }
    jobs = append(jobs, j)
  }
  err = rows.Err()
  if err != nil {
    return nil, PGToAPIError(err, "")
  }
  return jobs, nil
}

func (s * PostgresJobStore) DequeueJobs(queue_name string, count int) ([]Job, error) {
  var jobs = make([]Job, 0, count)
  now := time.Now()
  rows, err := s.db.Query("UPDATE jobs SET state=20, state_changed_at=$1 WHERE id IN (SELECT id FROM jobs WHERE jobs.queue_name=$2 AND (jobs.state = 10 OR (jobs.state=20 AND jobs.state_changed_at < $3)) ORDER BY priority DESC LIMIT $4 FOR UPDATE SKIP LOCKED) RETURNING id, queue_name, quid, priority, data", now, queue_name, now.Add(time.Minute * -1), count)
  if err != nil {
    return nil, PGToAPIError(err, "Job update error: ")
  }
  defer rows.Close()
  for rows.Next() {
    j := Job{}
    err := rows.Scan(&j.ID, &j.QueueName, &j.Quid, &j.Priority, &j.Data)
    if err != nil {
      return nil, PGToAPIError(err, "Job scan error: ")
    }
    jobs = append(jobs, j)
  }
  err = rows.Err()
  if err != nil {
    return nil, PGToAPIError(err, "Job dequeue error: ")
  }
  return jobs, nil
}

func (s * PostgresJobStore) ReleaseJob(id int64) error {
  job := Job{}
  err := s.db.QueryRow("DELETE FROM jobs WHERE id=$1 RETURNING id, queue_name", id).Scan(&job.ID, &job.QueueName)
  if err != nil {
    return PGToAPIError(err, "")
  }
  s.NotifyJobsUpdated(job.QueueName)
  return nil
}

func (s * PostgresJobStore) NotifyJobsUpdated(queue_name string) error {
  /*
  _, err = s.db.Exec("SELECT pg_notify('jobs_updated',$1)", queue_name)
  if err != nil {
    fmt.Printf("Notify error.")
    return PGToAPIError(err)
  }
  */
  s.notifs <- queue_name
  return nil
}

func (s *PostgresJobStore) ManageQueues() error {
  var err error
  var lt time.Time
  var kp bool
  var qn string
  qm := make(map[string]time.Time)
  fmt.Printf("Listening for updates.\n")
  for {
    qn = <-s.notifs
    fmt.Printf("Notification received.\n")
    // check last update time
    lt, kp = qm[qn]
    if kp == false || (lt.Add(10 * time.Second).Before(time.Now())) {
      err = s.updateQueueStatus(qn)
      if err != nil {
        fmt.Printf("%v\n", err.Error())
      }
      qm[qn] = time.Now()
    }
  }
}

func (s *PostgresJobStore) updateQueueStatus(queue_name string) error {
  queue := Queue{}
  tx, err := s.db.Begin()
  if err != nil {
    return PGToAPIError(err, "")
  }
  // lock queue
  err = tx.QueryRow("SELECT id FROM queues WHERE name=$1 AND updated_at < $2 FOR UPDATE NOWAIT", queue_name, time.Now().Add(-10 * time.Second)).Scan(&queue.ID)
  if err != nil {
    return PGToAPIError(err, "")
  }

  // get count
  var count int
  var minp int
  err = tx.QueryRow("SELECT COUNT(*), COALESCE(MIN(priority), 0) FROM jobs WHERE jobs.queue_name=$1", queue_name).Scan(&count, &minp)
  if err != nil { return PGToAPIError(err, "") }

  // write count, priority to queue
  err = tx.QueryRow("UPDATE queues SET jobs_count=$1, min_priority=$2, is_locked=($1 >= capacity), updated_at=$3 WHERE id=$4 RETURNING name, is_locked, jobs_count, min_priority", count, minp, time.Now(), queue.ID).Scan(&queue.Name, &queue.IsLocked, &queue.JobsCount, &queue.MinPriority)
  if err != nil { return PGToAPIError(err, "") }

  // commit
  err = tx.Commit()
  if err != nil { return PGToAPIError(err, "") }

  fmt.Printf("Updating queue to %+v.\n", queue)
  return nil
}
