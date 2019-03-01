package pgpq

import (
	"os"
	"fmt"
	"time"
	"database/sql"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/golang-migrate/migrate"
	migrate_pg "github.com/golang-migrate/migrate/database/postgres"
	_ "github.com/golang-migrate/migrate/source/file"
)

const (
	PostgresDBVersion = 20180624195613
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
	err = pgdb.Ping()
	if err != nil { return nil, fmt.Errorf("PostgresJobStore: Could not establish connection with database (%s)", err) }
	store.db = pgdb

	return &store, nil
}

func PGToAPIError(err error, prefix string) *APIError {
	msg := fmt.Sprintf("%s%s", prefix, err.Error())
	log.Error(prefix, err)
	return &APIError{Message: msg, Type: "StoreError"}
}

func (s *PostgresJobStore) EnqueueJob(job *Job) error {
	queue := Queue{}

	// prepare job
	err := job.Prepare()
	if err != nil { return &APIError{Message: ("Job could not be prepared: " + err.Error()), Type: "JobError"} }

	// check if queue empty
	err = s.db.QueryRow("SELECT id, name, jobs_count, capacity, min_priority FROM queues WHERE name = $1 LIMIT 1", job.QueueName).Scan(&queue.ID, &queue.Name, &queue.JobsCount, &queue.Capacity, &queue.MinPriority)
	if err != nil {
		if err == sql.ErrNoRows {
			// create new queue
			err = s.db.QueryRow("INSERT INTO queues(name, capacity, created_at, updated_at) VALUES($1, $2, $3, $3) RETURNING id, name", job.QueueName, 1000000, time.Now()).Scan(&queue.ID, &queue.Name)
			if err != nil {
				return PGToAPIError(err, "")
			}
		} else {
			return PGToAPIError(err, "")
		}
	}

	if queue.IsFull() && queue.MinPriority != nil && job.Priority <= *queue.MinPriority {
		return &APIError{Message: "Queue is full and new job has too low priority.", Type: "QueueFull"}
	}

	// insert job row
	tx, err := s.db.Begin()
	if err != nil { rollbackTx(tx); return PGToAPIError(err, "Could not start transaction.") }

	err = tx.QueryRow("INSERT INTO jobs(queue_name, quid, priority, data, state, created_at) VALUES($1, $2, $3, $4, $5, $6) ON CONFLICT (queue_name, quid) DO UPDATE SET priority=GREATEST(jobs.priority, EXCLUDED.priority), data=EXCLUDED.data, state_changed_at=$6 RETURNING id, priority, data, state, state_changed_at, created_at", job.QueueName, job.Quid, job.Priority, job.Data, 10, time.Now()).Scan(&job.ID, &job.Priority, &job.Data, &job.State, &job.StateChangedAt, &job.CreatedAt)
	if err != nil { rollbackTx(tx); return PGToAPIError(err, "") }

	// if new job, do some upkeep
	if job.StateChangedAt == nil {
		if queue.IsFull() {
			// delete lower job if needed
			var new_min_pri int
			err = tx.QueryRow("DELETE FROM jobs WHERE id IN (SELECT id FROM jobs AS ijs WHERE ijs.queue_name=$1 ORDER BY priority ASC NULLS FIRST LIMIT 1 FOR UPDATE SKIP LOCKED) RETURNING priority", queue.Name).Scan(&new_min_pri)
			if err != nil { rollbackTx(tx); return PGToAPIError(err, "enqueue delete") }
			err = tx.QueryRow("UPDATE queues SET min_priority=$1 WHERE id = $2 RETURNING min_priority", new_min_pri, queue.ID).Scan(&queue.MinPriority)
		} else {
			// increment queue jobs count
			_, err = tx.Exec("UPDATE queues SET jobs_count = jobs_count + 1 WHERE id = $1", queue.ID)
			if err != nil { rollbackTx(tx); return PGToAPIError(err, "") }
		}
	}

	// commit
	err = tx.Commit()
	if err != nil { return PGToAPIError(err, "Could not commit transaction.") }

	return nil
}

func (s * PostgresJobStore) PeekJobs(queue_name string, count int) ([]Job, error) {
	var jobs = make([]Job, 0, count)
	rows, err := s.db.Query("SELECT id, queue_name, quid, priority, data, state, created_at FROM jobs WHERE queue_name = $1 ORDER BY priority DESC NULLS LAST LIMIT $2", queue_name, count)
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
	rows, err := s.db.Query("UPDATE jobs SET state=20, state_changed_at=$1 WHERE id IN (SELECT id FROM jobs WHERE jobs.queue_name=$2 AND (jobs.state = 10 OR (jobs.state=20 AND jobs.state_changed_at < $3)) ORDER BY priority DESC NULLS LAST LIMIT $4 FOR UPDATE SKIP LOCKED) RETURNING id, queue_name, quid, priority, data", now, queue_name, now.Add(time.Minute * -1), count)
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
	tx, err := s.db.Begin()
	if err != nil { return PGToAPIError(err, "Could not start transaction.") }
	err = tx.QueryRow("DELETE FROM jobs WHERE id=$1 RETURNING id, queue_name", id).Scan(&job.ID, &job.QueueName)
	if err != nil { rollbackTx(tx); return PGToAPIError(err, "") }

	_, err = tx.Exec("UPDATE queues SET jobs_count = jobs_count - 1 WHERE name = $1", job.QueueName)
	if err != nil { rollbackTx(tx); return PGToAPIError(err, "") }

	err = tx.Commit()
	if err != nil { return PGToAPIError(err, "Could not commit transaction.") }
	return nil
}

func (s *PostgresJobStore) GetQueues() ([]Queue, error) {
	var queues = make([]Queue, 0)
	rows, err := s.db.Query("SELECT id, name, capacity, jobs_count, is_locked, min_priority, created_at, updated_at FROM queues ORDER BY name ASC")
	if err != nil {
		return nil, PGToAPIError(err, "")
	}
	defer rows.Close()
	for rows.Next() {
		q := Queue{}
		err := rows.Scan(&q.ID, &q.Name, &q.Capacity, &q.JobsCount, &q.IsLocked, &q.MinPriority, &q.CreatedAt, &q.UpdatedAt)
		if err != nil {
			return nil, PGToAPIError(err, "")
		}
		queues = append(queues, q)
	}
	err = rows.Err()
	if err != nil {
		return nil, PGToAPIError(err, "")
	}
	return queues, nil
}

func (s *PostgresJobStore) GetQueue(name string, update bool) (*Queue, error) {
	q := new(Queue)
	err := s.db.QueryRow("SELECT id, name, capacity, jobs_count, is_locked, min_priority, created_at, updated_at FROM queues WHERE name=$1 LIMIT 1", name).Scan(&q.ID, &q.Name, &q.Capacity, &q.JobsCount, &q.IsLocked, &q.MinPriority, &q.CreatedAt, &q.UpdatedAt)
	if err != nil {
		return nil, PGToAPIError(err, "Could not find queue.")
	}
	if update == true {
		// need to update queue
		err := s.updateQueueStatus(q)
		if err != nil {
			return nil, PGToAPIError(err, "Could not update queue status.")
		}
	}
	return q, nil
}

func (s * PostgresJobStore) DeleteQueues() error {
	_, err := s.db.Exec("DELETE FROM jobs")
	if err != nil { return PGToAPIError(err, "Could not delete jobs.") }
	_, err = s.db.Exec("DELETE FROM queues")
	if err != nil { return PGToAPIError(err, "Could not delete jobs.") }
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
		//fmt.Printf("Notification received.\n")
		// check last update time
		lt, kp = qm[qn]
		if kp == false || (lt.Add(10 * time.Second).Before(time.Now())) {
			err = s.updateQueueStatus(&Queue{Name: qn})
			if err != nil {
				fmt.Printf("Could not update queue status. %v\n", err.Error())
			}
			qm[qn] = time.Now()
		}
	}
}

func (s *PostgresJobStore) PerformMigration() error {
	mgr := s.getMigrator()
	err := mgr.Up()
	if err != nil { return errors.Wrap(err, "Could not migrate database") }
	return nil
}

func (s *PostgresJobStore) ValidateDatabase() error {
	mgr := s.getMigrator()
	ver, dirty, err := mgr.Version()
	if err != nil {
		return errors.Wrap(err, "Could not get database version")
	}
	if dirty {
		return errors.Errorf("Database version %v is currently dirty.", ver)
	}
	if ver != PostgresDBVersion {
		return errors.Errorf("Database version is currently %v, should be %v", ver, PostgresDBVersion)
	}
	return nil
}

func (s *PostgresJobStore) getMigrator() *migrate.Migrate {
	homedir := os.Getenv("PGPQ_HOME")
	//fmt.Println("homedir: ", homedir)
	if homedir == "" {	panic("Home directory for pgpq not specified. Please set $PGPQ_HOME.") }
	msp := "file://" + homedir + "/db/pg"
	pgd, err := migrate_pg.WithInstance(s.db, &migrate_pg.Config{})
	mgr, err := migrate.NewWithDatabaseInstance(msp, "postgres", pgd)
	if err != nil {
		panic(errors.Wrap(err, "Could not create migration manager"))
	}
	return mgr
}

func (s *PostgresJobStore) CleanQueue(queue * Queue) error {
	var del_count int64

	log.Debugf("Cleaning queue %v", queue.Name)
	tstart := time.Now()
	tx, err := s.db.Begin()
	if err != nil { return PGToAPIError(err, "Could not start transaction.")
	}

	// try to lock queue if full
	// NOTE: move this to separate transaction to set is_locked if updated_at
	// is old, similar to claiming jobs. This way the queue row won't be
	// locked during entire transaction.
	err = tx.QueryRow("UPDATE queues SET is_locked = 't' WHERE id=$1 AND is_locked='f' AND jobs_count > capacity RETURNING jobs_count, capacity", queue.ID).Scan(&queue.JobsCount, &queue.Capacity)
	if err != nil { rollbackTx(tx); return PGToAPIError(err, "") }

	overflow := queue.JobsCount - queue.Capacity

	// delete any old jobs for queue
	res, err := tx.Exec("DELETE FROM jobs WHERE id IN (SELECT id FROM jobs WHERE jobs.queue_name=$1 ORDER BY priority ASC NULLS FIRST LIMIT $2)", queue.Name, overflow)
	if err != nil { rollbackTx(tx); return PGToAPIError(err, "") }

	del_count, err = res.RowsAffected()
	if err != nil { rollbackTx(tx); return PGToAPIError(err, "") }
	if del_count == 0 { rollbackTx(tx); return nil }

	// get lowest priority job
	err = tx.QueryRow("SELECT priority FROM jobs WHERE jobs.queue_name=$1 ORDER BY priority ASC NULLS FIRST LIMIT 1", queue.Name).Scan(&queue.MinPriority)
	if err != nil { rollbackTx(tx); return PGToAPIError(err, "") }

	// update queue
	_, err = tx.Exec("UPDATE queues SET min_priority=$1, jobs_count = (jobs_count - $2), is_locked = 'f', updated_At = $3, WHERE id = $4", queue.MinPriority, del_count, time.Now(), queue.ID)
	if err != nil { rollbackTx(tx); return PGToAPIError(err, "") }

	// commit
	err = tx.Commit()
	if err != nil { return PGToAPIError(err, "Could not commit transaction.") }

	log.Infof("Cleaned %v jobs from queue %v (took %s)", del_count, queue.Name, time.Now().Sub(tstart).String())
	return nil
}

func rollbackTx(tx *sql.Tx) {
	err := tx.Rollback()
	if err != nil {
		log.Error("Could not rollback transaction")
	}
}

// deprecated
func (s *PostgresJobStore) updateQueueStatus(queue *Queue) error {
	tstart := time.Now()
	tx, err := s.db.Begin()
	if err != nil { return PGToAPIError(err, "Could not start transaction.") }
	// lock queue
	err = tx.QueryRow("SELECT id FROM queues WHERE name=$1 AND updated_at < $2 FOR UPDATE NOWAIT", queue.Name, time.Now().Add(-10 * time.Second)).Scan(&queue.ID)
	if err != nil {
		return PGToAPIError(err, "Could not load queue. It might be processed by another thread.")
	}

	// get count
	var count int
	var minp int
	err = tx.QueryRow("SELECT COUNT(*), COALESCE(MIN(priority), 0) FROM jobs WHERE jobs.queue_name=$1", queue.Name).Scan(&count, &minp)
	if err != nil { return PGToAPIError(err, "Could not fetch job count.") }

	// write count, priority to queue
	tn := time.Now(); queue.UpdatedAt = &tn
	err = tx.QueryRow("UPDATE queues SET jobs_count=$1, min_priority=$2, is_locked=($1 >= capacity), updated_at=$3 WHERE id=$4 RETURNING name, is_locked, jobs_count, min_priority", count, minp, *queue.UpdatedAt, queue.ID).Scan(&queue.Name, &queue.IsLocked, &queue.JobsCount, &queue.MinPriority)
	if err != nil { return PGToAPIError(err, "Could not update queue status.") }

	// commit
	err = tx.Commit()
	if err != nil { return PGToAPIError(err, "Could not commit transaction.") }

	tend := time.Now()
	fmt.Printf("Updating queue to %+v (took %v).\n", queue, tend.Sub(tstart).String())
	return nil
}

