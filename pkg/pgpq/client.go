package pgpq

import (
	"fmt"
	"time"
	"io/ioutil"
	nurl "net/url"
	"net/http"
	"strconv"
	"encoding/json"
)

type QueueCacheEntry struct {
	Queue				*Queue
	UpdatedAt		time.Time
}

type Client struct {
	HostUrl			string
	HttpClient	*http.Client
	QueueCache	map[string]*QueueCacheEntry
}

func NewClient(url string) *Client {
	c := Client{}
	c.HostUrl = url
	c.HttpClient = &http.Client{Timeout: (15 * time.Second)}
	c.QueueCache = make(map[string]*QueueCacheEntry)
	return &c
}

func (c *Client) EnqueueJob(job *Job) (*APIResult, error) {
	hc := c.HttpClient
	url := fmt.Sprintf("%s/enqueue", c.HostUrl)
	data := nurl.Values{}
	data.Set("queue_name", job.QueueName)
	data.Set("quid", job.Quid)
	data.Set("priority", strconv.FormatInt(job.Priority, 10))
	if job.Data != nil {
		djs, err := json.Marshal(job.Data)
		if err != nil { return nil, err }
		data.Set("data", string(djs))
	}
	resp, err := hc.PostForm(url, data)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	res, err := parseResult(resp)
	if err != nil {
		return res, err
	}

	err = json.Unmarshal(res.Data, job)
	if err != nil { return res, err }
	return res, nil
}

func (c *Client) EnqueueJobs(jobs []*Job) (*APIResult, error) {
	hc := c.HttpClient
	url := fmt.Sprintf("%s/enqueues", c.HostUrl)
	data := nurl.Values{}
	jjsn, err := json.Marshal(jobs)
	data.Set("jobs", string(jjsn))
	resp, err := hc.PostForm(url, data)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	res, err := parseResult(resp)
	return res, err
}

func (c *Client) DequeueJobs(queue_name string, count int, timeout time.Duration) ([]Job, error) {
	jobs := make([]Job, 0, count)

	hc := c.HttpClient
	url := fmt.Sprintf("%s/dequeue", c.HostUrl)
	data := nurl.Values{}
	data.Set("queue_name", queue_name)
	data.Set("count", strconv.Itoa(count))
	if timeout > 0 {
		data.Set("timeout", strconv.Itoa(int(timeout.Seconds())))
	}
	resp, err := hc.PostForm(url, data)
	if err != nil {
		return jobs, err
	}
	defer resp.Body.Close()
	res, err := parseResult(resp)
	if err != nil {
		return jobs, err
	}

	err = json.Unmarshal(res.Data, &jobs)
	if err != nil { return jobs, err }
	return jobs, nil
}

func (c *Client) ReleaseJob(job_id int64) error {
	hc := c.HttpClient
	url := fmt.Sprintf("%s/release", c.HostUrl)
	data := nurl.Values{}
	data.Set("job_id", strconv.FormatInt(job_id, 10))
	resp, err := hc.PostForm(url, data)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, err = parseResult(resp)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) GetQueue(name string, update bool) (*Queue, error) {
	queue := new(Queue)
	hc := c.HttpClient
	updstr := "false"
	if update { updstr = "true" }
	url := fmt.Sprintf("%s/queue?name=%s&update=%s", c.HostUrl, name, updstr)
	resp, err := hc.Get(url)
	if err != nil { return nil, err }
	defer resp.Body.Close()
	res, err := parseResult(resp)
	if err != nil { return nil, err }
	err = json.Unmarshal(res.Data, &queue)
	if err != nil { return queue, err }
	return queue, nil
}

func (c *Client) GetCachedQueue(name string, staled time.Duration) (*Queue, error) {
	cache := c.QueueCache
	// check if cached
	qe := cache[name]
	if qe != nil && qe.UpdatedAt.After(time.Now().Add(-1 * staled)) {
		return qe.Queue, nil
	}
	// otherwise, fetch queue
	queue, err := c.GetQueue(name, false)
	if err != nil { return nil, err }
	cache[name] = &QueueCacheEntry{Queue: queue, UpdatedAt: time.Now()}
	return queue, nil
}

func parseResult(resp *http.Response) (*APIResult, error) {
	res := APIResult{}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(body, &res)
	if err != nil {
		return nil, err
	}
	if !res.Success {
		return &res, res.Error
	}
	return &res, nil
}
