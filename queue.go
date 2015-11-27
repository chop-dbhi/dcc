package dcc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/jpillora/backoff"
	"github.com/labstack/echo"
	"github.com/satori/go.uuid"
	"golang.org/x/net/context"
)

const (
	// Default number of attempts to try to notify
	NotifyAttempts = 5
)

var (
	defaultBackoff = backoff.Backoff{
		Jitter: true,
	}

	ErrQueueExecRequired = errors.New("queue: Exec function is required.")
)

// Params is a map of key-value pairs that are used for input and output.
type Params map[string]interface{}

func (p Params) Has(k string) bool {
	_, ok := p[k]
	return ok
}

func (p Params) String(k string) string {
	if v, ok := p[k]; ok {
		return v.(string)
	}

	return ""
}

func (p Params) Int(k string) int {
	if v, ok := p[k]; ok {
		return v.(int)
	}

	return 0
}

func (p Params) Bool(k string) bool {
	if v, ok := p[k]; ok {
		return v.(bool)
	}

	return false
}

func (p Params) Float(k string) float64 {
	if v, ok := p[k]; ok {
		return v.(float64)
	}

	return 0
}

// State denotes the state a task is in.
type State uint8

func (s State) String() string {
	switch s {
	case Created:
		return "created"

	case Queued:
		return "queued"

	case Running:
		return "running"

	case Finished:
		return "finished"

	default:
		return "unknown"
	}
}

const (
	Created State = iota + 1
	Queued
	Running
	Finished
)

// Task that performs some kind of validation. Fields should
// be added that correspond to the input parameters.
type Task struct {
	// Params contains the actual parameters for the task itself.
	Params Params

	// UUID is the unique identifier of the task.
	id uuid.UUID

	// Timestamps.
	started time.Time
	ended   time.Time

	// A callback URL that will be used by the task's queue.
	// to notify the client that the work is done.
	callback string
	token    string

	// Context is used to enforce guarantees for the lifecycle of the task.
	cxt context.Context

	// State of the task.
	state State

	output Params
	err    error

	backoff backoff.Backoff
	logger  *logrus.Logger
}

// ID returns the ID of the task.
func (t *Task) ID() string {
	return t.id.String()
}

// Started returns the time the task started.
func (t *Task) Started() time.Time {
	return t.started
}

// Ended returns the time the task ended.
func (t *Task) Ended() time.Time {
	return t.ended
}

func (t *Task) MarshalJSON() ([]byte, error) {
	aux := map[string]interface{}{
		"id":      t.id,
		"started": nil,
		"ended":   nil,
		"state":   t.state.String(),
		"error":   t.err,
		"output":  t.output,
	}

	if !t.started.IsZero() {
		aux["started"] = t.started
	}

	if !t.ended.IsZero() {
		aux["ended"] = t.ended
	}

	return json.Marshal(aux)
}

func (t *Task) UnmarshalJSON(b []byte) error {
	aux := make(map[string]interface{})

	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}

	var (
		ok bool
		v  interface{}
	)

	if v, ok = aux["callback"]; ok {
		t.callback = v.(string)
	}

	if v, ok = aux["token"]; ok {
		t.token = v.(string)
	}

	if v, ok = aux["params"]; ok {
		t.Params = v.(map[string]interface{})
	}

	return nil
}

// State returns the state of the task.
func (t *Task) State() State {
	return t.state
}

// Error impliments the error interface.
func (t *Task) Error() string {
	return fmt.Sprint(t.err)
}

// Queue is a service type that queues and operates on tasks. Tasks are enqueued
// by a POST request to the /task endpoint.
type Queue struct {
	// The number of workers to operate on the queue. This increases the
	// parallelism of the queue.
	Workers uint

	// Capacity of the queue.
	Capacity uint

	// Execute a task.
	Exec func(*Task) (Params, error)

	// Cancel cancels a running task.
	Cancel func(*Task) error

	// Reference to service.
	service *Service

	mux     sync.Mutex
	queue   chan *Task
	outbox  chan *Task
	tasks   map[uuid.UUID]*Task
	workers *sync.WaitGroup
}

// Task initializes a new task.
func (q *Queue) NewTask() *Task {
	id := uuid.NewV4()

	t := &Task{
		id:      id,
		cxt:     q.service.Context(),
		backoff: defaultBackoff,
		logger:  q.service.Logger,
	}

	q.tasks[t.id] = t

	return t
}

// Executes a task with the ability to cancel it.
func (q *Queue) exec(t *Task) {
	q.service.Logger.WithFields(logrus.Fields{
		"task": t.id,
	}).Infof("queue: executing task")

	q.service.available = false
	t.state = Running
	t.started = time.Now().UTC()

	cxt := t.cxt

	outch := make(chan Params)
	errch := make(chan error)

	go func() {
		p, err := q.Exec(t)

		if err != nil {
			errch <- err
		} else {
			outch <- p
		}
	}()

outer:
	for {
		select {
		case <-cxt.Done():
			t.err = cxt.Err()
			break outer

		case err := <-errch:
			t.err = err
			break outer

		case out := <-outch:
			t.output = out
			break outer
		}
	}

	t.state = Finished
	t.ended = time.Now().UTC()

	q.service.available = true

	// Send task to outbox.
	q.outbox <- t
}

func (q *Queue) notify(t *Task) error {
	if t.callback == "" {
		return nil
	}

	buf := bytes.Buffer{}

	if t.output != nil {
		if err := json.NewEncoder(&buf).Encode(t.output); err != nil {
			return err
		}
	}

	t.backoff.Reset()

	var (
		err  error
		resp *http.Response
	)

	client := http.Client{
		Timeout: 5 * time.Second,
	}

	req, err := http.NewRequest("POST", t.callback, &buf)

	if err != nil {
		panic(err)
	}

	req.Header.Set("content-type", "application/json")

	if t.token != "" {
		req.Header.Set("token", t.token)
	}

	// Attempt to notify multiple times.
	for i := 0; i < NotifyAttempts; i++ {
		resp, err = client.Do(req)

		if err != nil {
			continue
		}

		// 200 range, good to go.
		if resp.StatusCode > 200 && resp.StatusCode < 300 {
			return nil
		}

		time.Sleep(t.backoff.Duration())
	}

	// Error making the request.
	if err != nil {
		return err
	}

	// HTTP error.
	return fmt.Errorf("Notify error: %q", resp.Status)
}

// Init initialize the queue prior to starting.
func (q *Queue) Init(s *Service) error {
	if q.Exec == nil {
		return ErrQueueExecRequired
	}

	// Default to the number of cores.
	if q.Workers == 0 {
		q.Workers = uint(runtime.NumCPU())
	}

	if q.Capacity == 0 {
		q.Capacity = 1
	}

	// Reference for use throughout.
	q.service = s

	q.mux = sync.Mutex{}
	q.queue = make(chan *Task, q.Capacity)
	q.outbox = make(chan *Task, 10)
	q.tasks = make(map[uuid.UUID]*Task)
	q.workers = &sync.WaitGroup{}

	// Post a new task. If the service is already working on a task, a ServiceUnavailable
	// status will be returned.
	s.Mux.Post("/task", func(cxt *echo.Context) error {
		q.mux.Lock()
		defer q.mux.Unlock()

		if !q.service.available {
			return cxt.JSON(http.StatusServiceUnavailable, map[string]interface{}{
				"error": "task in progress",
			})
		}

		req := cxt.Request()
		defer req.Body.Close()

		task := q.NewTask()

		if err := json.NewDecoder(req.Body).Decode(task); err != nil {
			return cxt.NoContent(StatusUnprocessableEntity)
		}

		q.Enqueue(task)

		resp := cxt.Response()

		return json.NewEncoder(resp).Encode(task)
	})

	group := s.Mux.Group("/task/:id", func(cxt *echo.Context) error {
		id, err := uuid.FromString(cxt.Param("id"))

		if err != nil {
			return cxt.NoContent(http.StatusNotFound)
		}

		task, ok := q.tasks[id]

		if !ok {
			return cxt.NoContent(http.StatusNotFound)
		}

		cxt.Set("task", task)

		return nil
	})

	// Get the current running task. The ID is required in the URL.
	group.Get("", func(cxt *echo.Context) error {
		task := cxt.Get("task").(*Task)

		resp := cxt.Response()

		return json.NewEncoder(resp).Encode(task)
	})

	group.Options("", optionsHandler(echo.GET))

	if q.Cancel != nil {
		group.Delete("", func(cxt *echo.Context) error {
			task := cxt.Get("task").(*Task)
			return q.Cancel(task)
		})

		group.Options("", optionsHandler(echo.GET, echo.DELETE))
	} else {
		group.Options("", optionsHandler(echo.GET))
	}

	return nil
}

// Start starts the service.
func (q *Queue) Start(s *Service) error {
	cxt := s.Context()

	// Start worker goroutines to consume the queue.
	for i := 0; i < int(q.Workers); i++ {
		go func() {
			var t *Task

			q.workers.Add(1)

			for {
				select {
				case <-cxt.Done():
					q.workers.Done()
					return

				case t = <-q.queue:
					q.exec(t)
				}
			}
		}()
	}

	// Start goroutine to send notifications.
	go func() {
		var t *Task

		for {
			select {
			case <-cxt.Done():
				return

			case t = <-q.outbox:
				q.notify(t)
			}
		}
	}()

	return nil
}

// Stop stops the workers, but does not cancel running tasks.
func (q *Queue) Stop() error {
	return nil
}

// Enqueue queues a task.
func (q *Queue) Enqueue(t *Task) {
	t.state = Queued
	q.queue <- t
}
