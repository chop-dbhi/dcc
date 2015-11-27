package dcc

import (
	"testing"

	"github.com/blang/semver"
)

func TestQueue(t *testing.T) {
	var out []string

	q := &Queue{
		Exec: func(t *Task) (Params, error) {
			out = append(out, t.ID())
			return nil, nil
		},
	}

	s := &Service{
		Name: "queue-test",

		Version: semver.Version{
			Major: 1,
		},

		Debug: true,

		Handler: q,
	}

	if err := s.Start(nil); err != nil {
		t.Errorf("start: %s", err)
		return
	}

	// Enqueue blocks until each task is consumed.
	for i := 0; i < 5; i++ {
		t := q.NewTask()
		q.Enqueue(t)
	}

	if err := s.Stop(); err != nil {
		t.Errorf("stop: %s", err)
	}

	if len(out) != 5 {
		t.Errorf("expected %d elements, got %d", 5, len(out))
	}
}
