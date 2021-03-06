package dcc

import (
	"testing"
	"time"
)

func TestScheduler(t *testing.T) {
	var counter int

	s := &Service{
		Name: "scheduler-test",

		Version: "1.0.0",

		Debug: true,

		Handler: &Scheduler{
			Schedule: "@every 1s",

			Run: func(s *Service) error {
				counter++
				return nil
			},
		},
	}

	if err := s.Start(nil); err != nil {
		t.Error(err)
	}

	time.Sleep(time.Second * 2)

	s.Stop()

	if counter < 0 {
		t.Error("counter is zero")
	}
}
