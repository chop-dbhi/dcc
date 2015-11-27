package dcc

import (
	"testing"
	"time"

	"github.com/blang/semver"
)

func TestHandleFunc(t *testing.T) {
	var counter int

	s := &Service{
		Name: "handle-test",

		Version: semver.Version{
			Major: 1,
		},

		Debug: true,

		Handle: func(s *Service) error {
			cxt := s.Context()

			for {
				select {
				case <-cxt.Done():
					return nil

				default:
					counter++
				}

				time.Sleep(time.Millisecond * 100)
			}

			return nil
		},
	}

	if err := s.Start(nil); err != nil {
		t.Errorf("start: %s", err)
		return
	}

	time.Sleep(time.Second)

	if err := s.Stop(); err != nil {
		t.Errorf("stop: %s", err)
	}

	if counter == 0 {
		t.Error("counter is zero")
	}
}
