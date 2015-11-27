package dcc

import "github.com/robfig/cron"

type Scheduler struct {
	// Schedule is a valid cron string. See https://godoc.org/github.com/robfig/cron#hdr-Usage
	// for details.
	Schedule string

	// Run is the function that is executed at each interval.
	Run func(*Service) error

	service   *Service
	scheduler *cron.Cron
}

// Init initializes the service.
func (s *Scheduler) Init(x *Service) error {
	s.service = x
	s.service.Logger.Debug("scheduler: initializing")
	return nil
}

// Start starts the server process.
func (s *Scheduler) Start(x *Service) error {
	s.service.Logger.Debug("scheduler: starting")
	s.scheduler = cron.New()

	err := s.scheduler.AddFunc(s.Schedule, func() {
		s.service.Logger.Info("scheduler: running job")

		if err := s.Run(x); err != nil {
			s.service.Logger.Errorf("scheduler: %s", err)
		}
	})

	if err != nil {
		return err
	}

	s.scheduler.Start()
	return nil
}

// Stop stops the scheduler.
func (s *Scheduler) Stop() error {
	s.service.Logger.Debug("scheduler: stopping")
	s.scheduler.Stop()
	s.service.Logger.Debug("scheduler: stopped")
	return nil
}
