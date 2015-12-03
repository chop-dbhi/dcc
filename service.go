package dcc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"gopkg.in/tylerb/graceful.v1"

	"golang.org/x/net/context"
	"golang.org/x/net/http2"

	"github.com/Sirupsen/logrus"
	"github.com/blang/semver"
	"github.com/labstack/echo"
	"github.com/satori/go.uuid"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	mw "github.com/labstack/echo/middleware"
)

// 422 Unprocessable Entity HTTP status code.
const StatusUnprocessableEntity = 422

var (
	// Timeout when attempting to gracefully shut down a service.
	ShutdownTimeout = 10 * time.Second

	ErrNameRequired    = errors.New("service: name required")
	ErrVersionRequired = errors.New("service: version required")
	ErrHostRequired    = errors.New("service: host required")
	ErrPortRequired    = errors.New("service: port required")
	ErrHandlerRequired = errors.New("service: handler required")
)

// Add prefix with service name
func newLogger(out io.Writer) *logrus.Logger {
	return &logrus.Logger{
		Out:       out,
		Level:     logrus.InfoLevel,
		Formatter: new(logrus.TextFormatter),
	}
}

// optionsHandler returns an OPTIONS handler for a specific route.
func optionsHandler(methods ...string) echo.HandlerFunc {
	return func(c *echo.Context) error {
		resp := c.Response()
		resp.Header().Set("Allow", strings.Join(methods, ","))
		return c.NoContent(http.StatusOK)
	}
}

// hasOptions returns true if the slice of methods contains the OPTIONS string.
func hasOptions(a []string) bool {
	for _, m := range a {
		if m == "OPTIONS" {
			return true
		}
	}

	return false
}

// bindOptions adds an OPTIONS handler for each route that does not already define one.
func bindOptions(mux *echo.Echo) {
	routes := make(map[string][]string)

	for _, r := range mux.Routes() {
		routes[r.Path] = append(routes[r.Path], strings.ToUpper(r.Method))
	}

	for p, m := range routes {
		if hasOptions(m) {
			continue
		}

		mux.Options(p, optionsHandler(m...))
	}
}

func newServeMux() *echo.Echo {
	mux := echo.New()

	// Apply some useful middleware.
	mux.Use(mw.Logger())
	mux.Use(mw.Recover())
	mux.Use(mw.Gzip())

	return mux
}

// Info defines the base service information that is exposed by
// the root endpoint.
type Info struct {
	// Human-readonable name of the service.
	Name string

	// Version of the service.
	Version string

	// UUID of the service for tracing purposes.
	UUID uuid.UUID

	// Denotes whether the service is currently avaialble. When possible, this
	// will be set in favor of a 503 Service Unavailable response, however it
	// is semantically equivalent.
	Available bool

	// Time the service started.
	Time time.Time

	// Meta can store additional metadata baout the service.
	Meta map[string]interface{}
}

// Uptime returns the uptime of the service as a duration.
func (i *Info) Uptime() time.Duration {
	return time.Now().Sub(i.Time)
}

func (i *Info) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"name":      i.Name,
		"version":   i.Version,
		"available": i.Available,
		"time":      i.Time,
		"uuid":      i.UUID,
		"uptime":    i.Uptime(),
		"meta":      i.Meta,
	})
}

// Handler is an interface that enables defining various kinds of
// service types.
type Handler interface {
	// Init initializes the service handler.
	Init(*Service) error

	// Start starts the service handler. This method cannot block. Any blocking
	// should be performed in a goroutine and managed by the handler.
	Start(*Service) error

	// Stop stops the service handler and performs internal cleanup.
	Stop() error
}

// HandlerFunc wraps a single function for long-running service. The handler
// will be launched in a goroutine and therefore expected to block.
type HandlerFunc struct {
	handle func(*Service) error
	done   chan error
}

func (h *HandlerFunc) Init(*Service) error {
	h.done = make(chan error)
	return nil
}

func (h *HandlerFunc) Start(s *Service) error {
	go func() {
		h.done <- h.handle(s)
	}()

	return nil
}

func (h *HandlerFunc) Stop() error {
	return <-h.done
}

// Service defines the boilerplate components of a service.
type Service struct {
	// Name of the service.
	Name string

	// Labels are used to denote the environment, type, role, etc. of
	// the service.
	Labels []string

	// Version is the semantic version.
	Version string

	// Unique identifier of the service.
	UUID uuid.UUID

	// Host of the service. Defaults to 127.0.0.1.
	Host string

	// Port of the service. Defaults to 5000.
	Port int

	// If true, the service is considered in debug mode.
	Debug bool

	// Flags is a flagset for the command line arguments.
	Flags *pflag.FlagSet

	// Config is the service configuration loaded from a local or remote source.
	Config *Config

	// Handle is a bare handler function.
	Handle func(*Service) error

	// Handler is the service handler.
	Handler Handler

	// HTTP multiplexer.
	Mux *echo.Echo

	// Server is a custom HTTP server value.
	Server *http.Server

	// TLS certificate and key files. Alternatively, a TLSConfig
	// can be defined on the Server value above.
	TLSCert string
	TLSKey  string

	HTTP2 bool

	// Logger for the service.
	Logger *logrus.Logger

	initTime  time.Time
	startTime time.Time
	stopTime  time.Time

	// CLI config
	cli *viper.Viper

	// Top-level context with cancel function.
	cxt    context.Context
	cancel context.CancelFunc

	// Denotes whether the service is considered available.
	available bool

	// Graceful server that wraps the primary one.
	server *graceful.Server
}

// Validate validates the minimum requirements of the service.
func (s *Service) validate() error {
	// Check required components.
	if s.Name == "" {
		return ErrNameRequired
	}

	if _, err := semver.Parse(s.Version); err != nil {
		return err
	}

	if s.Host == "" {
		return ErrHostRequired
	}

	if s.Port <= 0 {
		return ErrPortRequired
	}

	if s.Handle == nil && s.Handler == nil {
		return ErrHandlerRequired
	}

	return nil
}

// Initializes the internal state for a new service.
func (s *Service) init() {
	if s.Logger == nil {
		s.Logger = newLogger(os.Stderr)
	}

	if s.Handler == nil {
		s.Handler = &HandlerFunc{handle: s.Handle}
	}

	// Initialize a UUID if not set.
	if s.UUID == uuid.Nil {
		s.UUID = uuid.NewV4()
	}

	// A pre-defined flagset may be supplied.
	if s.Flags == nil {
		flags := pflag.NewFlagSet(s.Name, pflag.ExitOnError)

		// Initialize the base set of options. All other configuration should
		// be specified and driven by the contents of the `config`.
		flags.String("host", "127.0.0.1", "Host of the service.")
		flags.Int("port", 5000, "Port of the service.")
		flags.Bool("debug", false, "Toggle debug mode.")
		flags.String("consul", "", "Host of Consul agent.")
		flags.String("config", "", "Path to the service config.")

		s.Flags = flags
	}

	// Bind the CLI arguments for the service.
	cli := viper.New()
	cli.SetDefault("host", "127.0.0.1")
	cli.SetDefault("port", 5000)

	s.cli = cli

	cli.BindPFlag("host", s.Flags.Lookup("host"))
	cli.BindPFlag("port", s.Flags.Lookup("port"))
	cli.BindPFlag("debug", s.Flags.Lookup("debug"))
	cli.BindPFlag("consul", s.Flags.Lookup("consul"))
	cli.BindPFlag("config", s.Flags.Lookup("config"))

	mux := newServeMux()
	s.Mux = mux

	// Bind the root endpoint with info about the service.
	mux.Get("/", func(cxt *echo.Context) error {
		return cxt.JSON(http.StatusOK, s.Info())
	})

	// Top-level context.
	cxt, cancel := context.WithCancel(context.Background())

	s.cxt = cxt
	s.cancel = cancel

	// Configuration.
	s.Config = newConfig(cxt, s.Name)
}

// Info returns the info of the service.
func (s *Service) Info() *Info {
	return &Info{
		Name:      s.Name,
		Version:   s.Version,
		Available: s.available,
		UUID:      s.UUID,
		Time:      s.startTime,
	}
}

// Context returns the context of the service.
func (s *Service) Context() context.Context {
	return s.cxt
}

// Start initializes and starts the service in the background.
func (s *Service) Start(args []string) error {
	s.init()

	s.Logger.Debug("service: parsing command-line arguments")

	if args == nil {
		args = os.Args[1:]
	}

	s.Flags.Parse(args)

	// Set debug.
	if s.cli.IsSet("debug") {
		s.Debug = true
	}

	// Setup various debugging hooks.
	if s.Debug {
		s.Logger.Info("service: debug mode enabled")
		s.Mux.SetDebug(true)
		s.Logger.Level = logrus.DebugLevel
	}

	s.Host = s.cli.GetString("host")
	s.Port = s.cli.GetInt("port")

	// Validate the service.
	s.Logger.Debug("service: validating")
	if err := s.validate(); err != nil {
		return err
	}

	// Initialize the service handler.
	s.Logger.Debug("service: initializing handler")
	if err := s.Handler.Init(s); err != nil {
		return err
	}

	// Bind all the options methods for the endpoints.
	bindOptions(s.Mux)

	// Start the service handler.
	s.Logger.Debug("service: starting handler")
	if err := s.Handler.Start(s); err != nil {
		return err
	}

	addr := fmt.Sprintf("%s:%d", s.Host, s.Port)

	if s.Server == nil {
		s.Server = &http.Server{}
	}

	s.Server.Addr = addr
	s.Server.Handler = s.Mux

	// Enable HTTP 2 support. Since echo is not being used
	// to create the http.Server, the mux.HTTP2 does not already
	// affect anything, but is here for completeness.
	if s.HTTP2 {
		s.Mux.HTTP2(true)
		http2.ConfigureServer(s.Server, nil)
	}

	// Wrap in a graceful server.
	s.server = &graceful.Server{
		Server:  s.Server,
		Timeout: ShutdownTimeout,
	}

	// Start the HTTP server in the background.
	s.Logger.Debug("service: starting HTTP server")

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		s.Logger.Infof("service: listening on %s...", addr)

		wg.Done()
		if err := s.server.ListenAndServe(); err != nil {
			s.Logger.WithError(err).Error("service: error from listen and serve")
		}
	}()

	wg.Wait()

	return nil
}

// Wait will block until the service stops.
func (s *Service) Wait() {
	<-s.server.StopChan()
}

// Stop stops the service and handler.
func (s *Service) Stop() error {
	s.Logger.Info("service: stop requested")

	s.Logger.Debug("service: canceling context")
	s.cancel()

	s.Logger.Debug("service: stopping handler")
	err := s.Handler.Stop()

	s.Logger.Debug("service: stopping server")
	s.server.Stop(s.server.Timeout)

	s.Wait()
	s.Logger.Info("service: stopped")

	return err
}

// Serve starts and waits for the server to stop.
func (s *Service) Serve() error {
	if err := s.Start(nil); err != nil {
		return err
	}

	s.Wait()
	return nil
}
