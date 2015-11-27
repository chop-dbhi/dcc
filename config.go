package dcc

import (
	"fmt"
	"net/url"
	"path/filepath"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/spf13/viper"
	"golang.org/x/net/context"
)

// trimExt returns a filename without the extension.
func trimExt(s string) string {
	ext := filepath.Ext(s)
	return s[:len(s)-len(ext)]
}

// setConfigPath set the primary source of configuration for the service.
func setConfigPath(v *viper.Viper, uri string) error {
	p, err := url.Parse(uri)

	if err != nil {
		return err
	}

	// Local path.
	if p.Scheme == "" {
		v.SetConfigFile(uri)
		return nil
	}

	// Consul.
	if p.Scheme == "consul" {
		ep := fmt.Sprintf("http://%s", p.Host)
		v.AddRemoteProvider("consul", ep, trimExt(p.Path))
		v.SetConfigType(filepath.Ext(p.Path)[1:])
		return nil
	}

	return nil
}

// Config encapsulates a key-value like interface for accessing configuration options.
type Config struct {
	// Key-value viper value.
	// TODO: change to be a private
	KV *viper.Viper

	// Path to the configuration file.
	Path string

	// Interval to update the configuration.
	Interval time.Duration

	// Time the config was last synced.
	Synced time.Time

	// Denotes whether this config is bound to a remote source.
	remote bool

	// Manage a watched config.
	watching chan struct{}
	watcher  *sync.WaitGroup

	// Upstream logger and context.
	logger *logrus.Logger
	cxt    context.Context
}

// Read reads the configuration from a file or remote source.
func (c *Config) Read() {
	if c.remote {
		c.KV.ReadRemoteConfig()
	} else {
		c.KV.ReadInConfig()
	}
}

// Watch watches the config file.
func (c *Config) watch() {
	if c.watching != nil {
		return
	}

	c.watching = make(chan struct{})
	c.watcher.Add(1)

	go func() {
		defer c.watcher.Done()

		for {
			t := time.NewTimer(c.Interval)

			select {
			case <-c.watching:
				c.logger.Debugf("config: stop watching signaled for %v", c.Path)
				return

			case <-c.cxt.Done():
				c.logger.Debugf("config: parent context signaled for %v", c.Path)
				return

			case <-t.C:
				var err error

				c.logger.Debugf("config: reading config from %v", c.Path)

				t0 := time.Now()

				if c.remote {
					err = c.KV.ReadRemoteConfig()
				} else {
					err = c.KV.ReadInConfig()
				}

				c.logger.Debugf("config: read took %s for %v", time.Now().Sub(t0), c.Path)

				if err != nil {
					c.logger.Errorf("config: error updating config {%s}", err)
				} else {
					c.Synced = time.Now()
					c.logger.Info("config: successfully updated config")
				}
			}
		}
	}()

	c.logger.Debugf("config: begin watching %v", c.Path)
}

// Unwatch stops watching the remote config.
func (c *Config) unwatch() {
	if c.watching == nil {
		return
	}

	close(c.watching)
	c.watcher.Wait()
	c.watching = nil

	c.logger.Debugf("config: stopped watching %v", c.Path)
}

func newConfig(cxt context.Context, prefix string) *Config {
	v := viper.New()

	// Support environment variables.
	v.AutomaticEnv()
	v.SetEnvPrefix(prefix)

	return &Config{
		KV:  v,
		cxt: cxt,
	}
}
