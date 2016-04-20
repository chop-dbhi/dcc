package main

import (
	"flag"
	"fmt"
	"os/exec"
	"time"

	"github.com/blang/semver"
	"github.com/chop-dbhi/dcc"
)

var (
	Name = "Sleep Cmd Service"

	Version = semver.Version{
		Major: 1,
		Minor: 0,
		Patch: 0,
	}
)

func main() {
	var (
		host  string
		port  int
		debug bool
	)

	flag.StringVar(&host, "host", "", "Host of the service.")
	flag.IntVar(&port, "port", 5000, "Port of the service.")
	flag.BoolVar(&debug, "debug", false, "Run with debug output.")

	flag.Parse()

	w := dcc.NewWorker(Name, Version)

	w.Run = func(t *dcc.Task) (dcc.Params, error) {
		ds := t.Params.String("duration")
		d, err := time.ParseDuration(ds)

		if err != nil {
			return nil, err
		}

		cmd := exec.Command("sleep", fmt.Sprint(d.Seconds()))
		out, err := cmd.Output()

		if err != nil {
			return nil, err
		}

		return dcc.Params{
			"output": string(out),
		}, nil
	}

	c := w.Config()

	c.Set

	dcc.Serve(w, host, port, debug)
}
