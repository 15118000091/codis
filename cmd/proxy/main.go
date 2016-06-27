// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/CodisLabs/codis/pkg/proxy"
	"github.com/CodisLabs/codis/pkg/utils"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/docopt/docopt-go"
)

func main() {
	const usage = `
Usage:
	codis-proxy [--ncpu=N [--max-ncpu=MAX]] [--config=CONF] [--log=FILE] [--log-level=LEVEL] [--host-admin=ADDR] [--host-proxy=ADDR] [--ulimit=NLIMIT]
	codis-proxy  --default-config
	codis-proxy  --version

Options:
	--ncpu=N                    set runtime.GOMAXPROCS to N, default is runtime.NumCPU().
	-c CONF, --config=CONF      run with the specific configuration.
	-l FILE, --log=FILE         set path/name of daliy rotated log file.
	--log-level=LEVEL           set the log-level, should be INFO,WARN,DEBUG or ERROR, default is INFO.
	--ulimit=NLIMIT             run 'ulimit -n' to check the maximum number of open file descriptors.
`

	d, err := docopt.Parse(usage, nil, true, "", false)
	if err != nil {
		log.PanicError(err, "parse arguments failed")
	}

	switch {

	case d["--default-config"]:
		fmt.Println(proxy.DefaultConfig)
		return

	case d["--version"].(bool):
		fmt.Println("version:", utils.Version)
		fmt.Println("compile:", utils.Compile)
		return

	}

	if s, ok := utils.Argument(d, "--log"); ok {
		w, err := log.NewRollingFile(s, log.DailyRolling)
		if err != nil {
			log.PanicErrorf(err, "open log file %s failed", s)
		} else {
			log.StdLog = log.New(w, "")
		}
	}
	log.SetLevel(log.LevelInfo)

	if s, ok := utils.Argument(d, "--log-level"); ok {
		if !log.SetLevelString(s) {
			log.Panicf("option --log-level = %s", s)
		}
	}

	if n, ok := utils.ArgumentInteger(d, "--ulimit"); ok {
		b, err := exec.Command("/bin/sh", "-c", "ulimit -n").Output()
		if err != nil {
			log.PanicErrorf(err, "run ulimit -n failed")
		}
		if v, err := strconv.Atoi(strings.TrimSpace(string(b))); err != nil || v < n {
			log.PanicErrorf(err, "ulimit too small: %d, should be at least %d", v, n)
		}
	}

	var ncpu int
	if n, ok := utils.ArgumentInteger(d, "--ncpu"); ok {
		ncpu = n
	} else {
		ncpu = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(ncpu)

	var maxncpu int
	if n, ok := utils.ArgumentInteger(d, "--max-ncpu"); ok {
		maxncpu = n
	}
	log.Warnf("set ncpu = %d, max-ncpu = %d", ncpu, maxncpu)

	if ncpu < maxncpu {
		go AutoGOMAXPROCS(ncpu, maxncpu)
	}

	config := proxy.NewDefaultConfig()
	if s, ok := utils.Argument(d, "--config"); ok {
		if err := config.LoadFromFile(s); err != nil {
			log.PanicErrorf(err, "load config %s failed", s)
		}
	}
	if s, ok := utils.Argument(d, "--host-admin"); ok {
		config.HostAdmin = s
		log.Warnf("option --host-admin = %s", s)
	}
	if s, ok := utils.Argument(d, "--host-proxy"); ok {
		config.HostProxy = s
		log.Warnf("option --host-proxy = %s", s)
	}

	s, err := proxy.New(config)
	if err != nil {
		log.PanicErrorf(err, "create proxy with config file failed\n%s", config)
	}
	defer s.Close()

	log.Warnf("create proxy with config\n%s", config)

	go func() {
		defer s.Close()
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGKILL, syscall.SIGTERM)

		sig := <-c
		log.Warnf("[%p] proxy receive signal = '%v'", s, sig)
	}()

	for !s.IsClosed() && !s.IsOnline() {
		log.Warnf("[%p] proxy waiting online ...", s)
		time.Sleep(time.Second)
	}

	for !s.IsClosed() {
		time.Sleep(time.Second)
	}

	log.Warnf("[%p] proxy exiting ...", s)
}

func AutoGOMAXPROCS(min, max int) {
	for {
		var ncpu = runtime.GOMAXPROCS(0)
		var less, more int
		var usage [10]float64
		for i := 0; i < len(usage) && more == 0; i++ {
			u, err := utils.CPUUsage(time.Second)
			if err != nil {
				log.WarnErrorf(err, "get cpu usage failed")
				time.Sleep(time.Second * 30)
				continue
			}
			switch {
			case u < 0.55 && ncpu > min:
				less++
			case u > 0.85 && ncpu < max:
				more++
			}
			usage[i] = u
		}
		var nn = ncpu
		switch {
		case more != 0:
			nn = ncpu + ((max - ncpu + 2) / 3)
		case less == len(usage):
			nn = ncpu - 1
		}
		if nn != ncpu {
			runtime.GOMAXPROCS(nn)
			var b bytes.Buffer
			for i, u := range usage {
				if i != 0 {
					fmt.Fprintf(&b, ", ")
				}
				fmt.Fprintf(&b, "%.3f", u)
			}
			log.Warnf("ncpu = %d -> %d, usage = [%s]", ncpu, nn, b.Bytes())
		}
	}
}
