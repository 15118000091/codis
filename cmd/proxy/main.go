// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/docopt/docopt-go"

	"github.com/CodisLabs/codis/pkg/proxy"
	"github.com/CodisLabs/codis/pkg/utils"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

func main() {
	const usage = `
Usage:
	codis-proxy [--ncpu=N [--maxcpu=MAX]] [--config=CONF] [--log=FILE] [--log-level=LEVEL] [--host-admin=ADDR] [--host-proxy=ADDR] [--ulimit=NLIMIT]
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

	if n, ok := utils.ArgumentInteger(d, "--ncpu"); ok {
		runtime.GOMAXPROCS(n)
	} else {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}

	mincpu := runtime.GOMAXPROCS(0)
	maxcpu, _ := utils.ArgumentInteger(d, "--maxcpu")
	switch {
	case mincpu <= maxcpu:
		log.Warnf("set ncpu = %d, maxcpu = %d", mincpu, maxcpu)
	default:
		log.Warnf("set ncpu = %d", mincpu)
	}
	if mincpu < maxcpu {
		go func() {
			for {
				update, err := SetMaxCPU(mincpu, maxcpu, time.Second)
				switch {
				case err != nil:
					log.WarnErrorf(err, "set max procs failed")
					time.Sleep(time.Second * 5)
				case !update:
					time.Sleep(time.Second * 3)
				default:
					time.Sleep(time.Millisecond * 500)
				}
			}
		}()
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

func SetMaxCPU(min, max int, interval time.Duration) (bool, error) {
	var now = time.Now()

	b, err := utils.GetCPUUsage()
	if err != nil {
		return false, err
	}

	time.Sleep(interval)

	e, err := utils.GetCPUUsage()
	if err != nil {
		return false, err
	}

	var usage = e - b

	current := runtime.GOMAXPROCS(0)
	percent := float64(usage) / float64(time.Since(now)) / float64(current) * 100

	switch {
	case percent < 55 && current > min:
		runtime.GOMAXPROCS(current - 1)
		log.Warnf("ncpu = %d, %3.2f%% usage = %s, -> %d", current, percent, usage, current-1)
		return true, nil
	case percent > 85 && current < max:
		runtime.GOMAXPROCS(current + 1)
		log.Warnf("ncpu = %d, %3.2f%% usage = %s, -> %d", current, percent, usage, current+1)
		return true, nil
	default:
		log.Infof("ncpu = %d, %3.2f%% usage = %s", current, percent, usage)
		return false, nil
	}
}
