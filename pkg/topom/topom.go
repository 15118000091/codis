// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"container/list"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/math2"
	"github.com/CodisLabs/codis/pkg/utils/rpc"
	"github.com/CodisLabs/codis/pkg/utils/sync2/atomic2"
)

type Topom struct {
	mu sync.Mutex

	token string
	xauth string
	model *models.Topom
	store *models.Store
	cache struct {
		slots []*models.SlotMapping
		group map[int]*models.Group
		proxy map[string]*models.Proxy
		hooks list.List
	}

	exit struct {
		C chan struct{}
	}

	config *Config
	online bool
	closed bool

	ladmin net.Listener
	redisp *RedisPool

	action struct {
		interval atomic2.Int64
		disabled atomic2.Bool

		progress struct {
			remain atomic2.Int64
			failed atomic2.Bool
		}
		executor atomic2.Int64
	}

	stats struct {
		servers map[string]*RedisStats
		proxies map[string]*ProxyStats
	}
}

var ErrClosedTopom = errors.New("use of closed topom")

func New(client models.Client, config *Config) (*Topom, error) {
	if err := models.ValidProductName(config.ProductName); err != nil {
		return nil, err
	}
	s := &Topom{config: config, store: models.NewStore(client, config.ProductName)}
	s.token = rpc.NewToken()
	s.xauth = rpc.NewXAuth(config.ProductName)
	s.model = &models.Topom{
		Token: s.token, StartTime: time.Now().String(),
	}
	s.model.ProductName = config.ProductName
	s.model.Pid = os.Getpid()
	s.model.Pwd, _ = os.Getwd()
	if b, err := exec.Command("uname", "-a").Output(); err != nil {
		log.WarnErrorf(err, "run command uname failed")
	} else {
		s.model.Sys = strings.TrimSpace(string(b))
	}

	s.exit.C = make(chan struct{})
	s.redisp = NewRedisPool(config.ProductAuth, time.Second*10)

	s.action.interval.Set(1000 * 10)

	s.stats.servers = make(map[string]*RedisStats)
	s.stats.proxies = make(map[string]*ProxyStats)

	if err := s.setup(); err != nil {
		s.Close()
		return nil, err
	}

	log.Warnf("create new topom:\n%s", s.model.Encode())

	go s.serveAdmin()

	return s, nil
}

func (s *Topom) setup() error {
	if l, err := net.Listen("tcp", s.config.AdminAddr); err != nil {
		return errors.Trace(err)
	} else {
		s.ladmin = l

		x, err := utils.ResolveAddr("tcp", l.Addr().String(), s.config.HostAdmin)
		if err != nil {
			return err
		}
		s.model.AdminAddr = x
	}
	return nil
}

func (s *Topom) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	close(s.exit.C)

	if s.ladmin != nil {
		s.ladmin.Close()
	}
	if s.redisp != nil {
		s.redisp.Close()
	}

	defer s.store.Close()

	if s.online {
		if err := s.store.Release(); err != nil {
			log.ErrorErrorf(err, "store: release lock of %s failed", s.config.ProductName)
			return errors.Errorf("store: release lock of %s failed", s.config.ProductName)
		}
	}
	return nil
}

func (s *Topom) Start(routines bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosedTopom
	}
	if s.online {
		return nil
	} else {
		if err := s.store.Acquire(s.model); err != nil {
			log.ErrorErrorf(err, "store: acquire lock of %s failed", s.config.ProductName)
			return errors.Errorf("store: acquire lock of %s failed", s.config.ProductName)
		}
		s.online = true
	}

	if !routines {
		return nil
	}

	go func() {
		for !s.IsClosed() {
			if s.IsOnline() {
				if w, _ := s.RefreshRedisStats(time.Second * 5); w != nil {
					w.Wait()
				}
			}
			time.Sleep(time.Second)
		}
	}()

	go func() {
		for !s.IsClosed() {
			if s.IsOnline() {
				if w, _ := s.RefreshProxyStats(time.Second * 5); w != nil {
					w.Wait()
				}
			}
			time.Sleep(time.Second)
		}
	}()

	go func() {
		for !s.IsClosed() {
			if s.IsOnline() {
				if err := s.ProcessSlotAction(); err != nil {
					log.WarnErrorf(err, "process slot action failed")
					time.Sleep(time.Second * 5)
				}
			}
			time.Sleep(time.Second)
		}
	}()

	go func() {
		for !s.IsClosed() {
			if s.IsOnline() {
				if err := s.ProcessSyncAction(); err != nil {
					log.WarnErrorf(err, "process sync action failed")
					time.Sleep(time.Second * 5)
				}
			}
			time.Sleep(time.Second)
		}
	}()

	return nil
}

func (s *Topom) XAuth() string {
	return s.xauth
}

func (s *Topom) Model() *models.Topom {
	return s.model
}

var ErrNotOnline = errors.New("topom is not online")

func (s *Topom) newContext() (*context, error) {
	if s.closed {
		return nil, ErrClosedTopom
	}
	if s.online {
		if err := s.refillCache(); err != nil {
			return nil, err
		} else {
			ctx := &context{}
			ctx.slots = s.cache.slots
			ctx.group = s.cache.group
			ctx.proxy = s.cache.proxy
			return ctx, nil
		}
	} else {
		return nil, ErrNotOnline
	}
}

func (s *Topom) Stats() (*Stats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}

	stats := &Stats{}
	stats.Closed = s.closed

	stats.Slots = ctx.slots

	stats.Group.Models = models.SortGroup(ctx.group)
	stats.Group.Stats = s.stats.servers

	stats.Proxy.Models = models.SortProxy(ctx.proxy)
	stats.Proxy.Stats = s.stats.proxies

	stats.SlotAction.Interval = s.action.interval.Get()
	stats.SlotAction.Disabled = s.action.disabled.Get()
	stats.SlotAction.Progress.Remain = s.action.progress.remain.Get()
	stats.SlotAction.Progress.Failed = s.action.progress.failed.Get()
	stats.SlotAction.Executor = s.action.executor.Get()

	return stats, nil
}

type Stats struct {
	Online bool `json:"online"`
	Closed bool `json:"closed"`

	Slots []*models.SlotMapping `json:"slots"`

	Group struct {
		Models []*models.Group        `json:"models"`
		Stats  map[string]*RedisStats `json:"stats"`
	} `json:"group"`

	Proxy struct {
		Models []*models.Proxy        `json:"models"`
		Stats  map[string]*ProxyStats `json:"stats"`
	} `json:"proxy"`

	SlotAction struct {
		Interval int64 `json:"interval"`
		Disabled bool  `json:"disabled"`

		Progress struct {
			Remain int64 `json:"remain"`
			Failed bool  `json:"failed"`
		} `json:"progress"`

		Executor int64 `json:"executor"`
	} `json:"slot_action"`
}

func (s *Topom) Config() *Config {
	return s.config
}

func (s *Topom) IsOnline() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.online && !s.closed
}

func (s *Topom) IsClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *Topom) GetSlotActionInterval() int {
	return int(s.action.interval.Get())
}

func (s *Topom) SetSlotActionInterval(us int) {
	us = math2.MinMaxInt(us, 0, 1000*1000)
	s.action.interval.Set(int64(us))
	log.Warnf("set action interval = %d", us)
}

func (s *Topom) GetSlotActionDisabled() bool {
	return s.action.disabled.Get()
}

func (s *Topom) SetSlotActionDisabled(value bool) {
	s.action.disabled.Set(value)
	log.Warnf("set action disabled = %t", value)
}

func (s *Topom) Slots() ([]*models.Slot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}
	return ctx.toSlotSlice(ctx.slots), nil
}

func (s *Topom) serveAdmin() {
	if s.IsClosed() {
		return
	}
	defer s.Close()

	log.Warnf("admin start service on %s", s.ladmin.Addr())

	eh := make(chan error, 1)
	go func(l net.Listener) {
		h := http.NewServeMux()
		h.Handle("/", newApiServer(s))
		hs := &http.Server{Handler: h}
		eh <- hs.Serve(l)
	}(s.ladmin)

	select {
	case <-s.exit.C:
		log.Warnf("admin shutdown")
	case err := <-eh:
		log.ErrorErrorf(err, "admin exit on error")
	}
}
