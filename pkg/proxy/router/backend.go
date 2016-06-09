// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package router

import (
	"fmt"
	"sync"
	"time"

	"github.com/CodisLabs/codis/pkg/proxy/redis"
	"github.com/CodisLabs/codis/pkg/utils"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

type BackendConn struct {
	addr string
	auth string
	stop sync.Once

	input chan *Request
}

func NewBackendConn(addr, auth string) *BackendConn {
	bc := &BackendConn{
		addr: addr, auth: auth,
		input: make(chan *Request, 1024),
	}
	go bc.Run()
	return bc
}

func (bc *BackendConn) Run() {
	log.Warnf("backend conn [%p] to %s, start service", bc, bc.addr)
	for k := 0; ; k++ {
		log.Warnf("backend conn [%p] to %s, rounds-[%d]", bc, bc.addr, k)
		if err := bc.loopWriter(k); err == nil {
			break
		}
		time.Sleep(time.Millisecond * 250)
	}
	log.Warnf("backend conn [%p] to %s, stop and exit", bc, bc.addr)
}

func (bc *BackendConn) Addr() string {
	return bc.addr
}

func (bc *BackendConn) Close() {
	bc.stop.Do(func() {
		close(bc.input)
	})
}

func (bc *BackendConn) PushBack(r *Request) {
	r.Batch.Add(1)
	bc.input <- r
}

func (bc *BackendConn) KeepAlive() bool {
	if len(bc.input) != 0 {
		return false
	}

	bc.PushBack(NewRequest("PING", []*redis.Resp{
		redis.NewBulkBytes([]byte("PING")),
	}, nil))

	return true
}

func (bc *BackendConn) loopReader(tasks <-chan *Request, c *redis.Conn, round int) (err error) {
	defer func() {
		c.Close()
		for r := range tasks {
			bc.setResponse(r, nil, err)
		}
		log.WarnErrorf(err, "backend conn [%p] to %s, reader-[%d] exit", bc, bc.addr, round)
	}()
	for r := range tasks {
		resp, err := c.Reader.Decode()
		if err != nil {
			return bc.setResponse(r, nil, err)
		}
		bc.setResponse(r, resp, nil)
	}
	return nil
}

func (bc *BackendConn) loopWriter(round int) (err error) {
	defer func() {
		for i := len(bc.input); i != 0; i-- {
			r := <-bc.input
			bc.setResponse(r, nil, err)
		}
		log.WarnErrorf(err, "backend conn [%p] to %s, writer-[%d] exit", bc, bc.addr, round)
	}()
	r, ok := <-bc.input
	if ok {
		c, tasks, err := bc.newBackendReader(round)
		if err != nil {
			return bc.setResponse(r, nil, err)
		}
		defer close(tasks)

		p := &FlushPolicy{
			Conn: c,

			MaxBuffered:   256,
			MaxIntervalMs: 300,
		}

		for ok {
			if !r.IsBroken() {
				if err := p.EncodeMultiBulk(r.Multi); err != nil {
					return bc.setResponse(r, nil, err)
				}
				if err := p.Flush(len(bc.input) == 0); err != nil {
					return bc.setResponse(r, nil, err)
				}
				tasks <- r
			} else {
				if err := p.Flush(len(bc.input) == 0); err != nil {
					return bc.setResponse(r, nil, err)
				}
				bc.setResponse(r, nil, ErrDiscardedRequest)
			}
			r, ok = <-bc.input
		}
	}
	return nil
}

func (bc *BackendConn) newBackendReader(round int) (*redis.Conn, chan<- *Request, error) {
	c, err := redis.DialTimeout(bc.addr, 1024*512, time.Second)
	if err != nil {
		return nil, nil, err
	}
	c.ReaderTimeout = time.Minute
	c.WriterTimeout = time.Minute

	if err := bc.verifyAuth(c); err != nil {
		c.Close()
		return nil, nil, err
	}

	tasks := make(chan *Request, 4096)
	go bc.loopReader(tasks, c, round)

	return c, tasks, nil
}

func (bc *BackendConn) verifyAuth(c *redis.Conn) error {
	if bc.auth == "" {
		return nil
	}

	multi := []*redis.Resp{
		redis.NewBulkBytes([]byte("AUTH")),
		redis.NewBulkBytes([]byte(bc.auth)),
	}

	if err := c.Writer.EncodeMultiBulk(multi, true); err != nil {
		return err
	}

	switch resp, err := c.Reader.Decode(); {
	case err != nil:
		return err
	case resp == nil:
		return errors.New(fmt.Sprintf("error resp: nil response"))
	case resp.IsError():
		return errors.New(fmt.Sprintf("error resp: %s", resp.Value))
	case resp.IsString():
		return nil
	default:
		return errors.New(fmt.Sprintf("error resp: should be string, but got %s", resp.Type))
	}
}

func (bc *BackendConn) setResponse(r *Request, resp *redis.Resp, err error) error {
	r.Response.Resp, r.Response.Err = resp, err
	if err != nil {
		r.Break()
	}
	if r.slot != nil {
		r.slot.Done()
	}
	r.Batch.Done()
	return err
}

type SharedBackendConn struct {
	*BackendConn
	mu sync.Mutex

	refcnt int
}

func NewSharedBackendConn(addr, auth string) *SharedBackendConn {
	return &SharedBackendConn{BackendConn: NewBackendConn(addr, auth), refcnt: 1}
}

func (s *SharedBackendConn) Close() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.refcnt <= 0 {
		log.Panicf("shared backend conn has been closed, close too many times")
	}
	if s.refcnt == 1 {
		s.BackendConn.Close()
	}
	s.refcnt--
	return s.refcnt == 0
}

func (s *SharedBackendConn) IncrRefcnt() *SharedBackendConn {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.refcnt == 0 {
		log.Panicf("shared backend conn has been closed")
	}
	s.refcnt++
	return s
}

type FlushPolicy struct {
	Conn *redis.Conn

	MaxBuffered   int
	MaxIntervalMs int64

	nbuffered int
}

func (p *FlushPolicy) NeedFlush() bool {
	if p.nbuffered != 0 {
		if p.nbuffered > p.MaxBuffered {
			return true
		}
		if d := utils.Microseconds() - p.Conn.LastWriteMs; d > p.MaxIntervalMs {
			return true
		}
	}
	return false
}

func (p *FlushPolicy) Flush(force bool) error {
	if force || p.NeedFlush() {
		if err := p.Conn.Writer.Flush(); err != nil {
			return err
		}
		p.nbuffered = 0
	}
	return nil
}

func (p *FlushPolicy) Encode(resp *redis.Resp) error {
	if err := p.Conn.Writer.Encode(resp, false); err != nil {
		return err
	}
	p.nbuffered++
	return nil
}

func (p *FlushPolicy) EncodeMultiBulk(array []*redis.Resp) error {
	if err := p.Conn.Writer.EncodeMultiBulk(array, false); err != nil {
		return err
	}
	p.nbuffered++
	return nil
}
