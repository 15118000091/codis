// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package router

import (
	"net"
	"sync"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

type Router struct {
	mu sync.Mutex

	auth string
	pool map[string]*SharedBackendConn

	slots [models.MaxSlotNum]Slot

	closed bool
}

func New() *Router {
	return NewWithAuth("")
}

func NewWithAuth(auth string) *Router {
	s := &Router{
		auth: auth,
		pool: make(map[string]*SharedBackendConn),
	}
	for i := 0; i < len(s.slots); i++ {
		s.slots[i].id = i
	}
	return s
}

func (s *Router) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	for i := 0; i < len(s.slots); i++ {
		s.resetSlot(i)
	}
	s.closed = true
	return nil
}

func (s *Router) GetSlots() []*models.Slot {
	s.mu.Lock()
	defer s.mu.Unlock()
	slots := make([]*models.Slot, 0, len(s.slots))
	for i := 0; i < len(s.slots); i++ {
		slot := &s.slots[i]
		slots = append(slots, &models.Slot{
			Id:          i,
			BackendAddr: slot.backend.addr,
			MigrateFrom: slot.migrate.from,
			Locked:      slot.lock.hold,
		})
	}
	return slots
}

var (
	ErrClosedRouter  = errors.New("use of closed router")
	ErrInvalidSlotId = errors.New("use of invalid slot id")
)

func (s *Router) FillSlot(idx int, addr, from string, locked bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosedRouter
	}
	if idx >= 0 && idx < len(s.slots) {
		return s.fillSlot(idx, addr, from, locked)
	} else {
		return ErrInvalidSlotId
	}
}

func (s *Router) KeepAlive() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosedRouter
	}
	for _, bc := range s.pool {
		bc.KeepAlive()
	}
	return nil
}

func (s *Router) Dispatch(r *Request) error {
	hkey := getHashKey(r.Multi, r.OpStr)
	slot := &s.slots[hashSlot(hkey)]
	return slot.forward(r, hkey)
}

func (s *Router) getBackendConn(addr string) *SharedBackendConn {
	if bc := s.pool[addr]; bc != nil {
		return bc.IncrRefcnt()
	} else {
		bc := NewSharedBackendConn(addr, s.auth)
		s.pool[addr] = bc
		return bc
	}
}

func (s *Router) putBackendConn(bc *SharedBackendConn) {
	if bc != nil && bc.Close() {
		delete(s.pool, bc.Addr())
	}
}

func (s *Router) resetSlot(idx int) {
	slot := &s.slots[idx]
	slot.blockAndWait()

	s.putBackendConn(slot.backend.bc)
	s.putBackendConn(slot.migrate.bc)
	slot.reset()

	slot.unblock()
}

func (s *Router) fillSlot(idx int, addr, from string, locked bool) error {
	slot := &s.slots[idx]
	slot.blockAndWait()

	s.putBackendConn(slot.backend.bc)
	s.putBackendConn(slot.migrate.bc)
	slot.reset()

	if len(addr) != 0 {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			log.ErrorErrorf(err, "split host-port failed, address = %s", addr)
		} else {
			slot.backend.host = []byte(host)
			slot.backend.port = []byte(port)
		}
		slot.backend.addr = addr
		slot.backend.bc = s.getBackendConn(addr)
	}
	if len(from) != 0 {
		slot.migrate.from = from
		slot.migrate.bc = s.getBackendConn(from)
	}

	if !locked {
		slot.unblock()
	}

	if slot.migrate.bc != nil {
		log.Warnf("fill slot %04d, backend.addr = %s, migrate.from = %s, locked = %t",
			idx, slot.backend.addr, slot.migrate.from, locked)
	} else {
		log.Warnf("fill slot %04d, backend.addr = %s, locked = %t",
			idx, slot.backend.addr, locked)
	}
	return nil
}
