// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

import (
	"fmt"
	"path/filepath"
)

func JoinPath(elem ...string) string {
	return filepath.ToSlash(filepath.Join(elem...))
}

type Store struct {
	client Client
	prefix string
}

func NewStore(client Client, name string) *Store {
	return &Store{
		client: client,
		prefix: JoinPath("/codis3", name),
	}
}

func (s *Store) Close() error {
	return s.client.Close()
}

func (s *Store) LockPath() string {
	return JoinPath(s.prefix, "topom")
}

func (s *Store) SlotPath(sid int) string {
	return JoinPath(s.prefix, "slots", fmt.Sprintf("slot-%04d", sid))
}

func (s *Store) GroupBase() string {
	return JoinPath(s.prefix, "group")
}

func (s *Store) GroupPath(gid int) string {
	return JoinPath(s.prefix, "group", fmt.Sprintf("group-%04d", gid))
}

func (s *Store) ProxyBase() string {
	return JoinPath(s.prefix, "proxy")
}

func (s *Store) ProxyPath(token string) string {
	return JoinPath(s.prefix, "proxy", fmt.Sprintf("proxy-%s", token))
}

func (s *Store) TopomClusterBase() string {
	return JoinPath(s.prefix, "topom-cluster")
}

func (s *Store) TopomClusterPath(name string) string {
	return JoinPath(s.prefix, "topom-cluster", name)
}

func (s *Store) Acquire(topom *Topom) error {
	return s.client.Create(s.LockPath(), topom.Encode())
}

func (s *Store) Release() error {
	return s.client.Delete(s.LockPath())
}

func (s *Store) SlotMappings() ([]*SlotMapping, error) {
	slots := make([]*SlotMapping, MaxSlotNum)
	for i := 0; i < len(slots); i++ {
		m, err := s.LoadSlotMapping(i, false)
		if err != nil {
			return nil, err
		}
		if m != nil {
			slots[i] = m
		} else {
			slots[i] = &SlotMapping{Id: i}
		}
	}
	return slots, nil
}

func (s *Store) LoadSlotMapping(sid int, must bool) (*SlotMapping, error) {
	b, err := s.client.Read(s.SlotPath(sid), must)
	if err != nil || b == nil {
		return nil, err
	}
	m := &SlotMapping{}
	if err := jsonDecode(m, b); err != nil {
		return nil, err
	}
	return m, nil
}

func (s *Store) UpdateSlotMapping(m *SlotMapping) error {
	return s.client.Update(s.SlotPath(m.Id), m.Encode())
}

func (s *Store) ListGroup() (map[int]*Group, error) {
	paths, err := s.client.List(s.GroupBase(), false)
	if err != nil {
		return nil, err
	}
	group := make(map[int]*Group)
	for _, path := range paths {
		b, err := s.client.Read(path, true)
		if err != nil {
			return nil, err
		}
		g := &Group{}
		if err := jsonDecode(g, b); err != nil {
			return nil, err
		}
		group[g.Id] = g
	}
	return group, nil
}

func (s *Store) LoadGroup(gid int, must bool) (*Group, error) {
	b, err := s.client.Read(s.GroupPath(gid), must)
	if err != nil || b == nil {
		return nil, err
	}
	g := &Group{}
	if err := jsonDecode(g, b); err != nil {
		return nil, err
	}
	return g, nil
}

func (s *Store) UpdateGroup(g *Group) error {
	return s.client.Update(s.GroupPath(g.Id), g.Encode())
}

func (s *Store) DeleteGroup(gid int) error {
	return s.client.Delete(s.GroupPath(gid))
}

func (s *Store) ListProxy() (map[string]*Proxy, error) {
	paths, err := s.client.List(s.ProxyBase(), false)
	if err != nil {
		return nil, err
	}
	proxy := make(map[string]*Proxy)
	for _, path := range paths {
		b, err := s.client.Read(path, true)
		if err != nil {
			return nil, err
		}
		p := &Proxy{}
		if err := jsonDecode(p, b); err != nil {
			return nil, err
		}
		proxy[p.Token] = p
	}
	return proxy, nil
}

func (s *Store) LoadProxy(token string, must bool) (*Proxy, error) {
	b, err := s.client.Read(s.ProxyPath(token), must)
	if err != nil || b == nil {
		return nil, err
	}
	p := &Proxy{}
	if err := jsonDecode(p, b); err != nil {
		return nil, err
	}
	return p, nil
}

func (s *Store) UpdateProxy(p *Proxy) error {
	return s.client.Update(s.ProxyPath(p.Token), p.Encode())
}

func (s *Store) DeleteProxy(token string) error {
	return s.client.Delete(s.ProxyPath(token))
}

func (s *Store) CreateTopomClusterEphemeral(topom *Topom) (<-chan struct{}, string, error) {
	return s.client.CreateEphemeralInOrder(s.TopomClusterBase(), topom.Encode())
}

func (s *Store) LoadTopomClusterEphemeral(path string, must bool) (*Topom, error) {
	b, err := s.client.Read(path, must)
	if err != nil || b == nil {
		return nil, err
	}
	t := &Topom{}
	if err := jsonDecode(t, b); err != nil {
		return nil, err
	}
	return t, nil
}

func (s *Store) ElectTopomClusterEphemeralLeader() (<-chan struct{}, string, error) {
	w, paths, err := s.client.ListEphemeralInOrder(s.TopomClusterBase())
	if err != nil {
		return nil, "", err
	}
	var leader string
	if len(paths) != 0 {
		leader = paths[0]
	}
	return w, leader, nil
}
