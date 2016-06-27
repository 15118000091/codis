// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/CodisLabs/codis/pkg/models/etcd"
	"github.com/CodisLabs/codis/pkg/models/zk"
	"github.com/CodisLabs/codis/pkg/utils/errors"
)

type Client interface {
	Create(path string, data []byte) error
	Update(path string, data []byte) error
	Delete(path string) error

	Read(path string) ([]byte, error)
	List(path string) ([]string, error)

	Close() error

	CreateEphemeral(path string, data []byte) (<-chan struct{}, error)
	CreateEphemeralInOrder(path string, data []byte) (<-chan struct{}, string, error)

	ListEphemeralInOrder(path string) (<-chan struct{}, []string, error)
}

var ErrUnknownCoordinator = errors.New("unknown coordinator type")

func NewClient(coordinator string, addrlist string, timeout time.Duration) (Client, error) {
	switch coordinator {
	case "zk", "zookeeper":
		return zkclient.New(addrlist, timeout)
	case "etcd":
		return etcdclient.New(addrlist, timeout)
	}
	return nil, errors.Trace(ErrUnknownCoordinator)
}

func EncodePath(elem ...string) string {
	return filepath.ToSlash(filepath.Join(elem...))
}

func DecodePath(path string) string {
	return filepath.FromSlash(path)
}

type Store struct {
	client Client
	prefix string
}

func NewStore(client Client, name string) *Store {
	return &Store{
		client: client,
		prefix: EncodePath("/codis3", name),
	}
}

func (s *Store) Close() error {
	return s.client.Close()
}

func (s *Store) LockPath() string {
	return EncodePath(s.prefix, "topom")
}

func (s *Store) SlotPath(sid int) string {
	return EncodePath(s.prefix, "slots", fmt.Sprintf("slot-%04d", sid))
}

func (s *Store) GroupBase() string {
	return EncodePath(s.prefix, "group")
}

func (s *Store) GroupPath(gid int) string {
	return EncodePath(s.prefix, "group", fmt.Sprintf("group-%04d", gid))
}

func (s *Store) ProxyBase() string {
	return EncodePath(s.prefix, "proxy")
}

func (s *Store) ProxyPath(token string) string {
	return EncodePath(s.prefix, "proxy", fmt.Sprintf("proxy-%s", token))
}

func (s *Store) TopomClusterBase() string {
	return EncodePath(s.prefix, "topom-cluster")
}

func (s *Store) TopomClusterPath(name string) string {
	return EncodePath(s.prefix, "topom-cluster", name)
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
		m, err := s.LoadSlotMapping(i)
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

func (s *Store) LoadSlotMapping(sid int) (*SlotMapping, error) {
	b, err := s.client.Read(s.SlotPath(sid))
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
	paths, err := s.client.List(s.GroupBase())
	if err != nil {
		return nil, err
	}
	group := make(map[int]*Group)
	for _, path := range paths {
		b, err := s.client.Read(path)
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

func (s *Store) LoadGroup(gid int) (*Group, error) {
	b, err := s.client.Read(s.GroupPath(gid))
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
	paths, err := s.client.List(s.ProxyBase())
	if err != nil {
		return nil, err
	}
	proxy := make(map[string]*Proxy)
	for _, path := range paths {
		b, err := s.client.Read(path)
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

func (s *Store) LoadProxy(token string) (*Proxy, error) {
	b, err := s.client.Read(s.ProxyPath(token))
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

func (s *Store) CreateTopomClusterEphemeral(topom *Topom) (<-chan struct{}, error) {
	w, _, err := s.client.CreateEphemeralInOrder(s.TopomClusterBase(), topom.Encode())
	return w, err
}

func (s *Store) LoadTopomClusterEphemeral(name string) (*Topom, error) {
	b, err := s.client.Read(s.TopomClusterPath(name))
	if err != nil {
		return nil, err
	}
	if b != nil {
		var t = &Topom{}
		if err := jsonDecode(t, b); err != nil {
			return nil, err
		}
		return t, nil
	}
	return nil, nil
}

func (s *Store) WatchTopomClusterLeader() (<-chan struct{}, string, error) {
	w, paths, err := s.client.ListEphemeralInOrder(s.TopomClusterBase())
	if err != nil {
		return nil, "", err
	}
	var leader string
	if len(paths) != 0 {
		_, leader = filepath.Split(DecodePath(paths[0]))
	}
	return w, leader, nil
}
