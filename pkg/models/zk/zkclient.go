// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package zkclient

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/samuel/go-zookeeper/zk"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

var ErrClosedZkClient = errors.New("use of closed zk client")

var DefaultLogfunc = func(format string, v ...interface{}) {
	log.Info("zookeeper - " + fmt.Sprintf(format, v...))
}

type ZkClient struct {
	sync.Mutex
	conn *zk.Conn

	addrlist string
	timeout  time.Duration

	logger *zkLogger
	dialAt time.Time
	closed bool
}

type zkLogger struct {
	logfunc func(format string, v ...interface{})
}

func (l *zkLogger) Printf(format string, v ...interface{}) {
	if l != nil && l.logfunc != nil {
		l.logfunc(format, v...)
	}
}

func New(addrlist string, timeout time.Duration) (*ZkClient, error) {
	return NewWithLogfunc(addrlist, timeout, DefaultLogfunc)
}

func NewWithLogfunc(addrlist string, timeout time.Duration, logfunc func(foramt string, v ...interface{})) (*ZkClient, error) {
	if timeout <= 0 {
		timeout = time.Second * 5
	}
	c := &ZkClient{
		addrlist: addrlist, timeout: timeout,
		logger: &zkLogger{logfunc},
	}
	if err := c.reset(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *ZkClient) reset() error {
	c.dialAt = time.Now()
	conn, events, err := zk.Connect(strings.Split(c.addrlist, ","), c.timeout)
	if err != nil {
		return errors.Trace(err)
	}
	if c.conn != nil {
		c.conn.Close()
	}
	c.conn = conn
	c.conn.SetLogger(c.logger)

	c.logger.Printf("zkclient setup new connection to %s", c.addrlist)

	go func() {
		for e := range events {
			log.Debugf("zookeeper event: %+v", e)
		}
	}()
	return nil
}

func (c *ZkClient) Close() error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true

	if c.conn != nil {
		c.conn.Close()
	}
	return nil
}

func (c *ZkClient) Do(fn func(conn *zk.Conn) error) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return errors.Trace(ErrClosedZkClient)
	}
	return c.shell(fn)
}

func (c *ZkClient) shell(fn func(conn *zk.Conn) error) error {
	if err := fn(c.conn); err != nil {
		for _, e := range []error{zk.ErrNoNode, zk.ErrNodeExists, zk.ErrNotEmpty} {
			if errors.Equal(e, err) {
				return err
			}
		}
		if retryAt := c.dialAt.Add(time.Second); time.Now().After(retryAt) {
			if err := c.reset(); err != nil {
				log.DebugErrorf(err, "zkclient reset connection failed")
			}
		}
		return err
	}
	return nil
}

func (c *ZkClient) Mkdir(path string) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return errors.Trace(ErrClosedZkClient)
	}
	log.Debugf("zkclient mkdir node %s", path)
	err := c.shell(func(conn *zk.Conn) error {
		return c.mkdir(conn, path)
	})
	if err != nil {
		log.Debugf("zkclient mkdir node %s failed: %s", path, err)
		return err
	}
	log.Debugf("zkclient mkdir OK")
	return nil
}

func (c *ZkClient) mkdir(conn *zk.Conn, path string) error {
	if path == "" || path == "/" {
		return nil
	}
	if exists, _, err := conn.Exists(path); err != nil {
		return errors.Trace(err)
	} else if exists {
		return nil
	}
	if err := c.mkdir(conn, filepath.Dir(path)); err != nil {
		return err
	}
	_, err := conn.Create(path, []byte{}, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *ZkClient) Create(path string, data []byte) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return errors.Trace(ErrClosedZkClient)
	}
	log.Debugf("zkclient create node %s", path)
	err := c.shell(func(conn *zk.Conn) error {
		_, err := c.create(conn, path, data, 0)
		return err
	})
	if err != nil {
		log.Debugf("zkclient create node %s failed: %s", path, err)
		return err
	}
	log.Debugf("zkclient create OK")
	return nil
}

func (c *ZkClient) CreateEphemeral(path string, data []byte) (<-chan struct{}, error) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, errors.Trace(ErrClosedZkClient)
	}
	var signal <-chan struct{}
	log.Debugf("zkclient create-ephemeral node %s", path)
	err := c.shell(func(conn *zk.Conn) error {
		p, err := c.create(conn, path, data, zk.FlagEphemeral)
		if err != nil {
			return err
		}
		w, err := c.watch(conn, p)
		if err != nil {
			return err
		}
		signal = w
		return nil
	})
	if err != nil {
		log.Debugf("zkclient create-ephemeral node %s failed: %s", path, err)
		return nil, err
	}
	log.Debugf("zkclient create-ephemeral OK", path)
	return signal, nil
}

func (c *ZkClient) create(conn *zk.Conn, path string, data []byte, flag int32) (string, error) {
	if err := c.mkdir(conn, filepath.Dir(path)); err != nil {
		return "", err
	}
	p, err := conn.Create(path, data, flag, zk.WorldACL(zk.PermAdmin|zk.PermRead|zk.PermWrite))
	if err != nil {
		return "", errors.Trace(err)
	}
	return p, nil
}

func (c *ZkClient) watch(conn *zk.Conn, path string) (<-chan struct{}, error) {
	_, _, w, err := conn.GetW(path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	signal := make(chan struct{})
	go func() {
		defer close(signal)
		<-w
		log.Debugf("zkclient watch node %s update", path)
	}()
	return signal, nil
}

func (c *ZkClient) Update(path string, data []byte) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return errors.Trace(ErrClosedZkClient)
	}
	log.Debugf("zkclient update node %s", path)
	err := c.shell(func(conn *zk.Conn) error {
		return c.update(conn, path, data)
	})
	if err != nil {
		log.Debugf("zkclient update node %s failed: %s", path, err)
		return err
	}
	log.Debugf("zkclient update OK")
	return nil
}

func (c *ZkClient) update(conn *zk.Conn, path string, data []byte) error {
	if exists, _, err := conn.Exists(path); err != nil {
		return errors.Trace(err)
	} else if !exists {
		_, err := c.create(conn, path, data, 0)
		if err != nil && errors.NotEqual(err, zk.ErrNodeExists) {
			return err
		}
	}
	_, err := conn.Set(path, data, -1)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *ZkClient) Delete(path string) error {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return errors.Trace(ErrClosedZkClient)
	}
	log.Debugf("zkclient delete node %s", path)
	err := c.shell(func(conn *zk.Conn) error {
		err := conn.Delete(path, -1)
		if err != nil && errors.NotEqual(err, zk.ErrNoNode) {
			return errors.Trace(err)
		}
		return nil
	})
	if err != nil {
		log.Debugf("zkclient delete node %s failed: %s", path, err)
		return err
	}
	log.Debugf("zkclient delete OK")
	return nil
}

func (c *ZkClient) Read(path string) ([]byte, error) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, errors.Trace(ErrClosedZkClient)
	}
	var data []byte
	err := c.shell(func(conn *zk.Conn) error {
		b, _, err := conn.Get(path)
		if err != nil && errors.NotEqual(err, zk.ErrNoNode) {
			return errors.Trace(err)
		}
		data = b
		return nil
	})
	if err != nil {
		log.Debugf("zkclient read node %s failed: %s", path, err)
		return nil, err
	}
	return data, nil
}

func (c *ZkClient) List(path string) ([]string, error) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, errors.Trace(ErrClosedZkClient)
	}
	var paths []string
	err := c.shell(func(conn *zk.Conn) error {
		nodes, _, err := conn.Children(path)
		if err != nil && errors.NotEqual(err, zk.ErrNoNode) {
			return errors.Trace(err)
		}
		for _, node := range nodes {
			paths = append(paths, models.EncodePath(path, node))
		}
		return nil
	})
	if err != nil {
		log.Debugf("zkclient list node %s failed: %s", path, err)
		return nil, err
	}
	return paths, nil
}

func (c *ZkClient) CreateEphemeralInOrder(path string, data []byte) (<-chan struct{}, string, error) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, "", errors.Trace(ErrClosedZkClient)
	}
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}
	var signal <-chan struct{}
	var node string
	log.Debugf("zkclient create-ephemeral-inorder node %s", path)
	err := c.shell(func(conn *zk.Conn) error {
		p, err := c.create(conn, path, data, zk.FlagEphemeral|zk.FlagSequence)
		if err != nil {
			return err
		}
		w, err := c.watch(conn, p)
		if err != nil {
			return err
		}
		signal, node = w, p
		return nil
	})
	if err != nil {
		log.Debugf("zkclient create-ephemeral-inorder node %s failed: %s", path, err)
		return nil, "", err
	}
	log.Debugf("zkclient create-ephemeral-inorder OK, node = %s", node)
	return signal, node, nil
}

func (c *ZkClient) ListEphemeralInOrder(path string) (<-chan struct{}, []string, error) {
	if err := c.Mkdir(path); err != nil {
		return nil, nil, err
	}
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return nil, nil, errors.Trace(ErrClosedZkClient)
	}
	var signal chan struct{}
	var paths []string
	log.Debugf("zkclient list-ephemeral-inorder node %s", path)
	err := c.shell(func(conn *zk.Conn) error {
		nodes, _, w, err := conn.ChildrenW(path)
		if err != nil {
			return err
		}
		sort.Strings(nodes)
		for _, node := range nodes {
			paths = append(paths, models.EncodePath(path, node))
		}
		signal = make(chan struct{})
		go func() {
			defer close(signal)
			<-w
			log.Debugf("zkclient list-ephemeral-inorder node %s update", path)
		}()
		return nil
	})
	if err != nil {
		log.Debugf("zkclient list-ephemeral-inorder node %s failed: %s", path, err)
		return nil, nil, err
	}
	log.Debugf("zkclient list-ephemeral-inorder OK")
	return signal, paths, nil
}
