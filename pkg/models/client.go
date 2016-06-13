package models

import (
	"time"

	"github.com/CodisLabs/codis/pkg/models/etcd"
	"github.com/CodisLabs/codis/pkg/models/zk"
	"github.com/CodisLabs/codis/pkg/utils/errors"
)

type Client interface {
	Create(path string, data []byte) error
	Update(path string, data []byte) error
	Delete(path string) error

	Read(path string, must bool) ([]byte, error)
	List(path string, must bool) ([]string, error)

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

func IsZkClient(c Client) bool {
	_, ok := c.(*zkclient.Client)
	return ok
}
