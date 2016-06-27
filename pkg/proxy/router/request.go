// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package router

import (
	"sync"

	"github.com/CodisLabs/codis/pkg/proxy/redis"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/sync2/atomic2"
)

type Dispatcher interface {
	Dispatch(r *Request) error
}

var ErrDiscardedRequest = errors.New("discarded request")

type Request struct {
	OpStr string
	Start int64

	Multi []*redis.Resp

	Coalesce func() error
	Response struct {
		Resp *redis.Resp
		Err  error
	}

	Wait *sync.WaitGroup
	slot *sync.WaitGroup

	Failed *atomic2.Bool
}

func NewRequest(multi []*redis.Resp) *Request {
	r := &Request{}
	r.Multi = multi
	return r
}

func (r *Request) SubRequest(multi []*redis.Resp) *Request {
	x := &Request{}
	x.OpStr = r.OpStr
	x.Multi = multi
	x.Wait = r.Wait
	x.Failed = r.Failed
	return x
}
