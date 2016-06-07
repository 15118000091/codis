// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package redis

import (
	"net"
	"time"

	"github.com/CodisLabs/codis/pkg/utils/errors"
)

type Conn struct {
	Socket net.Conn
	Reader *Decoder
	Writer *Encoder

	ReaderTimeout time.Duration
	WriterTimeout time.Duration
}

func DialTimeout(addr string, bufsize int, timeout time.Duration) (*Conn, error) {
	c, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return NewConnSize(c, bufsize), nil
}

func NewConn(sock net.Conn) *Conn {
	return NewConnSize(sock, 1024*64)
}

func NewConnSize(sock net.Conn, bufsize int) *Conn {
	conn := &Conn{Socket: sock}
	conn.Reader = NewDecoderSize(&connReader{Conn: conn}, bufsize)
	conn.Writer = NewEncoderSize(&connWriter{Conn: conn}, bufsize)
	return conn
}

func (c *Conn) LocalAddr() string {
	return c.Socket.LocalAddr().String()
}

func (c *Conn) RemoteAddr() string {
	return c.Socket.RemoteAddr().String()
}

func (c *Conn) Close() error {
	return c.Socket.Close()
}

func (c *Conn) SetKeepAlive(keepalive bool) error {
	if t, ok := c.Socket.(*net.TCPConn); ok {
		if err := t.SetKeepAlive(keepalive); err != nil {
			return errors.Trace(err)
		}
		return nil
	}
	return errors.Errorf("not tcp connection")
}

func (c *Conn) SetKeepAlivePeriod(d time.Duration) error {
	if t, ok := c.Socket.(*net.TCPConn); ok {
		if err := t.SetKeepAlivePeriod(d); err != nil {
			return errors.Trace(err)
		}
		return nil
	}
	return errors.Errorf("not tcp connection")
}

type connReader struct {
	*Conn
	hasDeadline bool
}

func (r *connReader) Read(b []byte) (int, error) {
	if timeout := r.ReaderTimeout; timeout != 0 {
		if err := r.Socket.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			return 0, errors.Trace(err)
		}
		r.hasDeadline = true
	} else if r.hasDeadline {
		if err := r.Socket.SetReadDeadline(time.Time{}); err != nil {
			return 0, errors.Trace(err)
		}
		r.hasDeadline = false
	}
	n, err := r.Socket.Read(b)
	if err != nil {
		err = errors.Trace(err)
	}
	return n, err
}

type connWriter struct {
	*Conn
	hasDeadline bool
}

func (w *connWriter) Write(b []byte) (int, error) {
	if timeout := w.WriterTimeout; timeout != 0 {
		if err := w.Socket.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			return 0, errors.Trace(err)
		}
		w.hasDeadline = true
	} else if w.hasDeadline {
		if err := w.Socket.SetWriteDeadline(time.Time{}); err != nil {
			return 0, errors.Trace(err)
		}
		w.hasDeadline = false
	}
	n, err := w.Socket.Write(b)
	if err != nil {
		err = errors.Trace(err)
	}
	return n, err
}

func IsTimeout(err error) bool {
	if err := errors.Cause(err); err != nil {
		e, ok := err.(*net.OpError)
		if ok {
			return e.Timeout()
		}
	}
	return false
}
