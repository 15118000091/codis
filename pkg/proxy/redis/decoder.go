// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package redis

import (
	"bufio"
	"bytes"
	"io"
	"strconv"

	"github.com/CodisLabs/codis/pkg/utils/errors"
)

var (
	ErrBadRespCRLFEnd  = errors.New("bad resp CRLF end")
	ErrBadRespBytesLen = errors.New("bad resp bytes len")
	ErrBadRespArrayLen = errors.New("bad resp array len")

	ErrBadRespBytesLenTooLong = errors.New("bad resp bytes len, too long")
	ErrBadRespArrayLenTooLong = errors.New("bad resp array len, too long")
)

func btoi(b []byte) (int64, error) {
	if len(b) != 0 && len(b) < 10 {
		var neg, i = false, 0
		switch b[0] {
		case '-':
			neg = true
			fallthrough
		case '+':
			i++
		}
		if len(b) != i {
			var n int64
			for ; i < len(b) && b[i] >= '0' && b[i] <= '9'; i++ {
				n = int64(b[i]-'0') + n*10
			}
			if len(b) == i {
				if neg {
					n = -n
				}
				return n, nil
			}
		}
	}

	if n, err := strconv.ParseInt(string(b), 10, 64); err != nil {
		return 0, errors.Trace(err)
	} else {
		return n, nil
	}
}

type Decoder struct {
	br *bufio.Reader

	Err error
}

var ErrFailedDecoder = errors.New("use of failed decoder")

func NewDecoder(br *bufio.Reader) *Decoder {
	return &Decoder{br: br}
}

func NewDecoderSize(r io.Reader, size int) *Decoder {
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReaderSize(r, size)
	}
	return &Decoder{br: br}
}

func (d *Decoder) Decode() (*Resp, error) {
	if d.Err != nil {
		return nil, errors.Trace(ErrFailedDecoder)
	}
	r, err := d.decodeResp(0)
	if err != nil {
		d.Err = err
	}
	return r, err
}

func Decode(br *bufio.Reader) (*Resp, error) {
	return NewDecoder(br).Decode()
}

func DecodeFromBytes(p []byte) (*Resp, error) {
	return Decode(bufio.NewReader(bytes.NewReader(p)))
}

func (d *Decoder) decodeResp(depth int) (*Resp, error) {
	b, err := d.br.ReadByte()
	if err != nil {
		return nil, errors.Trace(err)
	}
	switch t := RespType(b); t {
	case TypeString, TypeError, TypeInt:
		r := &Resp{Type: t}
		r.Value, err = d.decodeTextBytes()
		return r, err
	case TypeBulkBytes:
		r := &Resp{Type: t}
		r.Value, err = d.decodeBulkBytes()
		return r, err
	case TypeArray:
		r := &Resp{Type: t}
		r.Array, err = d.decodeArray(depth)
		return r, err
	default:
		if depth != 0 {
			return nil, errors.Errorf("bad resp type %s", t)
		}
		if err := d.br.UnreadByte(); err != nil {
			return nil, errors.Trace(err)
		}
		r := &Resp{Type: TypeArray}
		r.Array, err = d.decodeSingleLineBulkBytesArray()
		return r, err
	}
}

func (d *Decoder) decodeTextBytes() ([]byte, error) {
	b, err := d.br.ReadBytes('\n')
	if err != nil {
		return nil, errors.Trace(err)
	}
	if n := len(b) - 2; n < 0 || b[n] != '\r' {
		return nil, errors.Trace(ErrBadRespCRLFEnd)
	} else {
		return b[:n], nil
	}
}

func (d *Decoder) decodeTextBytesUnsafe() ([]byte, error) {
	b, err := d.br.ReadSlice('\n')
	if err != nil {
		if err != bufio.ErrBufferFull {
			return nil, errors.Trace(err)
		}
		prefix := append(make([]byte, 0, len(b)+16), b...)
		s, err := d.br.ReadBytes('\n')
		if err != nil {
			return nil, errors.Trace(err)
		}
		b = append(prefix, s...)
	}
	if n := len(b) - 2; n < 0 || b[n] != '\r' {
		return nil, errors.Trace(ErrBadRespCRLFEnd)
	} else {
		return b[:n], nil
	}
}

func (d *Decoder) decodeInt() (int64, error) {
	b, err := d.decodeTextBytesUnsafe()
	if err != nil {
		return 0, err
	}
	return btoi(b)
}

func (d *Decoder) decodeBulkBytes() ([]byte, error) {
	n, err := d.decodeInt()
	if err != nil {
		return nil, err
	}
	switch {
	case n < -1:
		return nil, errors.Trace(ErrBadRespBytesLen)
	case n > 1024*1024*512:
		return nil, errors.Trace(ErrBadRespBytesLenTooLong)
	case n == -1:
		return nil, nil
	}
	b := make([]byte, n+2)
	if _, err := io.ReadFull(d.br, b); err != nil {
		return nil, errors.Trace(err)
	}
	if b[n] != '\r' || b[n+1] != '\n' {
		return nil, errors.Trace(ErrBadRespCRLFEnd)
	}
	return b[:n], nil
}

func (d *Decoder) decodeArray(depth int) ([]*Resp, error) {
	n, err := d.decodeInt()
	if err != nil {
		return nil, err
	}
	switch {
	case n < -1:
		return nil, errors.Trace(ErrBadRespArrayLen)
	case n > 1024*1024:
		return nil, errors.Trace(ErrBadRespArrayLenTooLong)
	case n == -1:
		return nil, nil
	}
	a := make([]*Resp, n)
	for i := 0; i < len(a); i++ {
		if a[i], err = d.decodeResp(depth + 1); err != nil {
			return nil, err
		}
	}
	return a, nil
}

func (d *Decoder) decodeSingleLineBulkBytesArray() ([]*Resp, error) {
	b, err := d.decodeTextBytes()
	if err != nil {
		return nil, err
	}
	a := make([]*Resp, 0, 4)
	for l, r := 0, 0; r <= len(b); r++ {
		if r == len(b) || b[r] == ' ' {
			if l < r {
				a = append(a, &Resp{
					Type:  TypeBulkBytes,
					Value: b[l:r],
				})
			}
			l = r + 1
		}
	}
	return a, nil
}
