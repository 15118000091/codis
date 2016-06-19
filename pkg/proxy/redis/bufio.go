package redis

import (
	"bufio"
	"bytes"
	"io"

	"github.com/CodisLabs/codis/pkg/utils/math2"
)

type Reader struct {
	err error
	buf []byte

	rd   io.Reader
	rpos int
	wpos int
	last int

	pool []byte
}

func NewReaderSize(rd io.Reader, size int) *Reader {
	size = math2.MaxInt(size, 1024)
	return &Reader{rd: rd, buf: make([]byte, size), last: -1}
}

func (b *Reader) makeSlice(n int) []byte {
	if n >= 128 {
		return make([]byte, n)
	}
	var p = b.pool
	if len(p) < n {
		p = make([]byte, 8192)
	}
	b.pool = p[n:]
	return p[:n:n]
}

func (b *Reader) fill() error {
	if b.err != nil {
		return b.err
	}
	if b.rpos > 0 {
		copy(b.buf, b.buf[b.rpos:b.wpos])
		b.wpos = b.wpos - b.rpos
		b.rpos = 0
	}
	if b.wpos == len(b.buf) {
		return nil
	}
	n, err := b.rd.Read(b.buf[b.wpos:])
	if err != nil {
		b.err = err
	} else if n == 0 {
		b.err = io.ErrNoProgress
	} else {
		b.wpos = b.wpos + n
	}
	return b.err
}

func (b *Reader) Read(p []byte) (int, error) {
	if b.err != nil || len(p) == 0 {
		return 0, b.err
	}
	if b.buffered() == 0 {
		if len(p) >= len(b.buf) {
			n, err := b.rd.Read(p)
			if err != nil {
				b.err = err
			} else {
				b.last = int(p[n-1])
			}
			return n, b.err
		}
		if b.fill() != nil {
			return 0, b.err
		}
	}
	n := copy(p, b.buf[b.rpos:b.wpos])
	b.rpos = b.rpos + n
	b.last = int(p[n-1])
	return n, nil
}

func (b *Reader) ReadByte() (byte, error) {
	if b.err != nil {
		return 0, b.err
	}
	if b.buffered() == 0 {
		if b.fill() != nil {
			return 0, b.err
		}
	}
	c := b.buf[b.rpos]
	b.rpos = b.rpos + 1
	b.last = int(c)
	return c, nil
}

func (b *Reader) UnreadByte() error {
	if b.err != nil {
		return b.err
	}
	if b.last < 0 {
		return bufio.ErrInvalidUnreadByte
	}
	if b.wpos > 0 && b.rpos == 0 {
		return bufio.ErrInvalidUnreadByte
	}
	if b.rpos > 0 {
		b.rpos = b.rpos - 1
	} else {
		b.wpos = 1
	}
	b.buf[b.rpos] = byte(b.last)
	b.last = -1
	return nil
}

func (b *Reader) buffered() int {
	return b.wpos - b.rpos
}

func (b *Reader) ReadSlice(delim byte) ([]byte, error) {
	if b.err != nil {
		return nil, b.err
	}
	for {
		var index = bytes.IndexByte(b.buf[b.rpos:b.wpos], delim)
		if index >= 0 {
			n := index + 1
			s := b.buf[b.rpos : b.rpos+n]
			b.rpos = b.rpos + n
			b.last = int(s[n-1])
			return s, nil
		}
		if b.buffered() == len(b.buf) {
			n := len(b.buf)
			s := b.buf
			b.rpos = b.wpos
			b.last = int(s[n-1])
			return s, bufio.ErrBufferFull
		}
		if b.fill() != nil {
			return nil, b.err
		}
	}
}

func (b *Reader) ReadBytes(delim byte) ([]byte, error) {
	var last []byte
	var full [][]byte
	for last == nil {
		f, err := b.ReadSlice(delim)
		if err != nil {
			if err != bufio.ErrBufferFull {
				return nil, b.err
			}
			dup := b.makeSlice(len(f))
			copy(dup, f)
			full = append(full, dup)
		} else {
			last = f
		}
	}
	var size int
	for _, frag := range full {
		size += len(frag)
	}
	size += len(last)

	var n int
	var s = b.makeSlice(size)
	for _, frag := range full {
		n += copy(s[n:], frag)
	}
	copy(s[n:], last)
	return s, nil
}

type Writer struct {
	err error
	buf []byte

	wr  io.Writer
	end int
}

func NewWriterSize(wr io.Writer, size int) *Writer {
	if size <= 0 {
		size = 8192
	}
	return &Writer{wr: wr, buf: make([]byte, size)}
}

func (b *Writer) Flush() error {
	return b.flush()
}

func (b *Writer) flush() error {
	if b.err != nil {
		return b.err
	}
	if b.end == 0 {
		return nil
	}
	n, err := b.wr.Write(b.buf[:b.end])
	if err != nil {
		b.err = err
	} else if n < b.end {
		b.err = io.ErrShortWrite
	} else {
		b.end = 0
	}
	return b.err
}

func (b *Writer) available() int {
	return len(b.buf) - b.end
}

func (b *Writer) Write(p []byte) (nn int, err error) {
	for b.err == nil && len(p) > b.available() {
		var n int
		if b.end == 0 {
			n, b.err = b.wr.Write(p)
		} else {
			n = copy(b.buf[b.end:], p)
			b.end = b.end + n
			b.flush()
		}
		nn, p = nn+n, p[n:]
	}
	if b.err != nil || len(p) == 0 {
		return nn, b.err
	}
	n := copy(b.buf[b.end:], p)
	b.end = b.end + n
	return nn + n, nil
}

func (b *Writer) WriteByte(c byte) error {
	if b.err != nil {
		return b.err
	}
	if b.available() == 0 && b.flush() != nil {
		return b.err
	}
	b.buf[b.end] = c
	b.end = b.end + 1
	return nil
}

func (b *Writer) WriteString(s string) (nn int, err error) {
	for b.err == nil && len(s) > b.available() {
		n := copy(b.buf[b.end:], s)
		b.end = b.end + n
		b.flush()
		nn, s = nn+n, s[n:]
	}
	if b.err != nil || len(s) == 0 {
		return nn, b.err
	}
	n := copy(b.buf[b.end:], s)
	b.end = b.end + n
	return nn + n, nil
}
