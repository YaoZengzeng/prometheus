// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chunkenc

import (
	"fmt"
	"sync"

	"github.com/pkg/errors"
)

// Encoding is the identifier for a chunk encoding.
// Encoding是一个chunk encoding的标识符
type Encoding uint8

func (e Encoding) String() string {
	switch e {
	case EncNone:
		return "none"
	case EncXOR:
		return "XOR"
	}
	return "<unknown>"
}

// The different available chunk encodings.
// 不同的可用的chunk编码方式
const (
	EncNone Encoding = iota
	EncXOR
)

// Chunk holds a sequence of sample pairs that can be iterated over and appended to.
// Chunk维护了一系列的sample pairs，可以被迭代以及追加
type Chunk interface {
	Bytes() []byte
	Encoding() Encoding
	Appender() (Appender, error)
	Iterator() Iterator
	// 返回samples的数目
	NumSamples() int
}

// Appender adds sample pairs to a chunk.
// Appender增加sample pairs到一个chunk中
type Appender interface {
	Append(int64, float64)
}

// Iterator is a simple iterator that can only get the next value.
// Iterator是一个简单的iterator，只能用于获取下一个value
type Iterator interface {
	At() (int64, float64)
	Err() error
	Next() bool
}

// NewNopIterator returns a new chunk iterator that does not hold any data.
func NewNopIterator() Iterator {
	return nopIterator{}
}

type nopIterator struct{}

func (nopIterator) At() (int64, float64) { return 0, 0 }
func (nopIterator) Next() bool           { return false }
func (nopIterator) Err() error           { return nil }

// Pool is used to create and reuse chunk references to avoid allocations.
// Pool是用来创建以及重用chunc references来避免内存申请
type Pool interface {
	Put(Chunk) error
	Get(e Encoding, b []byte) (Chunk, error)
}

// pool is a memory pool of chunk objects.
// pool是chunk对象的一个memory pool
type pool struct {
	xor sync.Pool
}

// NewPool returns a new pool.
func NewPool() Pool {
	return &pool{
		xor: sync.Pool{
			New: func() interface{} {
				// 返回XORChunk的函数
				return &XORChunk{b: bstream{}}
			},
		},
	}
}

func (p *pool) Get(e Encoding, b []byte) (Chunk, error) {
	switch e {
	case EncXOR:
		c := p.xor.Get().(*XORChunk)
		c.b.stream = b
		c.b.count = 0
		return c, nil
	}
	return nil, errors.Errorf("invalid encoding %q", e)
}

func (p *pool) Put(c Chunk) error {
	switch c.Encoding() {
	case EncXOR:
		xc, ok := c.(*XORChunk)
		// This may happen often with wrapped chunks. Nothing we can really do about
		// it but returning an error would cause a lot of allocations again. Thus,
		// we just skip it.
		if !ok {
			return nil
		}
		xc.b.stream = nil
		xc.b.count = 0
		p.xor.Put(c)
	default:
		return errors.Errorf("invalid encoding %q", c.Encoding())
	}
	return nil
}

// FromData returns a chunk from a byte slice of chunk data.
// This is there so that users of the library can easily create chunks from
// bytes.
func FromData(e Encoding, d []byte) (Chunk, error) {
	switch e {
	case EncXOR:
		return &XORChunk{b: bstream{count: 0, stream: d}}, nil
	}
	return nil, fmt.Errorf("unknown chunk encoding: %d", e)
}
