// Copyright 2019 The Prometheus Authors
//
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

package wal

import (
	"encoding/binary"
	"hash/crc32"
	"io"

	"github.com/golang/snappy"
	"github.com/pkg/errors"
)

// Reader reads WAL records from an io.Reader.
// Reader从一个io.Reader中读取WAL records
type Reader struct {
	rdr       io.Reader
	err       error
	rec       []byte
	snappyBuf []byte
	buf       [pageSize]byte
	total     int64   // Total bytes processed.
	curRecTyp recType // Used for checking that the last record is not torn.
}

// NewReader returns a new reader.
func NewReader(r io.Reader) *Reader {
	return &Reader{rdr: r}
}

// Next advances the reader to the next records and returns true if it exists.
// It must not be called again after it returned false.
// Next将reader移动到下一个records并且返回true，如果存在的话
// 在它返回false之后不能被再次调用
func (r *Reader) Next() bool {
	err := r.next()
	if errors.Cause(err) == io.EOF {
		// The last WAL segment record shouldn't be torn(should be full or last).
		// The last record would be torn after a crash just before
		// the last record part could be persisted to disk.
		// 最后一个WAL segment record不应该被撕裂（应该为full或者last）
		// 在一次crash之后，如果最后一个record没写入磁盘，最后一个record会被撕裂
		if r.curRecTyp == recFirst || r.curRecTyp == recMiddle {
			r.err = errors.New("last record is torn")
		}
		return false
	}
	r.err = err
	return r.err == nil
}

func (r *Reader) next() (err error) {
	// We have to use r.buf since allocating byte arrays here fails escape
	// analysis and ends up on the heap, even though it seemingly should not.
	// 我们在这里使用r.buf，因为在这里申请byte arrays会导致escape analysis失败，最终在heap中分配
	// 虽然看起来不应该这样
	hdr := r.buf[:recordHeaderSize]
	buf := r.buf[recordHeaderSize:]

	r.rec = r.rec[:0]
	r.snappyBuf = r.snappyBuf[:0]

	i := 0
	for {
		if _, err = io.ReadFull(r.rdr, hdr[:1]); err != nil {
			return errors.Wrap(err, "read first header byte")
		}
		r.total++
		// 获取当前record的类型
		r.curRecTyp = recTypeFromHeader(hdr[0])
		compressed := hdr[0]&snappyMask != 0

		// Gobble up zero bytes.
		if r.curRecTyp == recPageTerm {
			// recPageTerm is a single byte that indicates the rest of the page is padded.
			// recPageTerm是一个字节，表示剩余的page为空
			// If it's the first byte in a page, buf is too small and
			// needs to be resized to fit pageSize-1 bytes.
			// 如果这是一个page的第一个字节，buf太小了并且需要resize到符合pageSize-1
			buf = r.buf[1:]

			// We are pedantic and check whether the zeros are actually up
			// to a page boundary.
			// It's not strictly necessary but may catch sketchy state early.
			k := pageSize - (r.total % pageSize)
			if k == pageSize {
				continue // Initial 0 byte was last page byte.
			}
			n, err := io.ReadFull(r.rdr, buf[:k])
			if err != nil {
				return errors.Wrap(err, "read remaining zeros")
			}
			r.total += int64(n)

			for _, c := range buf[:k] {
				// 确定padded page都要用0填充
				if c != 0 {
					return errors.New("unexpected non-zero byte in padded page")
				}
			}
			continue
		}
		// 读取剩余的header
		n, err := io.ReadFull(r.rdr, hdr[1:])
		if err != nil {
			return errors.Wrap(err, "read remaining header")
		}
		r.total += int64(n)

		var (
			length = binary.BigEndian.Uint16(hdr[1:])
			crc    = binary.BigEndian.Uint32(hdr[3:])
		)

		// 如果record的大小大于page剩余的大小
		if length > pageSize-recordHeaderSize {
			return errors.Errorf("invalid record size %d", length)
		}
		// 读取header中指定的长度
		n, err = io.ReadFull(r.rdr, buf[:length])
		if err != nil {
			return err
		}
		// 计算总的读取的数目
		r.total += int64(n)

		// 读取到的字节数必须和指定的record的长度相等
		if n != int(length) {
			return errors.Errorf("invalid size: expected %d, got %d", length, n)
		}
		if c := crc32.Checksum(buf[:length], castagnoliTable); c != crc {
			return errors.Errorf("unexpected checksum %x, expected %x", c, crc)
		}

		if compressed {
			r.snappyBuf = append(r.snappyBuf, buf[:length]...)
		} else {
			r.rec = append(r.rec, buf[:length]...)
		}

		if err := validateRecord(r.curRecTyp, i); err != nil {
			return err
		}
		// 如果是完整的record或者是record的最后一个fragment
		if r.curRecTyp == recLast || r.curRecTyp == recFull {
			if compressed && len(r.snappyBuf) > 0 {
				// The snappy library uses `len` to calculate if we need a new buffer.
				// In order to allocate as few buffers as possible make the length
				// equal to the capacity.
				r.rec = r.rec[:cap(r.rec)]
				r.rec, err = snappy.Decode(r.rec, r.snappyBuf)
				return err
			}
			return nil
		}

		// Only increment i for non-zero records since we use it
		// to determine valid content record sequences.
		// 只有遇到非零的records的时候增加i，因为我们使用它来确定合法的content record的数目
		i++
	}
}

// Err returns the last encountered error wrapped in a corruption error.
// If the reader does not allow to infer a segment index and offset, a total
// offset in the reader stream will be provided.
func (r *Reader) Err() error {
	if r.err == nil {
		return nil
	}
	if b, ok := r.rdr.(*segmentBufReader); ok {
		return &CorruptionErr{
			Err:     r.err,
			Dir:     b.segs[b.cur].Dir(),
			Segment: b.segs[b.cur].Index(),
			Offset:  int64(b.off),
		}
	}
	return &CorruptionErr{
		Err:     r.err,
		Segment: -1,
		Offset:  r.total,
	}
}

// Record returns the current record. The returned byte slice is only
// valid until the next call to Next.
func (r *Reader) Record() []byte {
	return r.rec
}

// Segment returns the current segment being read.
// Segment返回当前正在读取的segment
func (r *Reader) Segment() int {
	if b, ok := r.rdr.(*segmentBufReader); ok {
		return b.segs[b.cur].Index()
	}
	return -1
}

// Offset returns the current position of the segment being read.
// Offset返回当前正在读取的segment的当前位置
func (r *Reader) Offset() int64 {
	if b, ok := r.rdr.(*segmentBufReader); ok {
		return int64(b.off)
	}
	return r.total
}
