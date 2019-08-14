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

package chunks

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"

	"github.com/pkg/errors"
	"github.com/prometheus/tsdb/chunkenc"
	tsdb_errors "github.com/prometheus/tsdb/errors"
	"github.com/prometheus/tsdb/fileutil"
)

const (
	// MagicChunks is 4 bytes at the head of a series file.
	MagicChunks = 0x85BD40DD
	// MagicChunksSize is the size in bytes of MagicChunks.
	MagicChunksSize = 4

	chunksFormatV1          = 1
	ChunksFormatVersionSize = 1

	// chunkHeaderSize为5
	chunkHeaderSize = MagicChunksSize + ChunksFormatVersionSize
)

// Meta holds information about a chunk of data.
// Meta包含了一个chunk的数据的信息
type Meta struct {
	// Ref and Chunk hold either a reference that can be used to retrieve
	// chunk data or the data itself.
	// Generally, only one of them is set.
	Ref   uint64
	Chunk chunkenc.Chunk

	// Time range the data covers.
	// When MaxTime == math.MaxInt64 the chunk is still open and being appended to.
	MinTime, MaxTime int64
}

// writeHash writes the chunk encoding and raw data into the provided hash.
func (cm *Meta) writeHash(h hash.Hash) error {
	if _, err := h.Write([]byte{byte(cm.Chunk.Encoding())}); err != nil {
		return err
	}
	if _, err := h.Write(cm.Chunk.Bytes()); err != nil {
		return err
	}
	return nil
}

// OverlapsClosedInterval Returns true if the chunk overlaps [mint, maxt].
func (cm *Meta) OverlapsClosedInterval(mint, maxt int64) bool {
	// The chunk itself is a closed interval [cm.MinTime, cm.MaxTime].
	return cm.MinTime <= maxt && mint <= cm.MaxTime
}

var (
	errInvalidSize = fmt.Errorf("invalid size")
)

var castagnoliTable *crc32.Table

func init() {
	castagnoliTable = crc32.MakeTable(crc32.Castagnoli)
}

// newCRC32 initializes a CRC32 hash with a preconfigured polynomial, so the
// polynomial may be easily changed in one location at a later time, if necessary.
func newCRC32() hash.Hash32 {
	return crc32.New(castagnoliTable)
}

// Writer implements the ChunkWriter interface for the standard
// serialization format.
// Writer实现了ChunkWriter接口用于标准的序列化格式
type Writer struct {
	dirFile *os.File
	files   []*os.File
	wbuf    *bufio.Writer
	n       int64
	crc32   hash.Hash

	segmentSize int64
}

const (
	defaultChunkSegmentSize = 512 * 1024 * 1024
)

// NewWriter returns a new writer against the given directory.
// NewWriter返回一个给定目录的writer
func NewWriter(dir string) (*Writer, error) {
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, err
	}
	dirFile, err := fileutil.OpenDir(dir)
	if err != nil {
		return nil, err
	}
	cw := &Writer{
		dirFile:     dirFile,
		n:           0,
		crc32:       newCRC32(),
		// 默认的chunk segment size为512M
		segmentSize: defaultChunkSegmentSize,
	}
	return cw, nil
}

func (w *Writer) tail() *os.File {
	if len(w.files) == 0 {
		return nil
	}
	return w.files[len(w.files)-1]
}

// finalizeTail writes all pending data to the current tail file,
// truncates its size, and closes it.
// finalizeTail将所有pending的数据写入当前的的tail file
// 截取它的size并且关闭它
func (w *Writer) finalizeTail() error {
	tf := w.tail()
	if tf == nil {
		return nil
	}

	// 将缓存中的数据写入文件
	if err := w.wbuf.Flush(); err != nil {
		return err
	}
	if err := tf.Sync(); err != nil {
		return err
	}
	// As the file was pre-allocated, we truncate any superfluous zero bytes.
	// 截取多余的零字节
	off, err := tf.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if err := tf.Truncate(off); err != nil {
		return err
	}

	// 关闭文件
	return tf.Close()
}

func (w *Writer) cut() error {
	// Sync current tail to disk and close.
	// 同步当前的tail到磁盘并且关闭
	if err := w.finalizeTail(); err != nil {
		return err
	}

	p, _, err := nextSequenceFile(w.dirFile.Name())
	if err != nil {
		return err
	}
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	if err = fileutil.Preallocate(f, w.segmentSize, true); err != nil {
		return err
	}
	if err = w.dirFile.Sync(); err != nil {
		return err
	}

	// Write header metadata for new file.
	// 在新的文件的头部写入元数据
	metab := make([]byte, 8)
	binary.BigEndian.PutUint32(metab[:MagicChunksSize], MagicChunks)
	metab[4] = chunksFormatV1

	if _, err := f.Write(metab); err != nil {
		return err
	}

	w.files = append(w.files, f)
	if w.wbuf != nil {
		w.wbuf.Reset(f)
	} else {
		// 缓存大小为8M
		w.wbuf = bufio.NewWriterSize(f, 8*1024*1024)
	}
	// 重新将n设置为8
	w.n = 8

	return nil
}

func (w *Writer) write(b []byte) error {
	n, err := w.wbuf.Write(b)
	// n为累积的字节数
	w.n += int64(n)
	return err
}

// MergeOverlappingChunks removes the samples whose timestamp is overlapping.
// The last appearing sample is retained in case there is overlapping.
// This assumes that `chks []Meta` is sorted w.r.t. MinTime.
func MergeOverlappingChunks(chks []Meta) ([]Meta, error) {
	if len(chks) < 2 {
		return chks, nil
	}
	newChks := make([]Meta, 0, len(chks)) // Will contain the merged chunks.
	newChks = append(newChks, chks[0])
	last := 0
	for _, c := range chks[1:] {
		// We need to check only the last chunk in newChks.
		// Reason: (1) newChks[last-1].MaxTime < newChks[last].MinTime (non overlapping)
		//         (2) As chks are sorted w.r.t. MinTime, newChks[last].MinTime < c.MinTime.
		// So never overlaps with newChks[last-1] or anything before that.
		if c.MinTime > newChks[last].MaxTime {
			newChks = append(newChks, c)
			last++
			continue
		}
		nc := &newChks[last]
		if c.MaxTime > nc.MaxTime {
			nc.MaxTime = c.MaxTime
		}
		chk, err := MergeChunks(nc.Chunk, c.Chunk)
		if err != nil {
			return nil, err
		}
		nc.Chunk = chk
	}

	return newChks, nil
}

// MergeChunks vertically merges a and b, i.e., if there is any sample
// with same timestamp in both a and b, the sample in a is discarded.
func MergeChunks(a, b chunkenc.Chunk) (*chunkenc.XORChunk, error) {
	newChunk := chunkenc.NewXORChunk()
	app, err := newChunk.Appender()
	if err != nil {
		return nil, err
	}
	ait := a.Iterator()
	bit := b.Iterator()
	aok, bok := ait.Next(), bit.Next()
	for aok && bok {
		at, av := ait.At()
		bt, bv := bit.At()
		if at < bt {
			app.Append(at, av)
			aok = ait.Next()
		} else if bt < at {
			app.Append(bt, bv)
			bok = bit.Next()
		} else {
			app.Append(bt, bv)
			aok = ait.Next()
			bok = bit.Next()
		}
	}
	for aok {
		at, av := ait.At()
		app.Append(at, av)
		aok = ait.Next()
	}
	for bok {
		bt, bv := bit.At()
		app.Append(bt, bv)
		bok = bit.Next()
	}
	if ait.Err() != nil {
		return nil, ait.Err()
	}
	if bit.Err() != nil {
		return nil, bit.Err()
	}
	return newChunk, nil
}

func (w *Writer) WriteChunks(chks ...Meta) error {
	// Calculate maximum space we need and cut a new segment in case
	// we don't fit into the current one.
	// 计算我们需要的最大内存并且创建一个新的segment，如果当前的大小并不合适的话
	maxLen := int64(binary.MaxVarintLen32) // The number of chunks.
	for _, c := range chks {
		maxLen += binary.MaxVarintLen32 + 1 // The number of bytes in the chunk and its encoding.
		maxLen += int64(len(c.Chunk.Bytes()))
		maxLen += 4 // The 4 bytes of crc32
	}
	newsz := w.n + maxLen

	if w.wbuf == nil || w.n > w.segmentSize || newsz > w.segmentSize && maxLen <= w.segmentSize {
		// w.n > w.segmentSize这种情况是不可能发生的
		// 只有新加maxLen大于segmentSize才创建新的segment
		if err := w.cut(); err != nil {
			return err
		}
	}

	var (
		b   = [binary.MaxVarintLen32]byte{}
		// seq是当前写入的文件编号
		seq = uint64(w.seq()) << 32
	)
	// 将各个内存中的chunks写入chunks文件
	for i := range chks {
		chk := &chks[i]

		// 将ref合并为seq和w.n
		// 从chk.Ref能找到chunk在哪个文件中，以及在文件中的位置
		chk.Ref = seq | uint64(w.n)

		n := binary.PutUvarint(b[:], uint64(len(chk.Chunk.Bytes())))

		if err := w.write(b[:n]); err != nil {
			return err
		}
		b[0] = byte(chk.Chunk.Encoding())
		// 先写入chunk的编码类型
		if err := w.write(b[:1]); err != nil {
			return err
		}
		// 再写入chunk的内容
		if err := w.write(chk.Chunk.Bytes()); err != nil {
			return err
		}

		w.crc32.Reset()
		if err := chk.writeHash(w.crc32); err != nil {
			return err
		}
		// 再写入哈希
		if err := w.write(w.crc32.Sum(b[:0])); err != nil {
			return err
		}
	}

	return nil
}

func (w *Writer) seq() int {
	return len(w.files) - 1
}

func (w *Writer) Close() error {
	if err := w.finalizeTail(); err != nil {
		return err
	}

	// close dir file (if not windows platform will fail on rename)
	return w.dirFile.Close()
}

// ByteSlice abstracts a byte slice.
// ByteSlice抽象了一个字节切片
type ByteSlice interface {
	Len() int
	Range(start, end int) []byte
}

type realByteSlice []byte

func (b realByteSlice) Len() int {
	return len(b)
}

func (b realByteSlice) Range(start, end int) []byte {
	return b[start:end]
}

func (b realByteSlice) Sub(start, end int) ByteSlice {
	return b[start:end]
}

// Reader implements a SeriesReader for a serialized byte stream
// of series data.
// Reader实现了一个SeriesReader，对于series data的一系列序列化的byte stream
type Reader struct {
	// 底层的bytes包含了经过编码的series data
	bs   []ByteSlice // The underlying bytes holding the encoded series data.
	cs   []io.Closer // Closers for resources behind the byte slices.
	size int64       // The total size of bytes in the reader.
	pool chunkenc.Pool
}

func newReader(bs []ByteSlice, cs []io.Closer, pool chunkenc.Pool) (*Reader, error) {
	cr := Reader{pool: pool, bs: bs, cs: cs}
	var totalSize int64

	// 检测每个chunk文件的Header Size
	for i, b := range cr.bs {
		// chunk文件的大小必须大于chunkHeadSize
		if b.Len() < chunkHeaderSize {
			return nil, errors.Wrapf(errInvalidSize, "invalid chunk header in segment %d", i)
		}
		// Verify magic number.
		if m := binary.BigEndian.Uint32(b.Range(0, MagicChunksSize)); m != MagicChunks {
			return nil, errors.Errorf("invalid magic number %x", m)
		}

		// Verify chunk format version.
		if v := int(b.Range(MagicChunksSize, MagicChunksSize+ChunksFormatVersionSize)[0]); v != chunksFormatV1 {
			return nil, errors.Errorf("invalid chunk format version %d", v)
		}
		// 总大小加上b.Len()
		totalSize += int64(b.Len())
	}
	cr.size = totalSize
	return &cr, nil
}

// NewDirReader returns a new Reader against sequentially numbered files in the
// given directory.
// NewDirReader返回一个新的Reader，针对给定目录里的以顺序编号的文件
func NewDirReader(dir string, pool chunkenc.Pool) (*Reader, error) {
	// 读取chunks里面的序列文件
	files, err := sequenceFiles(dir)
	if err != nil {
		return nil, err
	}
	if pool == nil {
		pool = chunkenc.NewPool()
	}

	var (
		bs   []ByteSlice
		cs   []io.Closer
		merr tsdb_errors.MultiError
	)
	for _, fn := range files {
		// 打开mmap文件
		f, err := fileutil.OpenMmapFile(fn)
		if err != nil {
			merr.Add(errors.Wrap(err, "mmap files"))
			merr.Add(closeAll(cs))
			return nil, merr
		}
		cs = append(cs, f)
		bs = append(bs, realByteSlice(f.Bytes()))
	}

	reader, err := newReader(bs, cs, pool)
	if err != nil {
		merr.Add(err)
		merr.Add(closeAll(cs))
		return nil, merr
	}
	return reader, nil
}

func (s *Reader) Close() error {
	return closeAll(s.cs)
}

// Size returns the size of the chunks.
func (s *Reader) Size() int64 {
	return s.size
}

// Chunk returns a chunk from a given reference.
func (s *Reader) Chunk(ref uint64) (chunkenc.Chunk, error) {
	var (
		sgmSeq    = int(ref >> 32)
		sgmOffset = int((ref << 32) >> 32)
	)
	if sgmSeq >= len(s.bs) {
		return nil, errors.Errorf("reference sequence %d out of range", sgmSeq)
	}
	chkS := s.bs[sgmSeq]

	if sgmOffset >= chkS.Len() {
		return nil, errors.Errorf("offset %d beyond data size %d", sgmOffset, chkS.Len())
	}
	// With the minimum chunk length this should never cause us reading
	// over the end of the slice.
	chk := chkS.Range(sgmOffset, sgmOffset+binary.MaxVarintLen32)

	chkLen, n := binary.Uvarint(chk)
	if n <= 0 {
		return nil, errors.Errorf("reading chunk length failed with %d", n)
	}
	chk = chkS.Range(sgmOffset+n, sgmOffset+n+1+int(chkLen))

	return s.pool.Get(chunkenc.Encoding(chk[0]), chk[1:1+chkLen])
}

func nextSequenceFile(dir string) (string, int, error) {
	names, err := fileutil.ReadDir(dir)
	if err != nil {
		return "", 0, err
	}

	i := uint64(0)
	for _, n := range names {
		j, err := strconv.ParseUint(n, 10, 64)
		if err != nil {
			continue
		}
		i = j
	}
	// 下一个序列号
	return filepath.Join(dir, fmt.Sprintf("%0.6d", i+1)), int(i + 1), nil
}

func sequenceFiles(dir string) ([]string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var res []string

	for _, fi := range files {
		if _, err := strconv.ParseUint(fi.Name(), 10, 64); err != nil {
			continue
		}
		res = append(res, filepath.Join(dir, fi.Name()))
	}
	return res, nil
}

func closeAll(cs []io.Closer) (err error) {
	for _, c := range cs {
		if e := c.Close(); e != nil {
			err = e
		}
	}
	return err
}
