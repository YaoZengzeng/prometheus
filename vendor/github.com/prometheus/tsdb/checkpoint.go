// Copyright 2018 The Prometheus Authors

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

package tsdb

import (
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	tsdb_errors "github.com/prometheus/tsdb/errors"
	"github.com/prometheus/tsdb/fileutil"
	"github.com/prometheus/tsdb/wal"
)

// CheckpointStats returns stats about a created checkpoint.
type CheckpointStats struct {
	DroppedSeries     int
	DroppedSamples    int
	DroppedTombstones int
	TotalSeries       int // Processed series including dropped ones.
	TotalSamples      int // Processed samples including dropped ones.
	TotalTombstones   int // Processed tombstones including dropped ones.
}

// LastCheckpoint returns the directory name and index of the most recent checkpoint.
// If dir does not contain any checkpoints, ErrNotFound is returned.
// LastCheckpoint返回目录名以及最近的checkpoint的index，如果dir不包含任何checkpoints，返回ErrNotFound
func LastCheckpoint(dir string) (string, int, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return "", 0, err
	}
	// Traverse list backwards since there may be multiple checkpoints left.
	for i := len(files) - 1; i >= 0; i-- {
		fi := files[i]

		if !strings.HasPrefix(fi.Name(), checkpointPrefix) {
			continue
		}
		if !fi.IsDir() {
			return "", 0, errors.Errorf("checkpoint %s is not a directory", fi.Name())
		}
		idx, err := strconv.Atoi(fi.Name()[len(checkpointPrefix):])
		if err != nil {
			continue
		}
		return filepath.Join(dir, fi.Name()), idx, nil
	}
	return "", 0, ErrNotFound
}

// DeleteCheckpoints deletes all checkpoints in a directory below a given index.
// DeleteCheckpoints删除所有给定的index之前的checkpoints
func DeleteCheckpoints(dir string, maxIndex int) error {
	var errs tsdb_errors.MultiError

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, fi := range files {
		if !strings.HasPrefix(fi.Name(), checkpointPrefix) {
			continue
		}
		index, err := strconv.Atoi(fi.Name()[len(checkpointPrefix):])
		if err != nil || index >= maxIndex {
			continue
		}
		// 移除合法的，老的checkpoint目录
		if err := os.RemoveAll(filepath.Join(dir, fi.Name())); err != nil {
			errs.Add(err)
		}
	}
	return errs.Err()
}

const checkpointPrefix = "checkpoint."

// Checkpoint creates a compacted checkpoint of segments in range [first, last] in the given WAL.
// Checkpoint创建一个压缩的checkpoint在first到last范围内的segments，在给定的WAL中
// It includes the most recent checkpoint if it exists.
// 它包含最近的checkpoint，如果存在的话
// All series not satisfying keep and samples below mint are dropped.
// 所有不满足keep的series以及小于mint的samples都会被丢弃
//
// The checkpoint is stored in a directory named checkpoint.N in the same
// segmented format as the original WAL itself.
// checkpoint存储在一个名字为checkpoint.N的目录中，segment的格式和之前在WAL中的相同
// This makes it easy to read it through the WAL package and concatenate
// it with the original WAL.
// 这能够让从WAL中读取并且和原始的WAL连接来得更方便
//
// 所谓的checkpoint就是在wal中创建一个名字为checkpoint.N的子目录，然后生成一个wal对象，从wal目录
// 读取内容到checkpoint目录，生成一个子wal，并且将原wal中的一部分segment写入子wal中
// 做checkpoint的时候也要读wal再写wal
func Checkpoint(w *wal.WAL, from, to int, keep func(id uint64) bool, mint int64) (*CheckpointStats, error) {
	stats := &CheckpointStats{}
	var sgmReader io.ReadCloser

	{

		var sgmRange []wal.SegmentRange
		dir, idx, err := LastCheckpoint(w.Dir())
		if err != nil && err != ErrNotFound {
			return nil, errors.Wrap(err, "find last checkpoint")
		}
		last := idx + 1
		if err == nil {
			// err为nil，说明存在checkpoint
			if from > last {
				// 应该从当前checkpoint的下一个segment开始checkpoint
				return nil, fmt.Errorf("unexpected gap to last checkpoint. expected:%v, requested:%v", last, from)
			}
			// Ignore WAL files below the checkpoint. They shouldn't exist to begin with.
			// 忽略掉checkpoint之前的WAL，不应该从它们开始
			from = last

			// 这个sgmRange表示读取老的checkpoint中的内容
			sgmRange = append(sgmRange, wal.SegmentRange{Dir: dir, Last: math.MaxInt32})
		}

		sgmRange = append(sgmRange, wal.SegmentRange{Dir: w.Dir(), First: from, Last: to})
		// 根据sgmRange构建sgmReder
		sgmReader, err = wal.NewSegmentsRangeReader(sgmRange...)
		if err != nil {
			return nil, errors.Wrap(err, "create segment reader")
		}
		defer sgmReader.Close()
	}

	cpdir := filepath.Join(w.Dir(), fmt.Sprintf(checkpointPrefix+"%06d", to))
	cpdirtmp := cpdir + ".tmp"

	// 创建checkpoint的临时目录
	if err := os.MkdirAll(cpdirtmp, 0777); err != nil {
		return nil, errors.Wrap(err, "create checkpoint dir")
	}
	// New返回一个新的wal实例
	cp, err := wal.New(nil, nil, cpdirtmp, w.CompressionEnabled())
	if err != nil {
		return nil, errors.Wrap(err, "open checkpoint")
	}

	// Ensures that an early return caused by an error doesn't leave any tmp files.
	// 确认因为错误导致的提前返回不会留下任何tmp文件
	defer func() {
		cp.Close()
		os.RemoveAll(cpdirtmp)
	}()

	r := wal.NewReader(sgmReader)

	var (
		series  []RefSeries
		samples []RefSample
		tstones []Stone
		dec     RecordDecoder
		enc     RecordEncoder
		buf     []byte
		recs    [][]byte
	)
	for r.Next() {
		series, samples, tstones = series[:0], samples[:0], tstones[:0]

		// We don't reset the buffer since we batch up multiple records
		// before writing them to the checkpoint.
		// 我们不重置buffer，因为我们在将它们写入checkpoint以前会合并多个records
		// Remember where the record for this iteration starts.
		start := len(buf)
		rec := r.Record()

		switch dec.Type(rec) {
		case RecordSeries:
			// 对series进行解码
			series, err = dec.Series(rec, series)
			if err != nil {
				return nil, errors.Wrap(err, "decode series")
			}
			// Drop irrelevant series in place.
			// 丢弃不相关的series
			repl := series[:0]
			for _, s := range series {
				if keep(s.Ref) {
					// repl是能够保留下来的series
					repl = append(repl, s)
				}
			}
			if len(repl) > 0 {
				// 又对series进行
				buf = enc.Series(repl, buf)
			}
			stats.TotalSeries += len(series)
			stats.DroppedSeries += len(series) - len(repl)

		case RecordSamples:
			samples, err = dec.Samples(rec, samples)
			if err != nil {
				return nil, errors.Wrap(err, "decode samples")
			}
			// Drop irrelevant samples in place.
			// 丢弃不相关的samples
			repl := samples[:0]
			for _, s := range samples {
				if s.T >= mint {
					repl = append(repl, s)
				}
			}
			if len(repl) > 0 {
				buf = enc.Samples(repl, buf)
			}
			stats.TotalSamples += len(samples)
			stats.DroppedSamples += len(samples) - len(repl)

		case RecordTombstones:
			tstones, err = dec.Tombstones(rec, tstones)
			if err != nil {
				return nil, errors.Wrap(err, "decode deletes")
			}
			// Drop irrelevant tombstones in place.
			// 丢弃不相关的tombstones
			repl := tstones[:0]
			for _, s := range tstones {
				for _, iv := range s.intervals {
					if iv.Maxt >= mint {
						repl = append(repl, s)
						break
					}
				}
			}
			if len(repl) > 0 {
				buf = enc.Tombstones(repl, buf)
			}
			stats.TotalTombstones += len(tstones)
			stats.DroppedTombstones += len(tstones) - len(repl)

		default:
			return nil, errors.New("invalid record type")
		}
		if len(buf[start:]) == 0 {
			// 所有的record都被遗弃了，则直接跳过
			continue // All contents discarded.
		}
		recs = append(recs, buf[start:])

		// Flush records in 1 MB increments.
		// 当records的内容大于1MB，则进行flush
		if len(buf) > 1*1024*1024 {
			if err := cp.Log(recs...); err != nil {
				return nil, errors.Wrap(err, "flush records")
			}
			buf, recs = buf[:0], recs[:0]
		}
	}
	// If we hit any corruption during checkpointing, repairing is not an option.
	// The head won't know which series records are lost.
	// 如果在checkpoint的过程中遇到了corruption，恢复并不是一个选项
	// head不知道哪个series records丢弃了
	if r.Err() != nil {
		return nil, errors.Wrap(r.Err(), "read segments")
	}

	// Flush remaining records.
	// Flush剩余的records
	if err := cp.Log(recs...); err != nil {
		return nil, errors.Wrap(err, "flush records")
	}
	if err := cp.Close(); err != nil {
		return nil, errors.Wrap(err, "close checkpoint")
	}
	if err := fileutil.Replace(cpdirtmp, cpdir); err != nil {
		return nil, errors.Wrap(err, "rename checkpoint directory")
	}

	return stats, nil
}
