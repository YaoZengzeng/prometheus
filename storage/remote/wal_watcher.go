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

package remote

import (
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/fileutil"
	"github.com/prometheus/tsdb/wal"

	"github.com/prometheus/prometheus/pkg/timestamp"
)

const (
	readPeriod         = 10 * time.Millisecond
	checkpointPeriod   = 5 * time.Second
	segmentCheckPeriod = 100 * time.Millisecond
)

var (
	watcherRecordsRead = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "prometheus",
			Subsystem: "wal_watcher",
			Name:      "records_read_total",
			Help:      "Number of records read by the WAL watcher from the WAL.",
		},
		[]string{queue, "type"},
	)
	watcherRecordDecodeFails = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "prometheus",
			Subsystem: "wal_watcher",
			Name:      "record_decode_failures_total",
			Help:      "Number of records read by the WAL watcher that resulted in an error when decoding.",
		},
		[]string{queue},
	)
	watcherSamplesSentPreTailing = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "prometheus",
			Subsystem: "wal_watcher",
			Name:      "samples_sent_pre_tailing_total",
			Help:      "Number of sample records read by the WAL watcher and sent to remote write during replay of existing WAL.",
		},
		[]string{queue},
	)
	watcherCurrentSegment = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "prometheus",
			Subsystem: "wal_watcher",
			Name:      "current_segment",
			Help:      "Current segment the WAL watcher is reading records from.",
		},
		[]string{queue},
	)
)

func init() {
	prometheus.MustRegister(watcherRecordsRead)
	prometheus.MustRegister(watcherRecordDecodeFails)
	prometheus.MustRegister(watcherSamplesSentPreTailing)
	prometheus.MustRegister(watcherCurrentSegment)
}

// writeTo接口实现了Append，StoreSeries以及SeriesReset方法
type writeTo interface {
	Append([]tsdb.RefSample) bool
	StoreSeries([]tsdb.RefSeries, int)
	SeriesReset(int)
}

// WALWatcher watches the TSDB WAL for a given WriteTo.
// WALWatcher为给定的WriteTo监听TSDB WAL
type WALWatcher struct {
	name           string
	writer         writeTo
	logger         log.Logger
	walDir         string
	lastCheckpoint string

	startTime int64

	recordsReadMetric       *prometheus.CounterVec
	recordDecodeFailsMetric prometheus.Counter
	samplesSentPreTailing   prometheus.Counter
	currentSegmentMetric    prometheus.Gauge

	quit chan struct{}
	done chan struct{}

	// For testing, stop when we hit this segment.
	maxSegment int
}

// NewWALWatcher creates a new WAL watcher for a given WriteTo.
// NewWALWatcher为给定的WriteTo创建一个新的WAL watcher
func NewWALWatcher(logger log.Logger, name string, writer writeTo, walDir string) *WALWatcher {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	return &WALWatcher{
		logger: logger,
		writer: writer,
		walDir: path.Join(walDir, "wal"),
		name:   name,
		quit:   make(chan struct{}),
		done:   make(chan struct{}),

		recordsReadMetric:       watcherRecordsRead.MustCurryWith(prometheus.Labels{queue: name}),
		recordDecodeFailsMetric: watcherRecordDecodeFails.WithLabelValues(name),
		samplesSentPreTailing:   watcherSamplesSentPreTailing.WithLabelValues(name),
		currentSegmentMetric:    watcherCurrentSegment.WithLabelValues(name),

		maxSegment: -1,
	}
}

// Start the WALWatcher.
func (w *WALWatcher) Start() {
	level.Info(w.logger).Log("msg", "starting WAL watcher", "queue", w.name)
	go w.loop()
}

// Stop the WALWatcher.
func (w *WALWatcher) Stop() {
	close(w.quit)
	<-w.done
	level.Info(w.logger).Log("msg", "WAL watcher stopped", "queue", w.name)
}

func (w *WALWatcher) loop() {
	defer close(w.done)

	// We may encourter failures processing the WAL; we should wait and retry.
	// 我们可能会遇到处理WAL失败的情况，我们应该等待并且重试
	for !isClosed(w.quit) {
		w.startTime = timestamp.FromTime(time.Now())
		if err := w.run(); err != nil {
			// 在tailing的时候发生错误
			level.Error(w.logger).Log("msg", "error tailing WAL", "err", err)
		}

		select {
		case <-w.quit:
			return
		// 5s之后进行重试
		case <-time.After(5 * time.Second):
		}
	}
}

func (w *WALWatcher) run() error {
	// 找到最后一个segment
	_, lastSegment, err := w.firstAndLast()
	if err != nil {
		return errors.Wrap(err, "wal.Segments")
	}

	// Backfill from the checkpoint first if it exists.
	// 如果存在的话，首先获取checkpoint中的内容
	lastCheckpoint, checkpointIndex, err := tsdb.LastCheckpoint(w.walDir)
	if err != nil && err != tsdb.ErrNotFound {
		return errors.Wrap(err, "tsdb.LastCheckpoint")
	}

	if err == nil {
		if err = w.readCheckpoint(lastCheckpoint); err != nil {
			return errors.Wrap(err, "readCheckpoint")
		}
	}
	w.lastCheckpoint = lastCheckpoint

	// 当前正在读取的Segment
	currentSegment, err := w.findSegmentForIndex(checkpointIndex)
	if err != nil {
		return err
	}

	level.Debug(w.logger).Log("msg", "tailing WAL", "lastCheckpoint", lastCheckpoint, "checkpointIndex", checkpointIndex, "currentSegment", currentSegment, "lastSegment", lastSegment)
	for !isClosed(w.quit) {
		// 在metric设置当前正在读取的Segment
		w.currentSegmentMetric.Set(float64(currentSegment))
		level.Debug(w.logger).Log("msg", "processing segment", "currentSegment", currentSegment)

		// On start, after reading the existing WAL for series records, we have a pointer to what is the latest segment.
		// On subsequent calls to this function, currentSegment will have been incremented and we should open that segment.
		// 在启动的时候，在读取了已经存在的WAL的series records之后，我们有一个pointer指向最后一个segment
		// 之后再调用这个函数的时候，currentSegment会被增加并且我们应该打开那个segment
		if err := w.watch(currentSegment, currentSegment >= lastSegment); err != nil {
			return err
		}

		// For testing: stop when you hit a specific segment.
		if currentSegment == w.maxSegment {
			return nil
		}

		// 不断更新currentSegment
		currentSegment++
	}

	return nil
}

// findSegmentForIndex finds the first segment greater than or equal to index.
// findSegmentForIndex找到大于等于index的第一个segment
func (w *WALWatcher) findSegmentForIndex(index int) (int, error) {
	refs, err := w.segments(w.walDir)
	if err != nil {
		return -1, nil
	}

	for _, r := range refs {
		if r >= index {
			return r, nil
		}
	}

	return -1, errors.New("failed to find segment for index")
}

func (w *WALWatcher) firstAndLast() (int, int, error) {
	refs, err := w.segments(w.walDir)
	if err != nil {
		return -1, -1, nil
	}

	if len(refs) == 0 {
		return -1, -1, nil
	}
	return refs[0], refs[len(refs)-1], nil
}

// Copied from tsdb/wal/wal.go so we do not have to open a WAL.
// Plan is to move WAL watcher to TSDB and dedupe these implementations.
func (w *WALWatcher) segments(dir string) ([]int, error) {
	files, err := fileutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var refs []int
	var last int
	for _, fn := range files {
		// 将文件名转换为数字
		k, err := strconv.Atoi(fn)
		if err != nil {
			continue
		}
		if len(refs) > 0 && k > last+1 {
			return nil, errors.New("segments are not sequential")
		}
		refs = append(refs, k)
		last = k
	}
	// 从小到大对refs进行排序
	sort.Ints(refs)

	return refs, nil
}

// Use tail true to indicate that the reader is currently on a segment that is
// actively being written to. If false, assume it's a full segment and we're
// replaying it on start to cache the series records.
// 当tail为true表明reader当前处于正在活跃写入的segment
// 如果为false，则这是一个full segment并且我们在从头重放它用于缓存series records
func (w *WALWatcher) watch(segmentNum int, tail bool) error {
	segment, err := wal.OpenReadSegment(wal.SegmentName(w.walDir, segmentNum))
	if err != nil {
		return err
	}
	defer segment.Close()

	reader := wal.NewLiveReader(w.logger, segment)

	// readPeriod为10ms
	readTicker := time.NewTicker(readPeriod)
	defer readTicker.Stop()

	// checkpointPeriod为5s
	checkpointTicker := time.NewTicker(checkpointPeriod)
	defer checkpointTicker.Stop()

	// segmentCheckPeriod为100ms
	segmentTicker := time.NewTicker(segmentCheckPeriod)
	defer segmentTicker.Stop()

	// If we're replaying the segment we need to know the size of the file to know
	// when to return from watch and move on to the next segment.
	// 如果我们重放segment，我们需要知道文件的大小从而知道何时从watch返回并且转到下一个segment
	size := int64(math.MaxInt64)
	if !tail {
		segmentTicker.Stop()
		checkpointTicker.Stop()
		var err error
		size, err = getSegmentSize(w.walDir, segmentNum)
		if err != nil {
			return errors.Wrap(err, "getSegmentSize")
		}
	}

	for {
		select {
		case <-w.quit:
			return nil

		case <-checkpointTicker.C:
			// Periodically check if there is a new checkpoint so we can garbage
			// collect labels. As this is considered an optimisation, we ignore
			// errors during checkpoint processing.
			// 阶段性地检查是否有一个新的checkpoint，因此我们可以gc labels
			// 因为这被认为是一个优化，我们忽略在checkpoint期间的错误
			if err := w.garbageCollectSeries(segmentNum); err != nil {
				level.Warn(w.logger).Log("msg", "error process checkpoint", "err", err)
			}

		case <-segmentTicker.C:
			_, last, err := w.firstAndLast()
			if err != nil {
				return errors.Wrap(err, "segments")
			}

			// Check if new segments exists.
			// 检查是否有新的segments存在
			if last <= segmentNum {
				continue
			}

			// last大于segmentNum了，表示segmentNum不是最新的了，因此可以一次性读完
			err = w.readSegment(reader, segmentNum, tail)

			// Ignore errors reading to end of segment whilst replaying the WAL.
			// 重放WAL则直接返回错误
			if !tail {
				if err != nil && err != io.EOF {
					level.Warn(w.logger).Log("msg", "ignoring error reading to end of segment, may have dropped data", "err", err)
				} else if reader.Offset() != size {
					level.Warn(w.logger).Log("msg", "expected to have read whole segment, may have dropped data", "segment", segmentNum, "read", reader.Offset(), "size", size)
				}
				// 不是tail就直接返回
				return nil
			}

			// Otherwise, when we are tailing, non-EOFs are fatal.
			if err != io.EOF {
				return err
			}

			return nil

		case <-readTicker.C:
			err = w.readSegment(reader, segmentNum, tail)

			// Ignore all errors reading to end of segment whilst replaying the WAL.
			// 当前replaying WAL时读到segment的最后时，忽略所有的错误
			if !tail {
				if err != nil && err != io.EOF {
					level.Warn(w.logger).Log("msg", "ignoring error reading to end of segment, may have dropped data", "segment", segmentNum, "err", err)
				} else if reader.Offset() != size {
					level.Warn(w.logger).Log("msg", "expected to have read whole segment, may have dropped data", "segment", segmentNum, "read", reader.Offset(), "size", size)
				}
				return nil
			}

			// Otherwise, when we are tailing, non-EOFs are fatal.
			// 否则，如果不是tailing，那么非EOF的错误都是fatal
			if err != io.EOF {
				return err
			}
		}
	}
}

func (w *WALWatcher) garbageCollectSeries(segmentNum int) error {
	dir, _, err := tsdb.LastCheckpoint(w.walDir)
	if err != nil && err != tsdb.ErrNotFound {
		return errors.Wrap(err, "tsdb.LastCheckpoint")
	}

	// 如果dir为空或者等于lastCheckpoint，则直接返回
	if dir == "" || dir == w.lastCheckpoint {
		return nil
	}
	w.lastCheckpoint = dir

	index, err := checkpointNum(dir)
	if err != nil {
		return errors.Wrap(err, "error parsing checkpoint filename")
	}

	if index >= segmentNum {
		// 当前的segment已经在checkpoint之后了，跳过读取checkpoint
		level.Debug(w.logger).Log("msg", "current segment is behind the checkpoint, skipping reading of checkpoint", "current", fmt.Sprintf("%08d", segmentNum), "checkpoint", dir)
		return nil
	}

	// 检测到新的checkpoint
	level.Debug(w.logger).Log("msg", "new checkpoint detected", "new", dir, "currentSegment", segmentNum)

	if err = w.readCheckpoint(dir); err != nil {
		return errors.Wrap(err, "readCheckpoint")
	}

	// Clear series with a checkpoint or segment index # lower than the checkpoint we just read.
	// 用一个checkpoint或者segment index清楚series
	w.writer.SeriesReset(index)
	return nil
}

// readSegment遍历Segment中的record，写入w.writer中
func (w *WALWatcher) readSegment(r *wal.LiveReader, segmentNum int, tail bool) error {
	var (
		dec     tsdb.RecordDecoder
		series  []tsdb.RefSeries
		samples []tsdb.RefSample
	)

	for r.Next() && !isClosed(w.quit) {
		rec := r.Record()
		w.recordsReadMetric.WithLabelValues(recordType(dec.Type(rec))).Inc()

		switch dec.Type(rec) {
		case tsdb.RecordSeries:
			series, err := dec.Series(rec, series[:0])
			if err != nil {
				w.recordDecodeFailsMetric.Inc()
				return err
			}
			// 从reader中读取series，写入writer，也就是remote write queue
			w.writer.StoreSeries(series, segmentNum)

		case tsdb.RecordSamples:
			// If we're not tailing a segment we can ignore any samples records we see.
			// This speeds up replay of the WAL by > 10x.
			// 如果我们不是在tailing一个segment，我们可以忽略任何我们看到的samples records
			// 这会加速WAL的重放到十倍以上
			if !tail {
				break
			}
			samples, err := dec.Samples(rec, samples[:0])
			if err != nil {
				w.recordDecodeFailsMetric.Inc()
				return err
			}
			var send []tsdb.RefSample
			for _, s := range samples {
				if s.T > w.startTime {
					send = append(send, s)
				}
			}
			if len(send) > 0 {
				// Blocks  until the sample is sent to all remote write endpoints or closed (because enqueue blocks).
				// 阻塞直到sample被发送给了所有的remote write endpoints或者关闭
				w.writer.Append(send)
			}

		case tsdb.RecordTombstones:
			// noop
		case tsdb.RecordInvalid:
			return errors.New("invalid record")

		default:
			w.recordDecodeFailsMetric.Inc()
			return errors.New("unknown TSDB record type")
		}
	}
	return r.Err()
}

func recordType(rt tsdb.RecordType) string {
	switch rt {
	case tsdb.RecordInvalid:
		return "invalid"
	case tsdb.RecordSeries:
		return "series"
	case tsdb.RecordSamples:
		return "samples"
	case tsdb.RecordTombstones:
		return "tombstones"
	default:
		return "unknown"
	}
}

// Read all the series records from a Checkpoint directory.
// 从Checkpoint directory中读取所有的series records
func (w *WALWatcher) readCheckpoint(checkpointDir string) error {
	level.Debug(w.logger).Log("msg", "reading checkpoint", "dir", checkpointDir)
	index, err := checkpointNum(checkpointDir)
	if err != nil {
		return errors.Wrap(err, "checkpointNum")
	}

	// Ensure we read the whole contents of every segment in the checkpoint dir.
	// 确保我们读取了checkpoint dir中每个segment的内容
	segs, err := w.segments(checkpointDir)
	if err != nil {
		return errors.Wrap(err, "Unable to get segments checkpoint dir")
	}
	// 遍历checkpoint中的各个segments
	for _, seg := range segs {
		size, err := getSegmentSize(checkpointDir, seg)
		if err != nil {
			return errors.Wrap(err, "getSegmentSize")
		}

		sr, err := wal.OpenReadSegment(wal.SegmentName(checkpointDir, seg))
		if err != nil {
			return errors.Wrap(err, "unable to open segment")
		}
		defer sr.Close()

		r := wal.NewLiveReader(w.logger, sr)
		if err := w.readSegment(r, index, false); err != io.EOF && err != nil {
			return errors.Wrap(err, "readSegment")
		}

		if r.Offset() != size {
			return fmt.Errorf("readCheckpoint wasn't able to read all data from the checkpoint %s/%08d, size: %d, totalRead: %d", checkpointDir, seg, size, r.Offset())
		}
	}

	level.Debug(w.logger).Log("msg", "read series references from checkpoint", "checkpoint", checkpointDir)
	return nil
}

func checkpointNum(dir string) (int, error) {
	// Checkpoint dir names are in the format checkpoint.000001
	chunks := strings.Split(dir, ".")
	if len(chunks) != 2 {
		return 0, errors.Errorf("invalid checkpoint dir string: %s", dir)
	}

	result, err := strconv.Atoi(chunks[1])
	if err != nil {
		return 0, errors.Errorf("invalid checkpoint dir string: %s", dir)
	}

	return result, nil
}

// Get size of segment.
func getSegmentSize(dir string, index int) (int64, error) {
	i := int64(-1)
	fi, err := os.Stat(wal.SegmentName(dir, index))
	if err == nil {
		i = fi.Size()
	}
	return i, err
}

func isClosed(c chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}
