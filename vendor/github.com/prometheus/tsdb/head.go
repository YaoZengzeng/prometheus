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

package tsdb

import (
	"fmt"
	"math"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb/chunkenc"
	"github.com/prometheus/tsdb/chunks"
	"github.com/prometheus/tsdb/encoding"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
	"github.com/prometheus/tsdb/wal"
)

var (
	// ErrNotFound is returned if a looked up resource was not found.
	ErrNotFound = errors.Errorf("not found")

	// ErrOutOfOrderSample is returned if an appended sample has a
	// timestamp smaller than the most recent sample.
	ErrOutOfOrderSample = errors.New("out of order sample")

	// ErrAmendSample is returned if an appended sample has the same timestamp
	// as the most recent sample but a different value.
	ErrAmendSample = errors.New("amending sample")

	// ErrOutOfBounds is returned if an appended sample is out of the
	// writable time range.
	ErrOutOfBounds = errors.New("out of bounds")

	// emptyTombstoneReader is a no-op Tombstone Reader.
	// This is used by head to satisfy the Tombstones() function call.
	// emptyTombstoneReader是一个不做任何操作的Tombstone Reader
	// 它仅仅被head使用用于满足Tombstones()的函数调用要求
	emptyTombstoneReader = newMemTombstones()
)

// Head handles reads and writes of time series data within a time window.
// Head处理对于一个时间窗口内的时间序列的读写问题
type Head struct {
	chunkRange int64
	metrics    *headMetrics
	wal        *wal.WAL
	logger     log.Logger
	appendPool sync.Pool
	bytesPool  sync.Pool

	// 当前head中最小的和最大的samples
	minTime, maxTime int64 // Current min and max of the samples included in the head.
	minValidTime     int64 // Mint allowed to be added to the head. It shouldn't be lower than the maxt of the last persisted block.
	// 最新的series id
	lastSeriesID     uint64

	// All series addressable by their ID or hash.
	// 所有通过它们的ID或者哈希进行寻址的series
	series *stripeSeries

	symMtx  sync.RWMutex
	// symbols包含了所有label的key和value
	symbols map[string]struct{}
	// 所有label names到可能的values的映射
	values  map[string]stringset // label names to possible values

	deletedMtx sync.Mutex
	// 被删除的series以及它们必须保持到对应的WAL segment被删除以后
	deleted    map[uint64]int // Deleted series, and what WAL segment they must be kept until.

	postings *index.MemPostings // postings lists for terms
}

type headMetrics struct {
	activeAppenders         prometheus.Gauge
	series                  prometheus.Gauge
	seriesCreated           prometheus.Counter
	seriesRemoved           prometheus.Counter
	seriesNotFound          prometheus.Counter
	chunks                  prometheus.Gauge
	chunksCreated           prometheus.Counter
	chunksRemoved           prometheus.Counter
	gcDuration              prometheus.Summary
	minTime                 prometheus.GaugeFunc
	maxTime                 prometheus.GaugeFunc
	samplesAppended         prometheus.Counter
	walTruncateDuration     prometheus.Summary
	walCorruptionsTotal     prometheus.Counter
	headTruncateFail        prometheus.Counter
	headTruncateTotal       prometheus.Counter
	checkpointDeleteFail    prometheus.Counter
	checkpointDeleteTotal   prometheus.Counter
	checkpointCreationFail  prometheus.Counter
	checkpointCreationTotal prometheus.Counter
}

func newHeadMetrics(h *Head, r prometheus.Registerer) *headMetrics {
	m := &headMetrics{}

	m.activeAppenders = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_head_active_appenders",
		Help: "Number of currently active appender transactions",
	})
	m.series = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_head_series",
		Help: "Total number of series in the head block.",
	})
	m.seriesCreated = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_head_series_created_total",
		Help: "Total number of series created in the head",
	})
	m.seriesRemoved = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_head_series_removed_total",
		Help: "Total number of series removed in the head",
	})
	m.seriesNotFound = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_head_series_not_found_total",
		Help: "Total number of requests for series that were not found.",
	})
	m.chunks = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_head_chunks",
		Help: "Total number of chunks in the head block.",
	})
	m.chunksCreated = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_head_chunks_created_total",
		Help: "Total number of chunks created in the head",
	})
	m.chunksRemoved = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_head_chunks_removed_total",
		Help: "Total number of chunks removed in the head",
	})
	m.gcDuration = prometheus.NewSummary(prometheus.SummaryOpts{
		Name:       "prometheus_tsdb_head_gc_duration_seconds",
		Help:       "Runtime of garbage collection in the head block.",
		Objectives: map[float64]float64{},
	})
	m.maxTime = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_head_max_time",
		Help: "Maximum timestamp of the head block. The unit is decided by the library consumer.",
	}, func() float64 {
		return float64(h.MaxTime())
	})
	m.minTime = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_head_min_time",
		Help: "Minimum time bound of the head block. The unit is decided by the library consumer.",
	}, func() float64 {
		return float64(h.MinTime())
	})
	m.walTruncateDuration = prometheus.NewSummary(prometheus.SummaryOpts{
		Name:       "prometheus_tsdb_wal_truncate_duration_seconds",
		Help:       "Duration of WAL truncation.",
		Objectives: map[float64]float64{},
	})
	m.walCorruptionsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_wal_corruptions_total",
		Help: "Total number of WAL corruptions.",
	})
	m.samplesAppended = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_head_samples_appended_total",
		Help: "Total number of appended samples.",
	})
	m.headTruncateFail = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_head_truncations_failed_total",
		Help: "Total number of head truncations that failed.",
	})
	m.headTruncateTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_head_truncations_total",
		Help: "Total number of head truncations attempted.",
	})
	m.checkpointDeleteFail = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_checkpoint_deletions_failed_total",
		Help: "Total number of checkpoint deletions that failed.",
	})
	m.checkpointDeleteTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_checkpoint_deletions_total",
		Help: "Total number of checkpoint deletions attempted.",
	})
	m.checkpointCreationFail = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_checkpoint_creations_failed_total",
		Help: "Total number of checkpoint creations that failed.",
	})
	m.checkpointCreationTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_checkpoint_creations_total",
		Help: "Total number of checkpoint creations attempted.",
	})

	if r != nil {
		r.MustRegister(
			m.activeAppenders,
			m.chunks,
			m.chunksCreated,
			m.chunksRemoved,
			m.series,
			m.seriesCreated,
			m.seriesRemoved,
			m.seriesNotFound,
			m.minTime,
			m.maxTime,
			m.gcDuration,
			m.walTruncateDuration,
			m.walCorruptionsTotal,
			m.samplesAppended,
			m.headTruncateFail,
			m.headTruncateTotal,
			m.checkpointDeleteFail,
			m.checkpointDeleteTotal,
			m.checkpointCreationFail,
			m.checkpointCreationTotal,
		)
	}
	return m
}

// NewHead opens the head block in dir.
// NewHead在wal中打开head block
func NewHead(r prometheus.Registerer, l log.Logger, wal *wal.WAL, chunkRange int64) (*Head, error) {
	if l == nil {
		l = log.NewNopLogger()
	}
	if chunkRange < 1 {
		return nil, errors.Errorf("invalid chunk range %d", chunkRange)
	}
	h := &Head{
		wal:        wal,
		logger:     l,
		chunkRange: chunkRange,
		minTime:    math.MaxInt64,
		maxTime:    math.MinInt64,
		series:     newStripeSeries(),
		values:     map[string]stringset{},
		symbols:    map[string]struct{}{},
		postings:   index.NewUnorderedMemPostings(),
		deleted:    map[uint64]int{},
	}
	h.metrics = newHeadMetrics(h, r)

	return h, nil
}

// processWALSamples adds a partition of samples it receives to the head and passes
// them on to other workers.
// Samples before the mint timestamp are discarded.
// processWALSamples将它收到的一部分samples加入head并且将它们传递给其他worker
func (h *Head) processWALSamples(
	minValidTime int64,
	input <-chan []RefSample, output chan<- []RefSample,
) (unknownRefs uint64) {
	defer close(output)

	// Mitigate lock contention in getByID.
	refSeries := map[uint64]*memSeries{}

	mint, maxt := int64(math.MaxInt64), int64(math.MinInt64)

	for samples := range input {
		for _, s := range samples {
			if s.T < minValidTime {
				// 对于不合法的samples，直接跳过
				continue
			}
			// 获取对应的memSeries
			ms := refSeries[s.Ref]
			if ms == nil {
				ms = h.series.getByID(s.Ref)
				if ms == nil {
					// 如果ref找不到，则unknownRefs++
					unknownRefs++
					continue
				}
				refSeries[s.Ref] = ms
			}
			// 将sample加入memSeries
			_, chunkCreated := ms.append(s.T, s.V)
			if chunkCreated {
				// 如果创建了chunk，则更新metrics
				h.metrics.chunksCreated.Inc()
				h.metrics.chunks.Inc()
			}
			// 更新maxt和mint
			if s.T > maxt {
				maxt = s.T
			}
			if s.T < mint {
				mint = s.T
			}
		}
		// 再将用完的samples还给output
		output <- samples
	}
	// 更新mint和maxt
	h.updateMinMaxTime(mint, maxt)

	return unknownRefs
}

func (h *Head) updateMinMaxTime(mint, maxt int64) {
	// 持续更新，直到失效或者更新成功为止
	for {
		lt := h.MinTime()
		if mint >= lt {
			break
		}
		if atomic.CompareAndSwapInt64(&h.minTime, lt, mint) {
			break
		}
	}
	for {
		ht := h.MaxTime()
		if maxt <= ht {
			break
		}
		if atomic.CompareAndSwapInt64(&h.maxTime, ht, maxt) {
			break
		}
	}
}

func (h *Head) loadWAL(r *wal.Reader, multiRef map[uint64]uint64) (err error) {
	// Track number of samples that referenced a series we don't know about
	// for error reporting.
	// unknownRefs用于追踪我们不知道引用的series的samples的数目，用于错误报告
	var unknownRefs uint64

	// Start workers that each process samples for a partition of the series ID space.
	// They are connected through a ring of channels which ensures that all sample batches
	// read from the WAL are processed in order.
	// 启动workers，每个处理来自一部分series ID空间的samples
	// 它们通过一个ring of channels相连，这保证了从WAL中读取的sample batches都是有序的
	var (
		wg           sync.WaitGroup
		multiRefLock sync.Mutex
		n            = runtime.GOMAXPROCS(0)
		inputs       = make([]chan []RefSample, n)
		outputs      = make([]chan []RefSample, n)
	)
	wg.Add(n)

	defer func() {
		// For CorruptionErr ensure to terminate all workers before exiting.
		// 对于CorruptionErr，确保在退出之前终止所有的workers
		if _, ok := err.(*wal.CorruptionErr); ok {
			for i := 0; i < n; i++ {
				close(inputs[i])
				for range outputs[i] {
				}
			}
			wg.Wait()
		}
	}()

	for i := 0; i < n; i++ {
		outputs[i] = make(chan []RefSample, 300)
		inputs[i] = make(chan []RefSample, 300)

		// 创建worker，对RefSample进行处理
		go func(input <-chan []RefSample, output chan<- []RefSample) {
			// 对wal中的sample进行处理
			unknown := h.processWALSamples(h.minValidTime, input, output)
			atomic.AddUint64(&unknownRefs, unknown)
			wg.Done()
		}(inputs[i], outputs[i])
	}

	var (
		dec       RecordDecoder
		// RefSeries只有RefID和series的labels
		series    []RefSeries
		// RefSample是一个时间戳/value对，以及对于series的引用
		samples   []RefSample
		tstones   []Stone
		allStones = newMemTombstones()
	)
	defer func() {
		if err := allStones.Close(); err != nil {
			level.Warn(h.logger).Log("msg", "closing  memTombstones during wal read", "err", err)
		}
	}()
	for r.Next() {
		// 重用series, samples以及tsones
		series, samples, tstones = series[:0], samples[:0], tstones[:0]
		// 读取一个record，一个record要么是一堆series，一堆samples以及一堆tombstone
		rec := r.Record()

		switch dec.Type(rec) {
		case RecordSeries:
			series, err = dec.Series(rec, series)
			if err != nil {
				return &wal.CorruptionErr{
					Err:     errors.Wrap(err, "decode series"),
					Segment: r.Segment(),
					Offset:  r.Offset(),
				}
			}
			for _, s := range series {
				// 将series更新到Head中
				series, created := h.getOrCreateWithID(s.Ref, s.Labels.Hash(), s.Labels)

				if !created {
					// There's already a different ref for this series.
					// 对于这个series已经有一个不一样的ref了
					multiRefLock.Lock()
					// 获得或者新建的series的ref和s.Ref不一样
					multiRef[s.Ref] = series.ref
					multiRefLock.Unlock()
				}

				if h.lastSeriesID < s.Ref {
					// 刷新lastSeriesID
					h.lastSeriesID = s.Ref
				}
			}
		case RecordSamples:
			samples, err = dec.Samples(rec, samples)
			s := samples
			if err != nil {
				return &wal.CorruptionErr{
					Err:     errors.Wrap(err, "decode samples"),
					Segment: r.Segment(),
					Offset:  r.Offset(),
				}
			}
			// We split up the samples into chunks of 5000 samples or less.
			// With O(300 * #cores) in-flight sample batches, large scrapes could otherwise
			// cause thousands of very large in flight buffers occupying large amounts
			// of unused memory.
			// 我们将samples划分为每个chunk小于等于5000个samples，大概总共有O(300 * #cores)个sample在处理
			// 否则大量的scrape可能导致大量的in flight buffers，产生大量无用的内存
			for len(samples) > 0 {
				m := 5000
				// 每次至多5000个samples
				if len(samples) < m {
					m = len(samples)
				}
				shards := make([][]RefSample, n)
				for i := 0; i < n; i++ {
					var buf []RefSample
					select {
					// worker从对应的output channel获取buf
					case buf = <-outputs[i]:
					default:
					}
					// 重用buf
					// 如果outputs来不及的话，其实还是创建新的buf
					shards[i] = buf[:0]
				}
				for _, sam := range samples[:m] {
					if r, ok := multiRef[sam.Ref]; ok {
						// 更新ref
						sam.Ref = r
					}
					// 根据ref取模，将sample划分到对应的shard
					mod := sam.Ref % uint64(n)
					shards[mod] = append(shards[mod], sam)
				}
				// 还是要等待所有的shard都进入
				for i := 0; i < n; i++ {
					inputs[i] <- shards[i]
				}
				samples = samples[m:]
			}
			// 保留slice用于后期使用
			samples = s // Keep whole slice for reuse.
		case RecordTombstones:
			tstones, err = dec.Tombstones(rec, tstones)
			if err != nil {
				return &wal.CorruptionErr{
					Err:     errors.Wrap(err, "decode tombstones"),
					Segment: r.Segment(),
					Offset:  r.Offset(),
				}
			}
			for _, s := range tstones {
				for _, itv := range s.intervals {
					if itv.Maxt < h.minValidTime {
						continue
					}
					if m := h.series.getByID(s.ref); m == nil {
						unknownRefs++
						continue
					}
					allStones.addInterval(s.ref, itv)
				}
			}
		default:
			return &wal.CorruptionErr{
				Err:     errors.Errorf("invalid record type %v", dec.Type(rec)),
				Segment: r.Segment(),
				Offset:  r.Offset(),
			}
		}
	}

	// Signal termination to each worker and wait for it to close its output channel.
	for i := 0; i < n; i++ {
		// 关闭各个worker的input并且榨干相应的output
		close(inputs[i])
		for range outputs[i] {
		}
	}
	wg.Wait()

	if r.Err() != nil {
		return errors.Wrap(r.Err(), "read records")
	}

	if err := allStones.Iter(func(ref uint64, dranges Intervals) error {
		return h.chunkRewrite(ref, dranges)
	}); err != nil {
		return errors.Wrap(r.Err(), "deleting samples from tombstones")
	}

	if unknownRefs > 0 {
		// 未知的series references的数目
		level.Warn(h.logger).Log("msg", "unknown series references", "count", unknownRefs)
	}
	return nil
}

// Init loads data from the write ahead log and prepares the head for writes.
// It should be called before using an appender so that
// limits the ingested samples to the head min valid time.
// Init从wal加载数据并且准备写head
// 它应该在使用一个appender之前被使用，这样就能限制摄入的samples在head min valid time以内
func (h *Head) Init(minValidTime int64) error {
	h.minValidTime = minValidTime
	defer h.postings.EnsureOrder()
	// 在加载了wal之后，从head中移除过时的data
	defer h.gc() // After loading the wal remove the obsolete data from the head.

	if h.wal == nil {
		return nil
	}

	// Backfill the checkpoint first if it exists.
	// 如果error是ErrNotFound，则startFrom为0
	dir, startFrom, err := LastCheckpoint(h.wal.Dir())
	if err != nil && err != ErrNotFound {
		return errors.Wrap(err, "find last checkpoint")
	}
	multiRef := map[uint64]uint64{}
	if err == nil {
		// NewSegmentsReader返回目录里所有segment的reader
		sr, err := wal.NewSegmentsReader(dir)
		if err != nil {
			return errors.Wrap(err, "open checkpoint")
		}
		defer func() {
			if err := sr.Close(); err != nil {
				level.Warn(h.logger).Log("msg", "error while closing the wal segments reader", "err", err)
			}
		}()

		// A corrupted checkpoint is a hard error for now and requires user
		// intervention. There's likely little data that can be recovered anyway.
		// 如果checkpoint被损坏了，现在是很难恢复的，需要用户介入，尽管很少有数据能够被恢复
		// 把checkpoint的内容载入head
		if err := h.loadWAL(wal.NewReader(sr), multiRef); err != nil {
			return errors.Wrap(err, "backfill checkpoint")
		}
		startFrom++
	}

	// Find the last segment.
	// 找到最后一个segment
	_, last, err := h.wal.Segments()
	if err != nil {
		return errors.Wrap(err, "finding WAL segments")
	}

	// Backfill segments from the most recent checkpoint onwards.
	// 从最近的checkpoint开始回填segments
	for i := startFrom; i <= last; i++ {
		s, err := wal.OpenReadSegment(wal.SegmentName(h.wal.Dir(), i))
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("open WAL segment: %d", i))
		}

		// 依次将各个segment载入head
		sr := wal.NewSegmentBufReader(s)
		err = h.loadWAL(wal.NewReader(sr), multiRef)
		if err := sr.Close(); err != nil {
			level.Warn(h.logger).Log("msg", "error while closing the wal segments reader", "err", err)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

// Truncate removes old data before mint from the head.
// Truncate从head中移除老于mint的数据
// 在Truncate的时候会进行checkpoint
func (h *Head) Truncate(mint int64) (err error) {
	defer func() {
		if err != nil {
			h.metrics.headTruncateFail.Inc()
		}
	}()
	initialize := h.MinTime() == math.MaxInt64

	if h.MinTime() >= mint && !initialize {
		// 如果head不是初始化且h.MinTime大于MinTime，则Head进行Truncate
		return nil
	}
	// 设置head的minTime和minValidTime为mint
	atomic.StoreInt64(&h.minTime, mint)
	atomic.StoreInt64(&h.minValidTime, mint)

	// Ensure that max time is at least as high as min time.
	// 确保head的max time和min time至少一样高
	for h.MaxTime() < mint {
		atomic.CompareAndSwapInt64(&h.maxTime, h.MaxTime(), mint)
	}

	// This was an initial call to Truncate after loading blocks on startup.
	// We haven't read back the WAL yet, so do not attempt to truncate it.
	// 这是在启动加载blocks之后第一次调用Truncate，我们还没有读取WAL，因此不要试着去截取它
	if initialize {
		// 还没有读WAL，因此也没必要truncate
		return nil
	}

	h.metrics.headTruncateTotal.Inc()
	start := time.Now()

	// 首先对head进行gc
	h.gc()
	level.Info(h.logger).Log("msg", "head GC completed", "duration", time.Since(start))
	h.metrics.gcDuration.Observe(time.Since(start).Seconds())

	// 如果没有wal，则直接返回，不进行截取
	if h.wal == nil {
		return nil
	}
	start = time.Now()

	first, last, err := h.wal.Segments()
	if err != nil {
		return errors.Wrap(err, "get segment range")
	}
	// Start a new segment, so low ingestion volume TSDB don't have more WAL than
	// needed.
	// 启动一个新的segment，因此low ingestion volume TSDB不会有着太多的WAL
	err = h.wal.NextSegment()
	if err != nil {
		return errors.Wrap(err, "next segment")
	}
	last-- // Never consider last segment for checkpoint.
	// 从不考虑最后一个segment用于checkpoint
	if last < 0 {
		return nil // no segments yet.
	}
	// The lower third of segments should contain mostly obsolete samples.
	// If we have less than three segments, it's not worth checkpointing yet.
	// 后三分之一的segments应该包含大多数被遗弃的samples
	// 如果segment的数目小于三个，则不值得进行checkpoint
	last = first + (last-first)/3
	if last <= first {
		return nil
	}

	keep := func(id uint64) bool {
		// 如果id在series中或者deleted中，则返回true
		if h.series.getByID(id) != nil {
			return true
		}
		h.deletedMtx.Lock()
		// 如果series在h.deleted中，那么也需要保留
		_, ok := h.deleted[id]
		h.deletedMtx.Unlock()
		return ok
	}
	h.metrics.checkpointCreationTotal.Inc()
	// 进行checkpoint
	if _, err = Checkpoint(h.wal, first, last, keep, mint); err != nil {
		h.metrics.checkpointCreationFail.Inc()
		return errors.Wrap(err, "create checkpoint")
	}
	// 对wal进行截取
	if err := h.wal.Truncate(last + 1); err != nil {
		// If truncating fails, we'll just try again at the next checkpoint.
		// Leftover segments will just be ignored in the future if there's a checkpoint
		// that supersedes them.
		// 如果截取失败了，我们会在下一次checkpoint的时候再次尝试
		// 剩下的segments会被忽略，如果一个checkpoint分离了它们
		level.Error(h.logger).Log("msg", "truncating segments failed", "err", err)
	}

	// The checkpoint is written and segments before it is truncated, so we no
	// longer need to track deleted series that are before it.
	// checkpoint已经被写入，它之前的segments被截取了，因此我们不再需要追踪它之前被删除掉series
	h.deletedMtx.Lock()
	for ref, segment := range h.deleted {
		// first之前的segment都可以被删除了
		if segment < first {
			delete(h.deleted, ref)
		}
	}
	h.deletedMtx.Unlock()

	h.metrics.checkpointDeleteTotal.Inc()
	if err := DeleteCheckpoints(h.wal.Dir(), last); err != nil {
		// Leftover old checkpoints do not cause problems down the line beyond
		// occupying disk space.
		// 被遗留下来的老的checkpoints不会造成任何问题，除了占据一定的磁盘空间
		// They will just be ignored since a higher checkpoint exists.
		// 它们直接会被忽略，因此更高的checkpoint存在
		level.Error(h.logger).Log("msg", "delete old checkpoints", "err", err)
		h.metrics.checkpointDeleteFail.Inc()
	}
	h.metrics.walTruncateDuration.Observe(time.Since(start).Seconds())

	level.Info(h.logger).Log("msg", "WAL checkpoint complete",
		"first", first, "last", last, "duration", time.Since(start))

	return nil
}

// initTime initializes a head with the first timestamp. This only needs to be called
// for a completely fresh head with an empty WAL.
// Returns true if the initialization took an effect.
func (h *Head) initTime(t int64) (initialized bool) {
	if !atomic.CompareAndSwapInt64(&h.minTime, math.MaxInt64, t) {
		return false
	}
	// Ensure that max time is initialized to at least the min time we just set.
	// Concurrent appenders may already have set it to a higher value.
	atomic.CompareAndSwapInt64(&h.maxTime, math.MinInt64, t)

	return true
}

type rangeHead struct {
	head       *Head
	mint, maxt int64
}

func (h *rangeHead) Index() (IndexReader, error) {
	return h.head.indexRange(h.mint, h.maxt), nil
}

func (h *rangeHead) Chunks() (ChunkReader, error) {
	return h.head.chunksRange(h.mint, h.maxt), nil
}

func (h *rangeHead) Tombstones() (TombstoneReader, error) {
	return emptyTombstoneReader, nil
}

func (h *rangeHead) MinTime() int64 {
	return h.mint
}

func (h *rangeHead) MaxTime() int64 {
	return h.maxt
}

// initAppender is a helper to initialize the time bounds of the head
// upon the first sample it receives.
// initAppender是一个helper用来初始化head的time bounds，当接收到第一个sample的时候
type initAppender struct {
	app  Appender
	head *Head
}

func (a *initAppender) Add(lset labels.Labels, t int64, v float64) (uint64, error) {
	if a.app != nil {
		return a.app.Add(lset, t, v)
	}
	a.head.initTime(t)
	a.app = a.head.appender()

	return a.app.Add(lset, t, v)
}

func (a *initAppender) AddFast(ref uint64, t int64, v float64) error {
	if a.app == nil {
		return ErrNotFound
	}
	return a.app.AddFast(ref, t, v)
}

func (a *initAppender) Commit() error {
	if a.app == nil {
		return nil
	}
	return a.app.Commit()
}

func (a *initAppender) Rollback() error {
	if a.app == nil {
		return nil
	}
	return a.app.Rollback()
}

// Appender returns a new Appender on the database.
// Appender返回数据库的一个新的Appender
func (h *Head) Appender() Appender {
	h.metrics.activeAppenders.Inc()

	// The head cache might not have a starting point yet. The init appender
	// picks up the first appended timestamp as the base.
	// head cache可能还没有一个开始点，init appender选取第一个appended timestamp作为base
	if h.MinTime() == math.MaxInt64 {
		return &initAppender{head: h}
	}
	return h.appender()
}

func (h *Head) appender() *headAppender {
	// 每次调用appender就返回一个headAppender
	return &headAppender{
		head: h,
		// Set the minimum valid time to whichever is greater the head min valid time or the compaciton window.
		// This ensures that no samples will be added within the compaction window to avoid races.
		minValidTime: max(atomic.LoadInt64(&h.minValidTime), h.MaxTime()-h.chunkRange/2),
		// appender的mint和maxt都是自己定义的
		mint:         math.MaxInt64,
		maxt:         math.MinInt64,
		// 从buffer中找出samples的缓存
		samples:      h.getAppendBuffer(),
	}
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func (h *Head) getAppendBuffer() []RefSample {
	b := h.appendPool.Get()
	if b == nil {
		return make([]RefSample, 0, 512)
	}
	return b.([]RefSample)
}

func (h *Head) putAppendBuffer(b []RefSample) {
	//lint:ignore SA6002 safe to ignore and actually fixing it has some performance penalty.
	h.appendPool.Put(b[:0])
}

func (h *Head) getBytesBuffer() []byte {
	b := h.bytesPool.Get()
	if b == nil {
		return make([]byte, 0, 1024)
	}
	return b.([]byte)
}

func (h *Head) putBytesBuffer(b []byte) {
	//lint:ignore SA6002 safe to ignore and actually fixing it has some performance penalty.
	h.bytesPool.Put(b[:0])
}

type headAppender struct {
	head         *Head
	// 时间戳小于minValidTime是不被允许的s
	minValidTime int64 // No samples below this timestamp are allowed.
	mint, maxt   int64

	series  []RefSeries
	samples []RefSample
}

func (a *headAppender) Add(lset labels.Labels, t int64, v float64) (uint64, error) {
	if t < a.minValidTime {
		return 0, ErrOutOfBounds
	}

	// Ensure no empty labels have gotten through.
	lset = lset.WithoutEmpty()

	// 对labels取哈希值
	s, created := a.head.getOrCreate(lset.Hash(), lset)
	if created {
		// 如果是新建的series，则扩展a.series
		a.series = append(a.series, RefSeries{
			Ref:    s.ref,
			Labels: lset,
		})
	}
	return s.ref, a.AddFast(s.ref, t, v)
}

func (a *headAppender) AddFast(ref uint64, t int64, v float64) error {
	if t < a.minValidTime {
		return ErrOutOfBounds
	}

	// 通过ref，即id找到相应的series
	s := a.head.series.getByID(ref)
	if s == nil {
		return errors.Wrap(ErrNotFound, "unknown series")
	}
	s.Lock()
	if err := s.appendable(t, v); err != nil {
		s.Unlock()
		return err
	}
	// 将series的pendingCommit设置为true
	s.pendingCommit = true
	s.Unlock()

	// 到此处说明sample是合法的
	if t < a.mint {
		// 更新mint
		a.mint = t
	}
	if t > a.maxt {
		// 更新maxt
		a.maxt = t
	}

	// AddFast仅仅将samples扩展a.samples，在commit的时候才提交到sereis的chunk中
	a.samples = append(a.samples, RefSample{
		Ref:    ref,
		T:      t,
		V:      v,
		// sample中也包含对series的引用
		series: s,
	})
	return nil
}

func (a *headAppender) log() error {
	if a.head.wal == nil {
		return nil
	}

	buf := a.head.getBytesBuffer()
	defer func() { a.head.putBytesBuffer(buf) }()

	var rec []byte
	var enc RecordEncoder

	if len(a.series) > 0 {
		// 对appender中的series进行编码，写入wal中
		rec = enc.Series(a.series, buf)
		buf = rec[:0]

		if err := a.head.wal.Log(rec); err != nil {
			return errors.Wrap(err, "log series")
		}
	}
	if len(a.samples) > 0 {
		// 对appender中的samples进行编码，写入wal中
		rec = enc.Samples(a.samples, buf)
		buf = rec[:0]

		if err := a.head.wal.Log(rec); err != nil {
			return errors.Wrap(err, "log samples")
		}
	}
	return nil
}

func (a *headAppender) Commit() error {
	defer a.head.metrics.activeAppenders.Dec()
	// Commit之后将samples放回append buffer中
	defer a.head.putAppendBuffer(a.samples)

	// 首先调用a.log()写入wal，写入的起始就是这次append的series和samples
	if err := a.log(); err != nil {
		return errors.Wrap(err, "write to WAL")
	}

	total := len(a.samples)

	// 遍历所有samples，把它们加入到memSeries中
	for _, s := range a.samples {
		s.series.Lock()
		// 对每个Sample都独立加锁
		ok, chunkCreated := s.series.append(s.T, s.V)
		s.series.pendingCommit = false
		s.series.Unlock()

		if !ok {
			total--
		}
		if chunkCreated {
			// 如果有新的chunk被创建
			a.head.metrics.chunks.Inc()
			a.head.metrics.chunksCreated.Inc()
		}
	}

	// 增加samplesAppended这个metric
	a.head.metrics.samplesAppended.Add(float64(total))
	// 更新head的mint和maxt
	a.head.updateMinMaxTime(a.mint, a.maxt)

	return nil
}

func (a *headAppender) Rollback() error {
	a.head.metrics.activeAppenders.Dec()
	for _, s := range a.samples {
		s.series.Lock()
		// Rollback的话直接将sample所属的series的pendingCommit设置为false
		s.series.pendingCommit = false
		s.series.Unlock()
	}
	a.head.putAppendBuffer(a.samples)

	// Series are created in the head memory regardless of rollback. Thus we have
	// to log them to the WAL in any case.
	// Series是在head memory中创建的，和rollback无关，因此我们要把它写入WAL中，不管在什么情况下
	a.samples = nil
	return a.log()
}

// Delete all samples in the range of [mint, maxt] for series that satisfy the given
// label matchers.
func (h *Head) Delete(mint, maxt int64, ms ...labels.Matcher) error {
	// Do not delete anything beyond the currently valid range.
	mint, maxt = clampInterval(mint, maxt, h.MinTime(), h.MaxTime())

	ir := h.indexRange(mint, maxt)

	p, err := PostingsForMatchers(ir, ms...)
	if err != nil {
		return errors.Wrap(err, "select series")
	}

	var stones []Stone
	dirty := false
	for p.Next() {
		series := h.series.getByID(p.At())

		t0, t1 := series.minTime(), series.maxTime()
		if t0 == math.MinInt64 || t1 == math.MinInt64 {
			continue
		}
		// Delete only until the current values and not beyond.
		t0, t1 = clampInterval(mint, maxt, t0, t1)
		if h.wal != nil {
			stones = append(stones, Stone{p.At(), Intervals{{t0, t1}}})
		}
		if err := h.chunkRewrite(p.At(), Intervals{{t0, t1}}); err != nil {
			return errors.Wrap(err, "delete samples")
		}
		dirty = true
	}
	if p.Err() != nil {
		return p.Err()
	}
	var enc RecordEncoder
	if h.wal != nil {
		// Although we don't store the stones in the head
		// we need to write them to the WAL to mark these as deleted
		// after a restart while loading the WAL.
		if err := h.wal.Log(enc.Tombstones(stones, nil)); err != nil {
			return err
		}
	}
	if dirty {
		h.gc()
	}

	return nil
}

// chunkRewrite re-writes the chunks which overlaps with deleted ranges
// and removes the samples in the deleted ranges.
// Chunks is deleted if no samples are left at the end.
// chunkRewrite重写和deleted ranges重合的chunks并且移除deleted ranges里的samples
// 如果到最后没有samples遗留了，则Chunks被删除
func (h *Head) chunkRewrite(ref uint64, dranges Intervals) (err error) {
	if len(dranges) == 0 {
		return nil
	}

	ms := h.series.getByID(ref)
	ms.Lock()
	defer ms.Unlock()
	if len(ms.chunks) == 0 {
		return nil
	}

	metas := ms.chunksMetas()
	mint, maxt := metas[0].MinTime, metas[len(metas)-1].MaxTime
	it := newChunkSeriesIterator(metas, dranges, mint, maxt)

	ms.reset()
	for it.Next() {
		t, v := it.At()
		ok, _ := ms.append(t, v)
		if !ok {
			level.Warn(h.logger).Log("msg", "failed to add sample during delete")
		}
	}

	return nil
}

// gc removes data before the minimum timestamp from the head.
// gc移除head的minimum timestamp以前的数据
func (h *Head) gc() {
	// Only data strictly lower than this timestamp must be deleted.
	// 只有低于timestamp的数据才会被移除
	mint := h.MinTime()

	// Drop old chunks and remember series IDs and hashes if they can be
	// deleted entirely.
	// 丢弃老的chunks以及记住series IDs以及哈希，如果他们能被完全删除
	// gc只是删除内存中的series以及chunk
	deleted, chunksRemoved := h.series.gc(mint)
	seriesRemoved := len(deleted)

	h.metrics.seriesRemoved.Add(float64(seriesRemoved))
	h.metrics.series.Sub(float64(seriesRemoved))
	h.metrics.chunksRemoved.Add(float64(chunksRemoved))
	h.metrics.chunks.Sub(float64(chunksRemoved))

	// Remove deleted series IDs from the postings lists.
	// 从postings lists中移除已经被删除的series ID
	h.postings.Delete(deleted)

	if h.wal != nil {
		_, last, _ := h.wal.Segments()
		h.deletedMtx.Lock()
		// Keep series records until we're past segment 'last'
		// because the WAL will still have samples records with
		// this ref ID. If we didn't keep these series records then
		// on start up when we replay the WAL, or any other code
		// that reads the WAL, wouldn't be able to use those
		// samples since we would have no labels for that ref ID.
		// 保持series records，直到我们经过了'last'这个segment
		// 因为wal中还是会保存这个ref ID相关的samples，如果我们不保持这些series records
		// 那么当我们对WAL进行重放，或者任何其他的code读取WAL，将不能使用这些samples，因为
		// 对于这个ref ID我们没有samples
		for ref := range deleted {
			// 因为这些实际上已经被删除的wal
			h.deleted[ref] = last
		}
		h.deletedMtx.Unlock()
	}

	// Rebuild symbols and label value indices from what is left in the postings terms.
	symbols := make(map[string]struct{}, len(h.symbols))
	values := make(map[string]stringset, len(h.values))

	if err := h.postings.Iter(func(t labels.Label, _ index.Postings) error {
		// 基于postings构建symbols以及label value
		symbols[t.Name] = struct{}{}
		symbols[t.Value] = struct{}{}

		ss, ok := values[t.Name]
		if !ok {
			ss = stringset{}
			values[t.Name] = ss
		}
		ss.set(t.Value)
		return nil
	}); err != nil {
		// This should never happen, as the iteration function only returns nil.
		panic(err)
	}

	h.symMtx.Lock()

	// 恢复head中的symbols以及values
	h.symbols = symbols
	h.values = values

	h.symMtx.Unlock()
}

// Tombstones returns a new reader over the head's tombstones
func (h *Head) Tombstones() (TombstoneReader, error) {
	return emptyTombstoneReader, nil
}

// Index returns an IndexReader against the block.
func (h *Head) Index() (IndexReader, error) {
	return h.indexRange(math.MinInt64, math.MaxInt64), nil
}

func (h *Head) indexRange(mint, maxt int64) *headIndexReader {
	if hmin := h.MinTime(); hmin > mint {
		mint = hmin
	}
	return &headIndexReader{head: h, mint: mint, maxt: maxt}
}

// Chunks returns a ChunkReader against the block.
func (h *Head) Chunks() (ChunkReader, error) {
	return h.chunksRange(math.MinInt64, math.MaxInt64), nil
}

func (h *Head) chunksRange(mint, maxt int64) *headChunkReader {
	if hmin := h.MinTime(); hmin > mint {
		mint = hmin
	}
	return &headChunkReader{head: h, mint: mint, maxt: maxt}
}

// MinTime returns the lowest time bound on visible data in the head.
func (h *Head) MinTime() int64 {
	return atomic.LoadInt64(&h.minTime)
}

// MaxTime returns the highest timestamp seen in data of the head.
func (h *Head) MaxTime() int64 {
	return atomic.LoadInt64(&h.maxTime)
}

// compactable returns whether the head has a compactable range.
// compactable返回head是否有一个compactable range
// The head has a compactable range when the head time range is 1.5 times the chunk range.
// The 0.5 acts as a buffer of the appendable window.
// 当head的time range是1.5倍的chunk range，则head有compactable range
func (h *Head) compactable() bool {
	return h.MaxTime()-h.MinTime() > h.chunkRange/2*3
}

// Close flushes the WAL and closes the head.
func (h *Head) Close() error {
	if h.wal == nil {
		return nil
	}
	return h.wal.Close()
}

type headChunkReader struct {
	head       *Head
	mint, maxt int64
}

func (h *headChunkReader) Close() error {
	return nil
}

// packChunkID packs a seriesID and a chunkID within it into a global 8 byte ID.
// It panicks if the seriesID exceeds 5 bytes or the chunk ID 3 bytes.
func packChunkID(seriesID, chunkID uint64) uint64 {
	if seriesID > (1<<40)-1 {
		// series ID不能超过5个字节
		panic("series ID exceeds 5 bytes")
	}
	if chunkID > (1<<24)-1 {
		// chunkID不能超过3个字节
		panic("chunk ID exceeds 3 bytes")
	}
	return (seriesID << 24) | chunkID
}

func unpackChunkID(id uint64) (seriesID, chunkID uint64) {
	return id >> 24, (id << 40) >> 40
}

// Chunk returns the chunk for the reference number.
// Chunk返回reference number对应的chunk
func (h *headChunkReader) Chunk(ref uint64) (chunkenc.Chunk, error) {
	sid, cid := unpackChunkID(ref)

	s := h.head.series.getByID(sid)
	// This means that the series has been garbage collected.
	if s == nil {
		return nil, ErrNotFound
	}

	s.Lock()
	c := s.chunk(int(cid))

	// This means that the chunk has been garbage collected or is outside
	// the specified range.
	// 如果找不到chunk，则说明它已经被GC了或者处于指定的range之外
	if c == nil || !c.OverlapsClosedInterval(h.mint, h.maxt) {
		s.Unlock()
		return nil, ErrNotFound
	}
	s.Unlock()

	return &safeChunk{
		Chunk: c.chunk,
		s:     s,
		cid:   int(cid),
	}, nil
}

type safeChunk struct {
	chunkenc.Chunk
	s   *memSeries
	cid int
}

func (c *safeChunk) Iterator() chunkenc.Iterator {
	c.s.Lock()
	it := c.s.iterator(c.cid)
	c.s.Unlock()
	return it
}

type headIndexReader struct {
	head       *Head
	mint, maxt int64
}

func (h *headIndexReader) Close() error {
	return nil
}

func (h *headIndexReader) Symbols() (map[string]struct{}, error) {
	h.head.symMtx.RLock()
	defer h.head.symMtx.RUnlock()

	res := make(map[string]struct{}, len(h.head.symbols))

	// 返回head中存储的所有symbols，即所有label的key和value
	for s := range h.head.symbols {
		res[s] = struct{}{}
	}
	return res, nil
}

// LabelValues returns the possible label values
// LabelValues返回所有可能的label values
func (h *headIndexReader) LabelValues(names ...string) (index.StringTuples, error) {
	if len(names) != 1 {
		return nil, encoding.ErrInvalidSize
	}

	h.head.symMtx.RLock()
	sl := make([]string, 0, len(h.head.values[names[0]]))
	for s := range h.head.values[names[0]] {
		sl = append(sl, s)
	}
	h.head.symMtx.RUnlock()
	sort.Strings(sl)

	return index.NewStringTuples(sl, len(names))
}

// LabelNames returns all the unique label names present in the head.
// LabelNames返回head中所有唯一的label names
func (h *headIndexReader) LabelNames() ([]string, error) {
	h.head.symMtx.RLock()
	defer h.head.symMtx.RUnlock()
	labelNames := make([]string, 0, len(h.head.values))
	for name := range h.head.values {
		if name == "" {
			continue
		}
		labelNames = append(labelNames, name)
	}
	sort.Strings(labelNames)
	return labelNames, nil
}

// Postings returns the postings list iterator for the label pair.
// Postings返回给定的label pair的postings list iterator
func (h *headIndexReader) Postings(name, value string) (index.Postings, error) {
	return h.head.postings.Get(name, value), nil
}

func (h *headIndexReader) SortedPostings(p index.Postings) index.Postings {
	series := make([]*memSeries, 0, 128)

	// Fetch all the series only once.
	// 一次性找到所有的series
	for p.Next() {
		// 用id找到series
		s := h.head.series.getByID(p.At())
		if s == nil {
			level.Debug(h.head.logger).Log("msg", "looked up series not found")
		} else {
			series = append(series, s)
		}
	}
	if err := p.Err(); err != nil {
		return index.ErrPostings(errors.Wrap(err, "expand postings"))
	}

	// 对series根据label set进行排序
	sort.Slice(series, func(i, j int) bool {
		return labels.Compare(series[i].lset, series[j].lset) < 0
	})

	// Convert back to list.
	// 重新将series转换为id
	ep := make([]uint64, 0, len(series))
	// 返回排序后的series id	
	for _, p := range series {
		ep = append(ep, p.ref)
	}
	// 构建成一个list的postings
	return index.NewListPostings(ep)
}

// Series returns the series for the given reference.
// Series返回给定references的series
func (h *headIndexReader) Series(ref uint64, lbls *labels.Labels, chks *[]chunks.Meta) error {
	s := h.head.series.getByID(ref)

	if s == nil {
		h.head.metrics.seriesNotFound.Inc()
		return ErrNotFound
	}
	// 重用chunks和labelset
	*lbls = append((*lbls)[:0], s.lset...)

	s.Lock()
	defer s.Unlock()

	*chks = (*chks)[:0]

	for i, c := range s.chunks {
		// Do not expose chunks that are outside of the specified range.
		// 不要暴露指定的范围之外的chunks
		if !c.OverlapsClosedInterval(h.mint, h.maxt) {
			continue
		}
		// Set the head chunks as open (being appended to).
		maxTime := c.maxTime
		if s.headChunk == c {
			maxTime = math.MaxInt64
		}

		*chks = append(*chks, chunks.Meta{
			MinTime: c.minTime,
			MaxTime: maxTime,
			// 组合Ref ID和chunk id
			Ref:     packChunkID(s.ref, uint64(s.chunkID(i))),
		})
	}

	return nil
}

func (h *headIndexReader) LabelIndices() ([][]string, error) {
	h.head.symMtx.RLock()
	defer h.head.symMtx.RUnlock()
	res := [][]string{}
	for s := range h.head.values {
		res = append(res, []string{s})
	}
	return res, nil
}

func (h *Head) getOrCreate(hash uint64, lset labels.Labels) (*memSeries, bool) {
	// Just using `getOrSet` below would be semantically sufficient, but we'd create
	// a new series on every sample inserted via Add(), which causes allocations
	// and makes our series IDs rather random and harder to compress in postings.
	// 使用`getOrSet`就足够用了，但是我们在通过Add()插入每个sample都会创建一个新的series，它会导致内存分配
	// 并且让我们的ID更随机，在postings的时候压缩更难
	s := h.series.getByHash(hash, lset)
	if s != nil {
		return s, false
	}

	// Optimistically assume that we are the first one to create the series.
	// 乐观地假设我们是第一个创建sereis的
	// getOrCreate()对getOrCreateWithID()的封装，其实就是创建了一个id
	id := atomic.AddUint64(&h.lastSeriesID, 1)

	return h.getOrCreateWithID(id, hash, lset)
}

// 用id获取或者创建Head里的series
func (h *Head) getOrCreateWithID(id, hash uint64, lset labels.Labels) (*memSeries, bool) {
	s := newMemSeries(lset, id, h.chunkRange)

	s, created := h.series.getOrSet(hash, s)
	if !created {
		// 之前已经存在，则直接返回
		return s, false
	}

	// 增加series到计数，增加seriesCreated的计数
	h.metrics.series.Inc()
	h.metrics.seriesCreated.Inc()

	// 将id和lset放入postings
	// 构建label到id的索引
	h.postings.Add(id, lset)

	h.symMtx.Lock()
	defer h.symMtx.Unlock()

	// 更新head中的values和symbols字段
	for _, l := range lset {
		// head中保存所有label的name到所有value的映射
		valset, ok := h.values[l.Name]
		if !ok {
			valset = stringset{}
			h.values[l.Name] = valset
		}
		// 载valueset中设置l.Value
		valset.set(l.Value)

		// symbols中包含了所有label的name和value
		h.symbols[l.Name] = struct{}{}
		h.symbols[l.Value] = struct{}{}
	}

	return s, true
}

// seriesHashmap is a simple hashmap for memSeries by their label set. It is built
// on top of a regular hashmap and holds a slice of series to resolve hash collisions.
// Its methods require the hash to be submitted with it to avoid re-computations throughout
// the code.
// seriesHashmap是一个简单的hashmap，通过它们的label set找到memSeries，它构建在一个通常的hashmap之上
// 并且维护一个series的slice来解决哈希冲突，它的方法要求提供hash来避免重复计算
type seriesHashmap map[uint64][]*memSeries

func (m seriesHashmap) get(hash uint64, lset labels.Labels) *memSeries {
	for _, s := range m[hash] {
		// 有多个labelset哈希值相等的话，直接比较
		if s.lset.Equals(lset) {
			return s
		}
	}
	return nil
}

func (m seriesHashmap) set(hash uint64, s *memSeries) {
	l := m[hash]
	for i, prev := range l {
		// 遍历[]*memSeries，如果有哈希值相等的直接覆盖
		if prev.lset.Equals(s.lset) {
			l[i] = s
			return
		}
	}
	// 否则，扩展[]*memSeries
	m[hash] = append(l, s)
}

func (m seriesHashmap) del(hash uint64, lset labels.Labels) {
	var rem []*memSeries
	for _, s := range m[hash] {
		if !s.lset.Equals(lset) {
			rem = append(rem, s)
		}
	}
	if len(rem) == 0 {
		// 如果哈希值没有series了，则将其从seriesHashmap中删除
		delete(m, hash)
	} else {
		m[hash] = rem
	}
}

// stripeSeries locks modulo ranges of IDs and hashes to reduce lock contention.
// The locks are padded to not be on the same cache line. Filling the padded space
// with the maps was profiled to be slower – likely due to the additional pointer
// dereferences.
// stripeSeries锁住对ID以及哈希用模进行划分以防止锁争用
// locks被扩展了，以防止在同一个cache line中
type stripeSeries struct {
	series [stripeSize]map[uint64]*memSeries
	hashes [stripeSize]seriesHashmap
	locks  [stripeSize]stripeLock
}

const (
	stripeSize = 1 << 14
	stripeMask = stripeSize - 1
)

type stripeLock struct {
	sync.RWMutex
	// Padding to avoid multiple locks being on the same cache line.
	_ [40]byte
}

func newStripeSeries() *stripeSeries {
	s := &stripeSeries{}

	// 初始化每个series和hashes
	for i := range s.series {
		s.series[i] = map[uint64]*memSeries{}
	}
	for i := range s.hashes {
		s.hashes[i] = seriesHashmap{}
	}
	return s
}

// gc garbage collects old chunks that are strictly before mint and removes
// series entirely that have no chunks left.
// gc会清除那些严格在mint之前的老的chunks，并且移除整个series，如果它没有chunks了的话
func (s *stripeSeries) gc(mint int64) (map[uint64]struct{}, int) {
	var (
		deleted  = map[uint64]struct{}{}
		rmChunks = 0
	)
	// Run through all series and truncate old chunks. Mark those with no
	// chunks left as deleted and store their ID.
	// 遍历所有的series并且移除老的chunks，将那些没有chunks剩下的series标记为deleted并且存储它们的ID
	for i := 0; i < stripeSize; i++ {
		s.locks[i].Lock()

		// 对seriesHashmap进行遍历
		for hash, all := range s.hashes[i] {
			for _, series := range all {
				series.Lock()
				// 将mint之前对series移除
				rmChunks += series.truncateChunksBefore(mint)

				if len(series.chunks) > 0 || series.pendingCommit {
					// 如果还有chunk或者还有sample等待commit，则不将series标记为删除
					series.Unlock()
					continue
				}

				// The series is gone entirely. We need to keep the series lock
				// and make sure we have acquired the stripe locks for hash and ID of the
				// series alike.
				// 如果series被完全移除了，我们需要保持series lock并且确保我们也获取了该series的hash以及ID的stripe lock
				// If we don't hold them all, there's a very small chance that a series receives
				// samples again while we are half-way into deleting it.
				// 如果我们不能同时持有两个锁，那么有很小的可能性，这个series正在接收samples，而我们正在删除它
				j := int(series.ref & stripeMask)

				if i != j {
					// 只有i和j不相等的时候才锁，不然会死锁
					s.locks[j].Lock()
				}

				// deleted记录已经被删除的series
				deleted[series.ref] = struct{}{}
				s.hashes[i].del(hash, series.lset)
				delete(s.series[j], series.ref)

				if i != j {
					s.locks[j].Unlock()
				}

				series.Unlock()
			}
		}

		s.locks[i].Unlock()
	}

	return deleted, rmChunks
}

func (s *stripeSeries) getByID(id uint64) *memSeries {
	i := id & stripeMask

	s.locks[i].RLock()
	series := s.series[i][id]
	s.locks[i].RUnlock()

	return series
}

func (s *stripeSeries) getByHash(hash uint64, lset labels.Labels) *memSeries {
	i := hash & stripeMask

	s.locks[i].RLock()
	// 通过哈希找到series
	series := s.hashes[i].get(hash, lset)
	s.locks[i].RUnlock()

	return series
}

// 通过hash进行get，如果不存在，则通过series进行set
func (s *stripeSeries) getOrSet(hash uint64, series *memSeries) (*memSeries, bool) {
	// 取模，找到相应的series
	i := hash & stripeMask

	s.locks[i].Lock()

	// 更新hash列表
	if prev := s.hashes[i].get(hash, series.lset); prev != nil {
		s.locks[i].Unlock()
		// 如果之前已经有了，就直接返回
		return prev, false
	}
	// 哈希值到series到映射
	s.hashes[i].set(hash, series)
	s.locks[i].Unlock()

	// i是ref和stripeMask取模
	i = series.ref & stripeMask

	// 更新series列表
	s.locks[i].Lock()
	// series id到series的映射
	s.series[i][series.ref] = series
	s.locks[i].Unlock()

	return series, true
}

type sample struct {
	t int64
	v float64
}

func (s sample) T() int64 {
	return s.t
}

func (s sample) V() float64 {
	return s.v
}

// memSeries is the in-memory representation of a series. None of its methods
// are goroutine safe and it is the caller's responsibility to lock it.
// memSeries是一个series在内存中的表示
type memSeries struct {
	sync.Mutex

	ref          uint64
	lset         labels.Labels
	// chunk中保存了sample数据
	chunks       []*memChunk
	headChunk    *memChunk
	chunkRange   int64
	// 第一个chunk的ID
	firstChunkID int

	// 创建下一个chunk的时间戳
	nextAt        int64 // Timestamp at which to cut the next chunk.
	sampleBuf     [4]sample
	// 这个series是否有samples等待被commit
	pendingCommit bool // Whether there are samples waiting to be committed to this series.

	// 当前chunk的appender
	app chunkenc.Appender // Current appender for the chunk.
}

func newMemSeries(lset labels.Labels, id uint64, chunkRange int64) *memSeries {
	s := &memSeries{
		lset:       lset,
		ref:        id,
		chunkRange: chunkRange,
		nextAt:     math.MinInt64,
	}
	return s
}

func (s *memSeries) minTime() int64 {
	if len(s.chunks) == 0 {
		return math.MinInt64
	}
	return s.chunks[0].minTime
}

func (s *memSeries) maxTime() int64 {
	c := s.head()
	if c == nil {
		return math.MinInt64
	}
	return c.maxTime
}

func (s *memSeries) cut(mint int64) *memChunk {
	// 创建一个memory chunk
	c := &memChunk{
		chunk:   chunkenc.NewXORChunk(),
		minTime: mint,
		// maxTime是最小值
		maxTime: math.MinInt64,
	}
	// 扩展chunks
	s.chunks = append(s.chunks, c)
	s.headChunk = c

	// Set upper bound on when the next chunk must be started. An earlier timestamp
	// may be chosen dynamically at a later point.
	// 设置下一次chunk启动的upper bound，在之后可能会动态地选择一个更早的时间戳
	s.nextAt = rangeForTimestamp(mint, s.chunkRange)

	app, err := c.chunk.Appender()
	if err != nil {
		// 不应该出现错误的地方，直接panic
		panic(err)
	}
	s.app = app
	return c
}

func (s *memSeries) chunksMetas() []chunks.Meta {
	metas := make([]chunks.Meta, 0, len(s.chunks))
	for _, chk := range s.chunks {
		metas = append(metas, chunks.Meta{Chunk: chk.chunk, MinTime: chk.minTime, MaxTime: chk.maxTime})
	}
	return metas
}

// reset re-initialises all the variable in the memSeries except 'lset', 'ref',
// and 'chunkRange', like how it would appear after 'newMemSeries(...)'.
func (s *memSeries) reset() {
	s.chunks = nil
	s.headChunk = nil
	s.firstChunkID = 0
	s.nextAt = math.MinInt64
	s.sampleBuf = [4]sample{}
	s.pendingCommit = false
	s.app = nil
}

// appendable checks whether the given sample is valid for appending to the series.
// appendable检测是否给定的sample对于加入到series中说合法的
func (s *memSeries) appendable(t int64, v float64) error {
	// 返回head chunk
	c := s.head()
	if c == nil {
		return nil
	}

	if t > c.maxTime {
		// 只能大于maxTime，表示新的时序
		return nil
	}
	if t < c.maxTime {
		return ErrOutOfOrderSample
	}
	// We are allowing exact duplicates as we can encounter them in valid cases
	// like federation and erroring out at that time would be extremely noisy.
	// 可能遇到完全相同的sample，比如在federation的场景下，如果在这种情况下报错将非常noisy
	if math.Float64bits(s.sampleBuf[3].v) != math.Float64bits(v) {
		return ErrAmendSample
	}
	return nil
}

func (s *memSeries) chunk(id int) *memChunk {
	ix := id - s.firstChunkID
	if ix < 0 || ix >= len(s.chunks) {
		return nil
	}
	return s.chunks[ix]
}

func (s *memSeries) chunkID(pos int) int {
	return pos + s.firstChunkID
}

// truncateChunksBefore removes all chunks from the series that have not timestamp
// at or after mint. Chunk IDs remain unchanged.
// truncateChunksBefore从series中移除所有没有在mint之后的sample的chunks，Chunk IDs保持不变
func (s *memSeries) truncateChunksBefore(mint int64) (removed int) {
	var k int
	for i, c := range s.chunks {
		// 遍历chunks，直到chunk的maxTime大于等于mint
		if c.maxTime >= mint {
			break
		}
		k = i + 1
	}
	s.chunks = append(s.chunks[:0], s.chunks[k:]...)
	s.firstChunkID += k
	if len(s.chunks) == 0 {
		s.headChunk = nil
	} else {
		// 确保head chunk是最后一个chunk
		s.headChunk = s.chunks[len(s.chunks)-1]
	}

	return k
}

// append adds the sample (t, v) to the series.
// append将sample (t, v)加入到series中
func (s *memSeries) append(t int64, v float64) (success, chunkCreated bool) {
	// Based on Gorilla white papers this offers near-optimal compression ratio
	// so anything bigger that this has diminishing returns and increases
	// the time range within which we have to decompress all samples.
	// 120个sample，压缩最优
	const samplesPerChunk = 120

	c := s.head()

	if c == nil {
		// 如果memSeries中还没有chunk，就创建一个
		c = s.cut(t)
		chunkCreated = true
	}
	numSamples := c.chunk.NumSamples()

	// Out of order sample.
	if c.maxTime >= t {
		// 失序的sample，直接返回false
		return false, chunkCreated
	}
	// If we reach 25% of a chunk's desired sample count, set a definitive time
	// at which to start the next chunk.
	// At latest it must happen at the timestamp set when the chunk was cut.
	if numSamples == samplesPerChunk/4 {
		s.nextAt = computeChunkEndTime(c.minTime, c.maxTime, s.nextAt)
	}
	if t >= s.nextAt {
		// 如果达到了nextAt，则创建一个新的chunk
		c = s.cut(t)
		chunkCreated = true
	}
	// 直接将t, v append到最新的chunk
	s.app.Append(t, v)

	// 更新maxTime
	c.maxTime = t

	// 替换前四个samples
	s.sampleBuf[0] = s.sampleBuf[1]
	s.sampleBuf[1] = s.sampleBuf[2]
	s.sampleBuf[2] = s.sampleBuf[3]
	s.sampleBuf[3] = sample{t: t, v: v}

	return true, chunkCreated
}

// computeChunkEndTime estimates the end timestamp based the beginning of a chunk,
// its current timestamp and the upper bound up to which we insert data.
// It assumes that the time range is 1/4 full.
func computeChunkEndTime(start, cur, max int64) int64 {
	a := (max - start) / ((cur - start + 1) * 4)
	if a == 0 {
		return max
	}
	return start + (max-start)/a
}

func (s *memSeries) iterator(id int) chunkenc.Iterator {
	c := s.chunk(id)
	// TODO(fabxc): Work around! A querier may have retrieved a pointer to a series' chunk,
	// which got then garbage collected before it got accessed.
	// We must ensure to not garbage collect as long as any readers still hold a reference.
	if c == nil {
		return chunkenc.NewNopIterator()
	}

	if id-s.firstChunkID < len(s.chunks)-1 {
		return c.chunk.Iterator()
	}
	// Serve the last 4 samples for the last chunk from the sample buffer
	// as their compressed bytes may be mutated by added samples.
	it := &memSafeIterator{
		Iterator: c.chunk.Iterator(),
		i:        -1,
		total:    c.chunk.NumSamples(),
		buf:      s.sampleBuf,
	}
	return it
}

func (s *memSeries) head() *memChunk {
	return s.headChunk
}

type memChunk struct {
	chunk            chunkenc.Chunk
	minTime, maxTime int64
}

// Returns true if the chunk overlaps [mint, maxt].
func (mc *memChunk) OverlapsClosedInterval(mint, maxt int64) bool {
	return mc.minTime <= maxt && mint <= mc.maxTime
}

type memSafeIterator struct {
	chunkenc.Iterator

	i     int
	total int
	buf   [4]sample
}

func (it *memSafeIterator) Next() bool {
	if it.i+1 >= it.total {
		return false
	}
	it.i++
	if it.total-it.i > 4 {
		return it.Iterator.Next()
	}
	return true
}

func (it *memSafeIterator) At() (int64, float64) {
	if it.total-it.i > 4 {
		return it.Iterator.At()
	}
	s := it.buf[4-(it.total-it.i)]
	return s.t, s.v
}

type stringset map[string]struct{}

func (ss stringset) set(s string) {
	ss[s] = struct{}{}
}

func (ss stringset) String() string {
	return strings.Join(ss.slice(), ",")
}

func (ss stringset) slice() []string {
	slice := make([]string, 0, len(ss))
	for k := range ss {
		slice = append(slice, k)
	}
	sort.Strings(slice)
	return slice
}
