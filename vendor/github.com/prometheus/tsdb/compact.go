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
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb/chunkenc"
	"github.com/prometheus/tsdb/chunks"
	tsdb_errors "github.com/prometheus/tsdb/errors"
	"github.com/prometheus/tsdb/fileutil"
	"github.com/prometheus/tsdb/index"
	"github.com/prometheus/tsdb/labels"
)

// ExponentialBlockRanges returns the time ranges based on the stepSize.
func ExponentialBlockRanges(minSize int64, steps, stepSize int) []int64 {
	ranges := make([]int64, 0, steps)
	curRange := minSize
	for i := 0; i < steps; i++ {
		ranges = append(ranges, curRange)
		curRange = curRange * int64(stepSize)
	}

	return ranges
}

// Compactor provides compaction against an underlying storage
// of time series data.
// Compactor提供了对于时序数据的压缩
type Compactor interface {
	// Plan returns a set of directories that can be compacted concurrently.
	// The directories can be overlapping.
	// Results returned when compactions are in progress are undefined.
	// Plan返回一系列可以被并行压缩的目录，目录可以有重叠
	// 当压缩正在进行的时候，返回的结果是不确定的
	Plan(dir string) ([]string, error)

	// Write persists a Block into a directory.
	// No Block is written when resulting Block has 0 samples, and returns empty ulid.ULID{}.
	// Write将一个Block持久化到一个目录中
	// 当resulting Block有着0个samples时，没有Block会被写入，并且返回空的ULID
	Write(dest string, b BlockReader, mint, maxt int64, parent *BlockMeta) (ulid.ULID, error)

	// Compact runs compaction against the provided directories. Must
	// only be called concurrently with results of Plan().
	// Compact对提供的目录运行压缩，只能对Plan()返回的结果并行调用
	// Can optionally pass a list of already open blocks,
	// to avoid having to reopen them.
	// 可以可选地提供一系列已经打开的blocks，从而避免重新打开它们
	// When resulting Block has 0 samples
	//  * No block is written.
	//  * The source dirs are marked Deletable.
	//  * Returns empty ulid.ULID{}.
	// 当Block没有samples时：
	//  * 没有block被写入
	//	* 源目录被标记为Deletable
	//	* 返回空的ulid.ULID
	Compact(dest string, dirs []string, open []*Block) (ulid.ULID, error)
}

// LeveledCompactor implements the Compactor interface.
// LeveledCompactor实现了Compactor接口
type LeveledCompactor struct {
	metrics   *compactorMetrics
	logger    log.Logger
	ranges    []int64
	chunkPool chunkenc.Pool
	ctx       context.Context
}

type compactorMetrics struct {
	ran               prometheus.Counter
	populatingBlocks  prometheus.Gauge
	overlappingBlocks prometheus.Counter
	duration          prometheus.Histogram
	chunkSize         prometheus.Histogram
	chunkSamples      prometheus.Histogram
	chunkRange        prometheus.Histogram
}

func newCompactorMetrics(r prometheus.Registerer) *compactorMetrics {
	m := &compactorMetrics{}

	m.ran = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_compactions_total",
		Help: "Total number of compactions that were executed for the partition.",
	})
	m.populatingBlocks = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_compaction_populating_block",
		Help: "Set to 1 when a block is currently being written to the disk.",
	})
	m.overlappingBlocks = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_vertical_compactions_total",
		Help: "Total number of compactions done on overlapping blocks.",
	})
	m.duration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "prometheus_tsdb_compaction_duration_seconds",
		Help:    "Duration of compaction runs",
		Buckets: prometheus.ExponentialBuckets(1, 2, 10),
	})
	m.chunkSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "prometheus_tsdb_compaction_chunk_size_bytes",
		Help:    "Final size of chunks on their first compaction",
		Buckets: prometheus.ExponentialBuckets(32, 1.5, 12),
	})
	m.chunkSamples = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "prometheus_tsdb_compaction_chunk_samples",
		Help:    "Final number of samples on their first compaction",
		Buckets: prometheus.ExponentialBuckets(4, 1.5, 12),
	})
	m.chunkRange = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "prometheus_tsdb_compaction_chunk_range_seconds",
		Help:    "Final time range of chunks on their first compaction",
		Buckets: prometheus.ExponentialBuckets(100, 4, 10),
	})

	if r != nil {
		r.MustRegister(
			m.ran,
			m.populatingBlocks,
			m.overlappingBlocks,
			m.duration,
			m.chunkRange,
			m.chunkSamples,
			m.chunkSize,
		)
	}
	return m
}

// NewLeveledCompactor returns a LeveledCompactor.
func NewLeveledCompactor(ctx context.Context, r prometheus.Registerer, l log.Logger, ranges []int64, pool chunkenc.Pool) (*LeveledCompactor, error) {
	if len(ranges) == 0 {
		return nil, errors.Errorf("at least one range must be provided")
	}
	if pool == nil {
		pool = chunkenc.NewPool()
	}
	if l == nil {
		l = log.NewNopLogger()
	}
	return &LeveledCompactor{
		ranges:    ranges,
		chunkPool: pool,
		logger:    l,
		metrics:   newCompactorMetrics(r),
		ctx:       ctx,
	}, nil
}

type dirMeta struct {
	dir  string
	meta *BlockMeta
}

// Plan returns a list of compactable blocks in the provided directory.
// Plan返回给定目录一系列可以压缩的blocks
func (c *LeveledCompactor) Plan(dir string) ([]string, error) {
	dirs, err := blockDirs(dir)
	if err != nil {
		return nil, err
	}
	if len(dirs) < 1 {
		return nil, nil
	}

	var dms []dirMeta
	for _, dir := range dirs {
		meta, _, err := readMetaFile(dir)
		if err != nil {
			return nil, err
		}
		// 读取各个block的metadata
		dms = append(dms, dirMeta{dir, meta})
	}
	return c.plan(dms)
}

func (c *LeveledCompactor) plan(dms []dirMeta) ([]string, error) {
	// 首先对blocks进行排序
	sort.Slice(dms, func(i, j int) bool {
		return dms[i].meta.MinTime < dms[j].meta.MinTime
	})

	res := c.selectOverlappingDirs(dms)
	if len(res) > 0 {
		return res, nil
	}
	// No overlapping blocks, do compaction the usual way.
	// 没有重合的blocks，能以正常的方式进行压缩
	// We do not include a recently created block with max(minTime), so the block which was just created from WAL.
	// This gives users a window of a full block size to piece-wise backup new data without having to care about data overlap.
	// 我们没有包括最新的block，这个block刚刚从wal中创建，这给了用户一个窗口，能有一个完整的block size用于分段的备份，而不需要担心数据的重合
	dms = dms[:len(dms)-1]

	for _, dm := range c.selectDirs(dms) {
		res = append(res, dm.dir)
	}
	if len(res) > 0 {
		return res, nil
	}

	// Compact any blocks with big enough time range that have >5% tombstones.
	// 压缩任何有着大于5%的tombstones的blocks
	for i := len(dms) - 1; i >= 0; i-- {
		meta := dms[i].meta
		if meta.MaxTime-meta.MinTime < c.ranges[len(c.ranges)/2] {
			break
		}
		if float64(meta.Stats.NumTombstones)/float64(meta.Stats.NumSeries+1) > 0.05 {
			return []string{dms[i].dir}, nil
		}
	}

	return nil, nil
}

// selectDirs returns the dir metas that should be compacted into a single new block.
// If only a single block range is configured, the result is always nil.
// selectDirs返回应该被压缩为一个新的block的dir metas，如果只有一个block range，那么返回的结果总是nil
func (c *LeveledCompactor) selectDirs(ds []dirMeta) []dirMeta {
	// 如果c.ranges小于2，则直接返回nil
	if len(c.ranges) < 2 || len(ds) < 1 {
		return nil
	}

	highTime := ds[len(ds)-1].meta.MinTime

	for _, iv := range c.ranges[1:] {
		parts := splitByRange(ds, iv)
		if len(parts) == 0 {
			continue
		}

	Outer:
		for _, p := range parts {
			// Do not select the range if it has a block whose compaction failed.
			for _, dm := range p {
				// 不要选择compaction失败过的block
				if dm.meta.Compaction.Failed {
					continue Outer
				}
			}

			// 求出整个block group的mint和maxt
			mint := p[0].meta.MinTime
			maxt := p[len(p)-1].meta.MaxTime
			// Pick the range of blocks if it spans the full range (potentially with gaps)
			// or is before the most recent block.
			// This ensures we don't compact blocks prematurely when another one of the same
			// size still fits in the range.
			// 如果blocks跨过了足够长的时间，或者在最近的block之前，选择这些blocks，这不会过早地压缩blocks
			if (maxt-mint == iv || maxt <= highTime) && len(p) > 1 {
				// 选出一个合适的block group就返回
				return p
			}
		}
	}

	return nil
}

// selectOverlappingDirs returns all dirs with overlapping time ranges.
// selectOverlappingDirs返回有着重合的时间窗口的所有目录
// It expects sorted input by mint and returns the overlapping dirs in the same order as received.
func (c *LeveledCompactor) selectOverlappingDirs(ds []dirMeta) []string {
	if len(ds) < 2 {
		return nil
	}
	var overlappingDirs []string
	globalMaxt := ds[0].meta.MaxTime
	for i, d := range ds[1:] {
		if d.meta.MinTime < globalMaxt {
			if len(overlappingDirs) == 0 { // When it is the first overlap, need to add the last one as well.
				overlappingDirs = append(overlappingDirs, ds[i].dir)
			}
			overlappingDirs = append(overlappingDirs, d.dir)
		} else if len(overlappingDirs) > 0 {
			break
		}
		if d.meta.MaxTime > globalMaxt {
			globalMaxt = d.meta.MaxTime
		}
	}
	return overlappingDirs
}

// splitByRange splits the directories by the time range. The range sequence starts at 0.
// splitByRange按照时间窗口对目录进行划分，range sequence从0开始
//
// For example, if we have blocks [0-10, 10-20, 50-60, 90-100] and the split range tr is 30
// it returns [0-10, 10-20], [50-60], [90-100].
// 比如，如果我们有blocks[0-10, 10-20, 50-60, 90-100]，并且分割的range tr是30，那么返回的结果是
// [0-10, 10-20], [50-60], [90-100]
func splitByRange(ds []dirMeta, tr int64) [][]dirMeta {
	var splitDirs [][]dirMeta

	for i := 0; i < len(ds); {
		var (
			group []dirMeta
			t0    int64
			m     = ds[i].meta
		)
		// Compute start of aligned time range of size tr closest to the current block's start.
		if m.MinTime >= 0 {
			t0 = tr * (m.MinTime / tr)
		} else {
			t0 = tr * ((m.MinTime - tr + 1) / tr)
		}
		// Skip blocks that don't fall into the range. This can happen via mis-alignment or
		// by being the multiple of the intended range.
		// 如果范围大于range的block就跳过
		if m.MaxTime > t0+tr {
			i++
			continue
		}

		// Add all dirs to the current group that are within [t0, t0+tr].
		for ; i < len(ds); i++ {
			// Either the block falls into the next range or doesn't fit at all (checked above).
			if ds[i].meta.MaxTime > t0+tr {
				break
			}
			group = append(group, ds[i])
		}

		if len(group) > 0 {
			// 将tr范围内的block聚合成一个block
			splitDirs = append(splitDirs, group)
		}
	}

	return splitDirs
}

// 用多BlockMeta，构建一个新的BlockMeta
func compactBlockMetas(uid ulid.ULID, blocks ...*BlockMeta) *BlockMeta {
	res := &BlockMeta{
		ULID:    uid,
		// 新的BlockMeta的MinTime就是第一个Block的MinTime
		MinTime: blocks[0].MinTime,
	}

	sources := map[ulid.ULID]struct{}{}
	// For overlapping blocks, the Maxt can be
	// in any block so we track it globally.
	maxt := int64(math.MinInt64)

	for _, b := range blocks {
		if b.MaxTime > maxt {
			maxt = b.MaxTime
		}
		if b.Compaction.Level > res.Compaction.Level {
			res.Compaction.Level = b.Compaction.Level
		}
		for _, s := range b.Compaction.Sources {
			// 记录blocks的各个sources
			sources[s] = struct{}{}
		}
		// blocks都是新的block的Parent
		res.Compaction.Parents = append(res.Compaction.Parents, BlockDesc{
			ULID:    b.ULID,
			MinTime: b.MinTime,
			MaxTime: b.MaxTime,
		})
	}
	res.Compaction.Level++

	for s := range sources {
		res.Compaction.Sources = append(res.Compaction.Sources, s)
	}
	sort.Slice(res.Compaction.Sources, func(i, j int) bool {
		return res.Compaction.Sources[i].Compare(res.Compaction.Sources[j]) < 0
	})

	// 设置MaxTime为所有blocks的maxt
	res.MaxTime = maxt
	return res
}

// Compact creates a new block in the compactor's directory from the blocks in the
// provided directories.
// Compact在compactor的目录，根据提供的directories创建一个新的block
func (c *LeveledCompactor) Compact(dest string, dirs []string, open []*Block) (uid ulid.ULID, err error) {
	var (
		blocks []BlockReader
		bs     []*Block
		metas  []*BlockMeta
		uids   []string
	)
	start := time.Now()

	for _, d := range dirs {
		meta, _, err := readMetaFile(d)
		if err != nil {
			return uid, err
		}

		var b *Block

		// Use already open blocks if we can, to avoid
		// having the index data in memory twice.
		// 使用已经打开的blocks，如果可以的话，用来避免在内存中出现两次index数据
		for _, o := range open {
			if meta.ULID == o.Meta().ULID {
				b = o
				break
			}
		}

		if b == nil {
			var err error
			b, err = OpenBlock(c.logger, d, c.chunkPool)
			if err != nil {
				return uid, err
			}
			defer b.Close()
		}

		metas = append(metas, meta)
		blocks = append(blocks, b)
		bs = append(bs, b)
		uids = append(uids, meta.ULID.String())
	}

	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	uid = ulid.MustNew(ulid.Now(), entropy)

	meta := compactBlockMetas(uid, metas...)
	err = c.write(dest, meta, blocks...)
	if err == nil {
		if meta.Stats.NumSamples == 0 {
			for _, b := range bs {
				b.meta.Compaction.Deletable = true
				n, err := writeMetaFile(c.logger, b.dir, &b.meta)
				if err != nil {
					level.Error(c.logger).Log(
						"msg", "Failed to write 'Deletable' to meta file after compaction",
						"ulid", b.meta.ULID,
					)
				}
				b.numBytesMeta = n
			}
			uid = ulid.ULID{}
			// 压缩之后导致空的block
			level.Info(c.logger).Log(
				"msg", "compact blocks resulted in empty block",
				"count", len(blocks),
				"sources", fmt.Sprintf("%v", uids),
				"duration", time.Since(start),
			)
		} else {
			level.Info(c.logger).Log(
				"msg", "compact blocks",
				"count", len(blocks),
				"mint", meta.MinTime,
				"maxt", meta.MaxTime,
				"ulid", meta.ULID,
				"sources", fmt.Sprintf("%v", uids),
				"duration", time.Since(start),
			)
		}
		return uid, nil
	}

	var merr tsdb_errors.MultiError
	merr.Add(err)
	if err != context.Canceled {
		for _, b := range bs {
			if err := b.setCompactionFailed(); err != nil {
				merr.Add(errors.Wrapf(err, "setting compaction failed for block: %s", b.Dir()))
			}
		}
	}

	return uid, merr
}

func (c *LeveledCompactor) Write(dest string, b BlockReader, mint, maxt int64, parent *BlockMeta) (ulid.ULID, error) {
	start := time.Now()

	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	// 创建一个新的uid
	uid := ulid.MustNew(ulid.Now(), entropy)

	// BlockMeta指定了Block的ULID以及这个Block最大最小时间
	meta := &BlockMeta{
		ULID:    uid,
		MinTime: mint,
		MaxTime: maxt,
	}
	// 新建的block，压缩级别为1
	meta.Compaction.Level = 1
	meta.Compaction.Sources = []ulid.ULID{uid}

	if parent != nil {
		// parent不为nil，指定parent的信息
		meta.Compaction.Parents = []BlockDesc{
			{ULID: parent.ULID, MinTime: parent.MinTime, MaxTime: parent.MaxTime},
		}
	}

	// 将blocks写入
	err := c.write(dest, meta, b)
	if err != nil {
		return uid, err
	}

	if meta.Stats.NumSamples == 0 {
		// 如果没有写入samples，返回空
		return ulid.ULID{}, nil
	}

	// 写入了block
	level.Info(c.logger).Log(
		"msg", "write block",
		"mint", meta.MinTime,
		"maxt", meta.MaxTime,
		"ulid", meta.ULID,
		"duration", time.Since(start),
	)
	return uid, nil
}

// instrumentedChunkWriter is used for level 1 compactions to record statistics
// about compacted chunks.
// instrumentedChunkWriter是用在第一层的压缩用于记录压缩的chunks的数据
type instrumentedChunkWriter struct {
	ChunkWriter

	size    prometheus.Histogram
	samples prometheus.Histogram
	trange  prometheus.Histogram
}

func (w *instrumentedChunkWriter) WriteChunks(chunks ...chunks.Meta) error {
	for _, c := range chunks {
		w.size.Observe(float64(len(c.Chunk.Bytes())))
		w.samples.Observe(float64(c.Chunk.NumSamples()))
		w.trange.Observe(float64(c.MaxTime - c.MinTime))
	}
	return w.ChunkWriter.WriteChunks(chunks...)
}

// write creates a new block that is the union of the provided blocks into dir.
// It cleans up all files of the old blocks after completing successfully.
// write创建一个新的block，它将提供的blocks合并到dir中，它在全部完成之后清除老的blocks的所有文件
func (c *LeveledCompactor) write(dest string, meta *BlockMeta, blocks ...BlockReader) (err error) {
	dir := filepath.Join(dest, meta.ULID.String())
	tmp := dir + ".tmp"
	var closers []io.Closer
	defer func(t time.Time) {
		var merr tsdb_errors.MultiError
		merr.Add(err)
		merr.Add(closeAll(closers))
		err = merr.Err()

		// RemoveAll returns no error when tmp doesn't exist so it is safe to always run it.
		// 当tmp不存在的时候，RemoveAll不会返回错误，因此总是调用它都是安全的
		if err := os.RemoveAll(tmp); err != nil {
			level.Error(c.logger).Log("msg", "removed tmp folder after failed compaction", "err", err.Error())
		}
		c.metrics.ran.Inc()
		c.metrics.duration.Observe(time.Since(t).Seconds())
	}(time.Now())

	// 移除可能存在的tmp文件
	if err = os.RemoveAll(tmp); err != nil {
		return err
	}

	if err = os.MkdirAll(tmp, 0777); err != nil {
		return err
	}

	// Populate chunk and index files into temporary directory with
	// data of all blocks.
	// 将所有block的数据写入tmp文件夹的chunk以及index file中
	var chunkw ChunkWriter

	// 创建一个chunk writer
	chunkw, err = chunks.NewWriter(chunkDir(tmp))
	if err != nil {
		return errors.Wrap(err, "open chunk writer")
	}
	closers = append(closers, chunkw)
	// Record written chunk sizes on level 1 compactions.
	// 当压缩的级别为level 1时，记录写入的chunk size
	if meta.Compaction.Level == 1 {
		chunkw = &instrumentedChunkWriter{
			ChunkWriter: chunkw,
			size:        c.metrics.chunkSize,
			samples:     c.metrics.chunkSamples,
			trange:      c.metrics.chunkRange,
		}
	}

	// 构建index writer
	indexw, err := index.NewWriter(filepath.Join(tmp, indexFilename))
	if err != nil {
		return errors.Wrap(err, "open index writer")
	}
	closers = append(closers, indexw)

	// 填充block
	if err := c.populateBlock(blocks, meta, indexw, chunkw); err != nil {
		return errors.Wrap(err, "write compaction")
	}

	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	default:
	}

	// We are explicitly closing them here to check for error even
	// though these are covered under defer. This is because in Windows,
	// you cannot delete these unless they are closed and the defer is to
	// make sure they are closed if the function exits due to an error above.
	var merr tsdb_errors.MultiError
	for _, w := range closers {
		merr.Add(w.Close())
	}
	closers = closers[:0] // Avoid closing the writers twice in the defer.
	if merr.Err() != nil {
		return merr.Err()
	}

	// Populated block is empty, so exit early.
	// 如果填充的block为空，则直接早点返回
	if meta.Stats.NumSamples == 0 {
		return nil
	}

	// 写入元数据
	if _, err = writeMetaFile(c.logger, tmp, meta); err != nil {
		return errors.Wrap(err, "write merged meta")
	}

	// Create an empty tombstones file.
	// 创建一个新的tombstones文件
	if _, err := writeTombstoneFile(c.logger, tmp, newMemTombstones()); err != nil {
		return errors.Wrap(err, "write new tombstones file")
	}

	df, err := fileutil.OpenDir(tmp)
	if err != nil {
		return errors.Wrap(err, "open temporary block dir")
	}
	defer func() {
		if df != nil {
			df.Close()
		}
	}()

	// 对临时的block目录进行同步
	if err := df.Sync(); err != nil {
		return errors.Wrap(err, "sync temporary dir file")
	}

	// Close temp dir before rename block dir (for windows platform).
	// 在重命名block目录之前，关闭临时目录
	if err = df.Close(); err != nil {
		return errors.Wrap(err, "close temporary dir")
	}
	df = nil

	// Block successfully written, make visible and remove old ones.
	// Block被成功写入，让它visible并且移除老的block
	if err := fileutil.Replace(tmp, dir); err != nil {
		return errors.Wrap(err, "rename block dir")
	}

	return nil
}

// populateBlock fills the index and chunk writers with new data gathered as the union
// of the provided blocks. It returns meta information for the new block.
// It expects sorted blocks input by mint.
// populateBlock用provided blocks提供的数据填充index以及chunk writers，它返回新的block的元数据，它期望提供的blocks按照mint进行排序
func (c *LeveledCompactor) populateBlock(blocks []BlockReader, meta *BlockMeta, indexw IndexWriter, chunkw ChunkWriter) (err error) {
	if len(blocks) == 0 {
		return errors.New("cannot populate block from no readers")
	}

	var (
		set         ChunkSeriesSet
		allSymbols  = make(map[string]struct{}, 1<<16)
		closers     = []io.Closer{}
		overlapping bool
	)
	defer func() {
		var merr tsdb_errors.MultiError
		merr.Add(err)
		merr.Add(closeAll(closers))
		err = merr.Err()
		c.metrics.populatingBlocks.Set(0)
	}()
	c.metrics.populatingBlocks.Set(1)

	globalMaxt := blocks[0].MaxTime()
	// 合并多个block的series，indexer和tombstone
	// 压缩head时，仅仅只有一个block
	for i, b := range blocks {
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		default:
		}

		if !overlapping {
			if i > 0 && b.MinTime() < globalMaxt {
				c.metrics.overlappingBlocks.Inc()
				overlapping = true
				// 在压缩的时候发现了有重合的blocks
				level.Warn(c.logger).Log("msg", "found overlapping blocks during compaction", "ulid", meta.ULID)
			}
			if b.MaxTime() > globalMaxt {
				// 全局最大的max time
				globalMaxt = b.MaxTime()
			}
		}

		// 构建各种index, chunk以及tombstone reader
		indexr, err := b.Index()
		if err != nil {
			return errors.Wrapf(err, "open index reader for block %s", b)
		}
		closers = append(closers, indexr)

		chunkr, err := b.Chunks()
		if err != nil {
			return errors.Wrapf(err, "open chunk reader for block %s", b)
		}
		closers = append(closers, chunkr)

		tombsr, err := b.Tombstones()
		if err != nil {
			return errors.Wrapf(err, "open tombstone reader for block %s", b)
		}
		closers = append(closers, tombsr)

		symbols, err := indexr.Symbols()
		if err != nil {
			return errors.Wrap(err, "read symbols")
		}
		// 统一存放所有的symbols
		for s := range symbols {
			allSymbols[s] = struct{}{}
		}

		// 获取到所有的id
		all, err := indexr.Postings(index.AllPostingsKey())
		if err != nil {
			return err
		}
		all = indexr.SortedPostings(all)

		// 新的压缩的series set
		s := newCompactionSeriesSet(indexr, chunkr, tombsr, all)

		if i == 0 {
			// 如果i为0，则直接赋值为set，直接返回
			set = s
			continue
		}
		set, err = newCompactionMerger(set, s)
		if err != nil {
			return err
		}
	}

	// We fully rebuild the postings list index from merged series.
	// 从merged series重新构建posting list index
	var (
		// 重建一个新的mempostings
		postings = index.NewMemPostings()
		values   = map[string]stringset{}
		i        = uint64(0)
	)

	// 首先写入所有的symbols
	if err := indexw.AddSymbols(allSymbols); err != nil {
		return errors.Wrap(err, "add symbols")
	}

	// 找到下一个id的chunks等等
	for set.Next() {
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		default:
		}

		// 这里的chunks是没有被完全删除的
		lset, chks, dranges := set.At() // The chunks here are not fully deleted.
		if overlapping {
			// If blocks are overlapping, it is possible to have unsorted chunks.
			// 如果blocks有交叉，则可能没有排好序的chunks
			sort.Slice(chks, func(i, j int) bool {
				return chks[i].MinTime < chks[j].MinTime
			})
		}

		// Skip the series with all deleted chunks.
		// 如果所有的chunks都删除了，则跳过
		if len(chks) == 0 {
			continue
		}

		for i, chk := range chks {
			// Re-encode head chunks that are still open (being appended to) or
			// outside the compacted MaxTime range.
			// The chunk.Bytes() method is not safe for open chunks hence the re-encoding.
			// This happens when snapshotting the head block.
			//
			// Block time range is half-open: [meta.MinTime, meta.MaxTime) and
			// chunks are closed hence the chk.MaxTime >= meta.MaxTime check.
			//
			// TODO think how to avoid the typecasting to verify when it is head block.
			if _, isHeadChunk := chk.Chunk.(*safeChunk); isHeadChunk && chk.MaxTime >= meta.MaxTime {
				dranges = append(dranges, Interval{Mint: meta.MaxTime, Maxt: math.MaxInt64})

			} else
			// Sanity check for disk blocks.
			// chk.MaxTime == meta.MaxTime shouldn't happen as well, but will brake many users so not checking for that.
			if chk.MinTime < meta.MinTime || chk.MaxTime > meta.MaxTime {
				return errors.Errorf("found chunk with minTime: %d maxTime: %d outside of compacted minTime: %d maxTime: %d",
					chk.MinTime, chk.MaxTime, meta.MinTime, meta.MaxTime)
			}

			if len(dranges) > 0 {
				// Re-encode the chunk to not have deleted values.
				if !chk.OverlapsClosedInterval(dranges[0].Mint, dranges[len(dranges)-1].Maxt) {
					continue
				}
				newChunk := chunkenc.NewXORChunk()
				app, err := newChunk.Appender()
				if err != nil {
					return err
				}

				it := &deletedIterator{it: chk.Chunk.Iterator(), intervals: dranges}

				var (
					t int64
					v float64
				)
				for it.Next() {
					t, v = it.At()
					app.Append(t, v)
				}
				if err := it.Err(); err != nil {
					return errors.Wrap(err, "iterate chunk while re-encoding")
				}

				chks[i].Chunk = newChunk
				chks[i].MaxTime = t
			}
		}

		// 经过处理的chunks变为mergedChks
		mergedChks := chks
		if overlapping {
			mergedChks, err = chunks.MergeOverlappingChunks(chks)
			if err != nil {
				return errors.Wrap(err, "merge overlapping chunks")
			}
		}
		// 真正写入chunk
		if err := chunkw.WriteChunks(mergedChks...); err != nil {
			return errors.Wrap(err, "write chunks")
		}

		// 在index中写入series
		if err := indexw.AddSeries(i, lset, mergedChks...); err != nil {
			return errors.Wrap(err, "add series")
		}

		// 写入chunk的数目
		meta.Stats.NumChunks += uint64(len(mergedChks))
		// 一个series的所有chunk都写入了
		meta.Stats.NumSeries++
		for _, chk := range mergedChks {
			meta.Stats.NumSamples += uint64(chk.Chunk.NumSamples())
		}

		for _, chk := range mergedChks {
			// 循环利用chunks
			if err := c.chunkPool.Put(chk.Chunk); err != nil {
				return errors.Wrap(err, "put chunk")
			}
		}

		// values关联所有label name相关的values
		for _, l := range lset {
			valset, ok := values[l.Name]
			if !ok {
				valset = stringset{}
				values[l.Name] = valset
			}
			valset.set(l.Value)
		}
		// 重新构建series ID和它的label之间的索引
		postings.Add(i, lset)

		i++
	}
	if set.Err() != nil {
		return errors.Wrap(set.Err(), "iterate compaction set")
	}

	s := make([]string, 0, 256)
	for n, v := range values {
		s = s[:0]

		for x := range v {
			// 将value扩展到s
			s = append(s, x)
		}
		// 写入label name和相关的value
		if err := indexw.WriteLabelIndex([]string{n}, s); err != nil {
			return errors.Wrap(err, "write label index")
		}
	}

	for _, l := range postings.SortedKeys() {
		// 将postings写入index文件
		// 写入一组label，以及和它相关的一串series id
		if err := indexw.WritePostings(l.Name, l.Value, postings.Get(l.Name, l.Value)); err != nil {
			return errors.Wrap(err, "write postings")
		}
	}
	return nil
}

type compactionSeriesSet struct {
	p          index.Postings
	index      IndexReader
	chunks     ChunkReader
	tombstones TombstoneReader

	l         labels.Labels
	c         []chunks.Meta
	intervals Intervals
	err       error
}

func newCompactionSeriesSet(i IndexReader, c ChunkReader, t TombstoneReader, p index.Postings) *compactionSeriesSet {
	return &compactionSeriesSet{
		index:      i,
		chunks:     c,
		tombstones: t,
		p:          p,
	}
}

func (c *compactionSeriesSet) Next() bool {
	// 获取下一个series id
	if !c.p.Next() {
		// 所有id都遍历完毕
		return false
	}
	var err error

	c.intervals, err = c.tombstones.Get(c.p.At())
	if err != nil {
		c.err = errors.Wrap(err, "get tombstones")
		return false
	}

	// 根据id填充对应的label和chunk
	if err = c.index.Series(c.p.At(), &c.l, &c.c); err != nil {
		c.err = errors.Wrapf(err, "get series %d", c.p.At())
		return false
	}

	// Remove completely deleted chunks.
	// 移除完全删除的chunks
	if len(c.intervals) > 0 {
		chks := make([]chunks.Meta, 0, len(c.c))
		for _, chk := range c.c {
			if !(Interval{chk.MinTime, chk.MaxTime}.isSubrange(c.intervals)) {
				chks = append(chks, chk)
			}
		}

		c.c = chks
	}

	for i := range c.c {
		// chk是chunk的元数据
		chk := &c.c[i]

		// 利用c.chunks接口以及chk.Ref找到对应的chunk
		chk.Chunk, err = c.chunks.Chunk(chk.Ref)
		if err != nil {
			c.err = errors.Wrapf(err, "chunk %d not found", chk.Ref)
			return false
		}
	}

	return true
}

func (c *compactionSeriesSet) Err() error {
	if c.err != nil {
		return c.err
	}
	return c.p.Err()
}

func (c *compactionSeriesSet) At() (labels.Labels, []chunks.Meta, Intervals) {
	return c.l, c.c, c.intervals
}

type compactionMerger struct {
	a, b ChunkSeriesSet

	aok, bok  bool
	l         labels.Labels
	c         []chunks.Meta
	intervals Intervals
}

func newCompactionMerger(a, b ChunkSeriesSet) (*compactionMerger, error) {
	c := &compactionMerger{
		a: a,
		b: b,
	}
	// Initialize first elements of both sets as Next() needs
	// one element look-ahead.
	// 初始化两个sets的第一个elements
	c.aok = c.a.Next()
	c.bok = c.b.Next()

	return c, c.Err()
}

func (c *compactionMerger) compare() int {
	if !c.aok {
		return 1
	}
	if !c.bok {
		return -1
	}
	a, _, _ := c.a.At()
	b, _, _ := c.b.At()
	return labels.Compare(a, b)
}

// Next()填充下一个要用的labels以及[]chunks.Meta
func (c *compactionMerger) Next() bool {
	if !c.aok && !c.bok || c.Err() != nil {
		return false
	}
	// While advancing child iterators the memory used for labels and chunks
	// may be reused. When picking a series we have to store the result.
	var lset labels.Labels
	var chks []chunks.Meta

	d := c.compare()
	if d > 0 {
		lset, chks, c.intervals = c.b.At()
		c.l = append(c.l[:0], lset...)
		c.c = append(c.c[:0], chks...)

		c.bok = c.b.Next()
	} else if d < 0 {
		lset, chks, c.intervals = c.a.At()
		c.l = append(c.l[:0], lset...)
		c.c = append(c.c[:0], chks...)

		c.aok = c.a.Next()
	} else {
		// Both sets contain the current series. Chain them into a single one.
		// 两个set都包含当前的series，将它们连接成一个
		l, ca, ra := c.a.At()
		_, cb, rb := c.b.At()

		for _, r := range rb {
			ra = ra.add(r)
		}

		c.l = append(c.l[:0], l...)
		// 将一致的chunks合并起来
		c.c = append(append(c.c[:0], ca...), cb...)
		c.intervals = ra

		c.aok = c.a.Next()
		c.bok = c.b.Next()
	}

	return true
}

func (c *compactionMerger) Err() error {
	if c.a.Err() != nil {
		return c.a.Err()
	}
	return c.b.Err()
}

func (c *compactionMerger) At() (labels.Labels, []chunks.Meta, Intervals) {
	return c.l, c.c, c.intervals
}
