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

// Package tsdb implements a time series storage for float64 sample data.
package tsdb

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb/chunkenc"
	tsdb_errors "github.com/prometheus/tsdb/errors"
	"github.com/prometheus/tsdb/fileutil"
	_ "github.com/prometheus/tsdb/goversion"
	"github.com/prometheus/tsdb/labels"
	"github.com/prometheus/tsdb/wal"
	"golang.org/x/sync/errgroup"
)

// DefaultOptions used for the DB. They are sane for setups using
// millisecond precision timestamps.
var DefaultOptions = &Options{
	WALSegmentSize:         wal.DefaultSegmentSize,
	RetentionDuration:      15 * 24 * 60 * 60 * 1000, // 15 days in milliseconds
	BlockRanges:            ExponentialBlockRanges(int64(2*time.Hour)/1e6, 3, 5),
	NoLockfile:             false,
	AllowOverlappingBlocks: false,
	WALCompression:         false,
}

// Options of the DB storage.
type Options struct {
	// Segments (wal files) max size.
	// WALSegmentSize = 0, segment size is default size.
	// WALSegmentSize > 0, segment size is WALSegmentSize.
	// WALSegmentSize < 0, wal is disabled.
	WALSegmentSize int

	// Duration of persisted data to keep.
	// 持久化数据保存的时长
	RetentionDuration uint64

	// Maximum number of bytes in blocks to be retained.
	// 0 or less means disabled.
	// NOTE: For proper storage calculations need to consider
	// the size of the WAL folder which is not added when calculating
	// the current size of the database.
	// MaxBytes是保存的blocks的字节数
	MaxBytes int64

	// The sizes of the Blocks.
	// blocks的大小
	BlockRanges []int64

	// NoLockfile disables creation and consideration of a lock file.
	NoLockfile bool

	// Overlapping blocks are allowed if AllowOverlappingBlocks is true.
	// This in-turn enables vertical compaction and vertical query merge.
	AllowOverlappingBlocks bool

	// WALCompression will turn on Snappy compression for records on the WAL.
	WALCompression bool
}

// Appender allows appending a batch of data. It must be completed with a
// call to Commit or Rollback and must not be reused afterwards.
//
// Operations on the Appender interface are not goroutine-safe.
type Appender interface {
	// Add adds a sample pair for the given series. A reference number is
	// returned which can be used to add further samples in the same or later
	// transactions.
	// Returned reference numbers are ephemeral and may be rejected in calls
	// to AddFast() at any point. Adding the sample via Add() returns a new
	// reference number.
	// If the reference is 0 it must not be used for caching.
	Add(l labels.Labels, t int64, v float64) (uint64, error)

	// AddFast adds a sample pair for the referenced series. It is generally
	// faster than adding a sample by providing its full label set.
	AddFast(ref uint64, t int64, v float64) error

	// Commit submits the collected samples and purges the batch.
	Commit() error

	// Rollback rolls back all modifications made in the appender so far.
	Rollback() error
}

// DB handles reads and writes of time series falling into
// a hashed partition of a seriedb.
type DB struct {
	dir   string
	lockf fileutil.Releaser

	logger    log.Logger
	metrics   *dbMetrics
	opts      *Options
	chunkPool chunkenc.Pool
	compactor Compactor

	// Mutex for that must be held when modifying the general block layout.
	mtx    sync.RWMutex
	blocks []*Block

	head *Head

	compactc chan struct{}
	donec    chan struct{}
	stopc    chan struct{}

	// cmtx ensures that compactions and deletions don't run simultaneously.
	cmtx sync.Mutex

	// autoCompactMtx ensures that no compaction gets triggered while
	// changing the autoCompact var.
	autoCompactMtx sync.Mutex
	autoCompact    bool

	// Cancel a running compaction when a shutdown is initiated.
	compactCancel context.CancelFunc
}

type dbMetrics struct {
	loadedBlocks         prometheus.GaugeFunc
	symbolTableSize      prometheus.GaugeFunc
	reloads              prometheus.Counter
	reloadsFailed        prometheus.Counter
	compactionsTriggered prometheus.Counter
	compactionsFailed    prometheus.Counter
	timeRetentionCount   prometheus.Counter
	compactionsSkipped   prometheus.Counter
	startTime            prometheus.GaugeFunc
	tombCleanTimer       prometheus.Histogram
	blocksBytes          prometheus.Gauge
	sizeRetentionCount   prometheus.Counter
}

func newDBMetrics(db *DB, r prometheus.Registerer) *dbMetrics {
	m := &dbMetrics{}

	m.loadedBlocks = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_blocks_loaded",
		Help: "Number of currently loaded data blocks",
	}, func() float64 {
		db.mtx.RLock()
		defer db.mtx.RUnlock()
		return float64(len(db.blocks))
	})
	m.symbolTableSize = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_symbol_table_size_bytes",
		Help: "Size of symbol table on disk (in bytes)",
	}, func() float64 {
		db.mtx.RLock()
		blocks := db.blocks[:]
		db.mtx.RUnlock()
		symTblSize := uint64(0)
		for _, b := range blocks {
			symTblSize += b.GetSymbolTableSize()
		}
		return float64(symTblSize)
	})
	m.reloads = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_reloads_total",
		Help: "Number of times the database reloaded block data from disk.",
	})
	m.reloadsFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_reloads_failures_total",
		Help: "Number of times the database failed to reload block data from disk.",
	})
	m.compactionsTriggered = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_compactions_triggered_total",
		Help: "Total number of triggered compactions for the partition.",
	})
	m.compactionsFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_compactions_failed_total",
		Help: "Total number of compactions that failed for the partition.",
	})
	m.timeRetentionCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_time_retentions_total",
		// 因为maximum time limit达到而被删除的blocks的数目
		Help: "The number of times that blocks were deleted because the maximum time limit was exceeded.",
	})
	m.compactionsSkipped = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_compactions_skipped_total",
		Help: "Total number of skipped compactions due to disabled auto compaction.",
	})
	m.startTime = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_lowest_timestamp",
		Help: "Lowest timestamp value stored in the database. The unit is decided by the library consumer.",
	}, func() float64 {
		db.mtx.RLock()
		defer db.mtx.RUnlock()
		if len(db.blocks) == 0 {
			return float64(db.head.minTime)
		}
		return float64(db.blocks[0].meta.MinTime)
	})
	m.tombCleanTimer = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "prometheus_tsdb_tombstone_cleanup_seconds",
		Help: "The time taken to recompact blocks to remove tombstones.",
	})
	m.blocksBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_storage_blocks_bytes",
		Help: "The number of bytes that are currently used for local storage by all blocks.",
	})
	m.sizeRetentionCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_size_retentions_total",
		Help: "The number of times that blocks were deleted because the maximum number of bytes was exceeded.",
	})

	if r != nil {
		r.MustRegister(
			m.loadedBlocks,
			m.symbolTableSize,
			m.reloads,
			m.reloadsFailed,
			m.timeRetentionCount,
			m.compactionsTriggered,
			m.compactionsFailed,
			m.startTime,
			m.tombCleanTimer,
			m.blocksBytes,
			m.sizeRetentionCount,
		)
	}
	return m
}

// Open returns a new DB in the given directory.
func Open(dir string, l log.Logger, r prometheus.Registerer, opts *Options) (db *DB, err error) {
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, err
	}
	if l == nil {
		l = log.NewNopLogger()
	}
	if opts == nil {
		opts = DefaultOptions
	}
	// Fixup bad format written by Prometheus 2.1.
	if err := repairBadIndexVersion(l, dir); err != nil {
		return nil, err
	}
	// Migrate old WAL if one exists.
	// 转换老的格式的WAL
	if err := MigrateWAL(l, filepath.Join(dir, "wal")); err != nil {
		return nil, errors.Wrap(err, "migrate WAL")
	}

	db = &DB{
		dir:         dir,
		logger:      l,
		opts:        opts,
		compactc:    make(chan struct{}, 1),
		donec:       make(chan struct{}),
		stopc:       make(chan struct{}),
		// autoCompact为true
		autoCompact: true,
		chunkPool:   chunkenc.NewPool(),
	}
	db.metrics = newDBMetrics(db, r)

	if !opts.NoLockfile {
		absdir, err := filepath.Abs(dir)
		if err != nil {
			return nil, err
		}
		lockf, _, err := fileutil.Flock(filepath.Join(absdir, "lock"))
		if err != nil {
			return nil, errors.Wrap(err, "lock DB directory")
		}
		db.lockf = lockf
	}

	ctx, cancel := context.WithCancel(context.Background())
	// 创建compactor
	db.compactor, err = NewLeveledCompactor(ctx, r, l, opts.BlockRanges, db.chunkPool)
	if err != nil {
		cancel()
		return nil, errors.Wrap(err, "create leveled compactor")
	}
	db.compactCancel = cancel

	var wlog *wal.WAL
	segmentSize := wal.DefaultSegmentSize
	// Wal is enabled.
	if opts.WALSegmentSize >= 0 {
		// Wal is set to a custom size.
		if opts.WALSegmentSize > 0 {
			segmentSize = opts.WALSegmentSize
		}
		// 创建wal
		wlog, err = wal.NewSize(l, r, filepath.Join(dir, "wal"), segmentSize, opts.WALCompression)
		if err != nil {
			return nil, err
		}
	}

	// 创建head
	db.head, err = NewHead(r, l, wlog, opts.BlockRanges[0])
	if err != nil {
		return nil, err
	}

	// 重新加载db
	if err := db.reload(); err != nil {
		return nil, err
	}
	// Set the min valid time for the ingested samples
	// to be no lower than the maxt of the last block.
	// 设置摄入的samples的min valid time并且这个时间不能比上一个block的maxt低
	blocks := db.Blocks()
	minValidTime := int64(math.MinInt64)
	if len(blocks) > 0 {
		// 将minValidTime设置成上一个block的MaxTime
		minValidTime = blocks[len(blocks)-1].Meta().MaxTime
	}

	// 初始化head
	if initErr := db.head.Init(minValidTime); initErr != nil {
		db.head.metrics.walCorruptionsTotal.Inc()
		level.Warn(db.logger).Log("msg", "encountered WAL read error, attempting repair", "err", err)
		// 对损坏的WAL进行修复
		if err := wlog.Repair(initErr); err != nil {
			return nil, errors.Wrap(err, "repair corrupted WAL")
		}
	}

	// db.run()主要是针对触发自动压缩
	go db.run()

	return db, nil
}

// Dir returns the directory of the database.
func (db *DB) Dir() string {
	return db.dir
}

func (db *DB) run() {
	defer close(db.donec)

	backoff := time.Duration(0)

	for {
		select {
		case <-db.stopc:
			return
		case <-time.After(backoff):
		}

		// 实际触发的时间是2 * backoff + 1 Minute
		select {
		case <-time.After(1 * time.Minute):
			select {
			// 每隔1分钟触发一次compact
			case db.compactc <- struct{}{}:
			default:
			}
		case <-db.compactc:
			db.metrics.compactionsTriggered.Inc()

			db.autoCompactMtx.Lock()
			if db.autoCompact {
				// 对于db全局的压缩
				if err := db.compact(); err != nil {
					level.Error(db.logger).Log("msg", "compaction failed", "err", err)
					backoff = exponential(backoff, 1*time.Second, 1*time.Minute)
				} else {
					backoff = 0
				}
			} else {
				db.metrics.compactionsSkipped.Inc()
			}
			db.autoCompactMtx.Unlock()
		case <-db.stopc:
			return
		}
	}
}

// Appender opens a new appender against the database.
// Appender对数据库打开一个新的appender
func (db *DB) Appender() Appender {
	return dbAppender{db: db, Appender: db.head.Appender()}
}

// dbAppender wraps the DB's head appender and triggers compactions on commit
// if necessary.
// dbAppender封装了DB的header appender并且在commit的时候触发压缩，如果有必要的话
type dbAppender struct {
	Appender
	db *DB
}

func (a dbAppender) Commit() error {
	err := a.Appender.Commit()

	// We could just run this check every few minutes practically. But for benchmarks
	// and high frequency use cases this is the safer way.
	// 我们几乎每几分钟运行一次check，但是对于benchmark或者高频率的场景来说，这是更安全的方式
	if a.db.head.compactable() {
		select {
		case a.db.compactc <- struct{}{}:
		default:
		}
	}
	return err
}

// Compact data if possible. After successful compaction blocks are reloaded
// which will also trigger blocks to be deleted that fall out of the retention
// window.
// 如果可能的话对数据进行压缩，在成功压缩之后，blocks会被重载，它会触发超出时间窗口以外的blocks被删除
// If no blocks are compacted, the retention window state doesn't change. Thus,
// this is sufficient to reliably delete old data.
// 如果没有数据被压缩，则压缩窗口的状态不会改变，因此，这对于有效地删除老的数据是足够的了
// Old blocks are only deleted on reload based on the new block's parent information.
// See DB.reload documentation for further information.
// Old blocks只有在reload的时候基于新的block的parent information才会被删除
func (db *DB) compact() (err error) {
	db.cmtx.Lock()
	defer db.cmtx.Unlock()
	defer func() {
		if err != nil {
			db.metrics.compactionsFailed.Inc()
		}
	}()
	// Check whether we have pending head blocks that are ready to be persisted.
	// They have the highest priority.
	// 检查我们是否有pending head blocks等待被持久化，它们有着最高的优先级
	for {
		select {
		case <-db.stopc:
			return nil
		default:
		}
		if !db.head.compactable() {
			break
		}
		mint := db.head.MinTime()
		maxt := rangeForTimestamp(mint, db.head.chunkRange)

		// Wrap head into a range that bounds all reads to it.
		head := &rangeHead{
			head: db.head,
			mint: mint,
			// We remove 1 millisecond from maxt because block
			// intervals are half-open: [b.MinTime, b.MaxTime). But
			// chunk intervals are closed: [c.MinTime, c.MaxTime];
			// so in order to make sure that overlaps are evaluated
			// consistently, we explicitly remove the last value
			// from the block interval here.
			maxt: maxt - 1,
		}
		// 对head block进行持久化
		uid, err := db.compactor.Write(db.dir, head, mint, maxt, nil)
		if err != nil {
			return errors.Wrap(err, "persist head block")
		}

		runtime.GC()

		// 持久化之后立刻进行reload
		if err := db.reload(); err != nil {
			if err := os.RemoveAll(filepath.Join(db.dir, uid.String())); err != nil {
				return errors.Wrapf(err, "delete persisted head block after failed db reload:%s", uid)
			}
			return errors.Wrap(err, "reload blocks")
		}
		if (uid == ulid.ULID{}) {
			// Compaction resulted in an empty block.
			// Compaction造成了一个空的block
			// Head truncating during db.reload() depends on the persisted blocks and
			// in this case no new block will be persisted so manually truncate the head.
			// db.reload()依赖于persisted blocks并且在这种情况下没有新的block被持久化，因此手动截取head
			if err = db.head.Truncate(maxt); err != nil {
				return errors.Wrap(err, "head truncate failed (in compact)")
			}
		}
		runtime.GC()
	}

	// Check for compactions of multiple blocks.
	// 检查多个blocks的压缩
	for {
		plan, err := db.compactor.Plan(db.dir)
		if err != nil {
			return errors.Wrap(err, "plan compaction")
		}
		if len(plan) == 0 {
			break
		}

		select {
		case <-db.stopc:
			return nil
		default:
		}

		uid, err := db.compactor.Compact(db.dir, plan, db.blocks)
		if err != nil {
			return errors.Wrapf(err, "compact %s", plan)
		}
		runtime.GC()

		if err := db.reload(); err != nil {
			if err := os.RemoveAll(filepath.Join(db.dir, uid.String())); err != nil {
				return errors.Wrapf(err, "delete compacted block after failed db reload:%s", uid)
			}
			return errors.Wrap(err, "reload blocks")
		}
		runtime.GC()
	}

	return nil
}

func (db *DB) getBlock(id ulid.ULID) (*Block, bool) {
	for _, b := range db.blocks {
		if b.Meta().ULID == id {
			return b, true
		}
	}
	return nil, false
}

// reload blocks and trigger head truncation if new blocks appeared.
// Blocks that are obsolete due to replacement or retention will be deleted.
// 如果有新的blocks出现，reload会阻塞并且触发head truncation
// Blocks因为replacement或者retention而过时的会被删除
// 重新加载block并且移除可以移除的block
func (db *DB) reload() (err error) {
	defer func() {
		if err != nil {
			db.metrics.reloadsFailed.Inc()
		}
		db.metrics.reloads.Inc()
	}()

	loadable, corrupted, err := db.openBlocks()
	if err != nil {
		return err
	}

	deletable := db.deletableBlocks(loadable)

	// Corrupted blocks that have been replaced by parents can be safely ignored and deleted.
	// This makes it resilient against the process crashing towards the end of a compaction.
	// Creation of a new block and deletion of its parents cannot happen atomically.
	// By creating blocks with their parents, we can pick up the deletion where it left off during a crash.
	for _, block := range loadable {
		for _, b := range block.Meta().Compaction.Parents {
			// 如果block是可加载的，则它的parent可以从corrupted以及deletable中移除
			delete(corrupted, b.ULID)
			deletable[b.ULID] = nil
		}
	}
	if len(corrupted) > 0 {
		// Close all new blocks to release the lock for windows.
		for _, block := range loadable {
			if _, loaded := db.getBlock(block.Meta().ULID); !loaded {
				// 如果loadable中的block没有被加载，则直接关闭
				block.Close()
			}
		}
		return fmt.Errorf("unexpected corrupted block:%v", corrupted)
	}

	// All deletable blocks should not be loaded.
	// 所有可以被移除的blockd都不应该被加载
	var (
		bb         []*Block
		blocksSize int64
	)
	for _, block := range loadable {
		if _, ok := deletable[block.Meta().ULID]; ok {
			// 如果ULID相同，则更新deletable中的block
			deletable[block.Meta().ULID] = block
			continue
		}
		bb = append(bb, block)
		blocksSize += block.Size()

	}
	loadable = bb
	db.metrics.blocksBytes.Set(float64(blocksSize))

	// 将block从老到新进行排序
	sort.Slice(loadable, func(i, j int) bool {
		return loadable[i].Meta().MinTime < loadable[j].Meta().MinTime
	})
	if !db.opts.AllowOverlappingBlocks {
		if err := validateBlockSequence(loadable); err != nil {
			return errors.Wrap(err, "invalid block sequence")
		}
	}

	// Swap new blocks first for subsequently created readers to be seen.
	// 首先交换blocks，让之后创建的reader能够看到
	db.mtx.Lock()
	oldBlocks := db.blocks
	db.blocks = loadable
	db.mtx.Unlock()

	blockMetas := make([]BlockMeta, 0, len(loadable))
	for _, b := range loadable {
		blockMetas = append(blockMetas, b.Meta())
	}
	if overlaps := OverlappingBlocks(blockMetas); len(overlaps) > 0 {
		level.Warn(db.logger).Log("msg", "overlapping blocks found during reload", "detail", overlaps.String())
	}

	for _, b := range oldBlocks {
		if _, ok := deletable[b.Meta().ULID]; ok {
			// 用oldBlocks更新deletable
			deletable[b.Meta().ULID] = b
		}
	}

	if err := db.deleteBlocks(deletable); err != nil {
		return err
	}

	// Garbage collect data in the head if the most recent persisted block
	// covers data of its current time range.
	// GC head中的数据，如果最近持久化的block覆盖了它当前的time range的数据
	if len(loadable) == 0 {
		return nil
	}

	maxt := loadable[len(loadable)-1].Meta().MaxTime

	// 根据最新持久化的块的maxt来对head进行截取
	return errors.Wrap(db.head.Truncate(maxt), "head truncate failed")
}

func (db *DB) openBlocks() (blocks []*Block, corrupted map[ulid.ULID]error, err error) {
	dirs, err := blockDirs(db.dir)
	if err != nil {
		return nil, nil, errors.Wrap(err, "find blocks")
	}

	corrupted = make(map[ulid.ULID]error)
	for _, dir := range dirs {
		// 从block dir中读取元数据
		meta, _, err := readMetaFile(dir)
		if err != nil {
			level.Error(db.logger).Log("msg", "not a block dir", "dir", dir)
			continue
		}

		// See if we already have the block in memory or open it otherwise.
		// 看看我们在内存中是否已经又了block，否则打开它
		block, ok := db.getBlock(meta.ULID)
		if !ok {
			block, err = OpenBlock(db.logger, dir, db.chunkPool)
			if err != nil {
				// 打开过程中遇到错误的block都设置为corrupted
				corrupted[meta.ULID] = err
				continue
			}
		}
		blocks = append(blocks, block)
	}
	return blocks, corrupted, nil
}

// deletableBlocks returns all blocks past retention policy.
// deletableBlocks返回所有已经过了retention policy的blocks
func (db *DB) deletableBlocks(blocks []*Block) map[ulid.ULID]*Block {
	deletable := make(map[ulid.ULID]*Block)

	// Sort the blocks by time - newest to oldest (largest to smallest timestamp).
	// This ensures that the retentions will remove the oldest  blocks.
	// 按照时间对blocks进行排序 - 最新的到最老的（即从最大的时间戳到最小的时间戳）
	// 这确保retention会移除最老的blocks
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].Meta().MaxTime > blocks[j].Meta().MaxTime
	})

	for _, block := range blocks {
		if block.Meta().Compaction.Deletable {
			deletable[block.Meta().ULID] = block
		}
	}

	// 将所有超过时间范围的block设置为deletable
	for ulid, block := range db.beyondTimeRetention(blocks) {
		deletable[ulid] = block
	}

	// 将超过体积的block设置为deletable
	for ulid, block := range db.beyondSizeRetention(blocks) {
		deletable[ulid] = block
	}

	return deletable
}

func (db *DB) beyondTimeRetention(blocks []*Block) (deleteable map[ulid.ULID]*Block) {
	// Time retention is disabled or no blocks to work with.
	if len(db.blocks) == 0 || db.opts.RetentionDuration == 0 {
		return
	}

	deleteable = make(map[ulid.ULID]*Block)
	for i, block := range blocks {
		// The difference between the first block and this block is larger than
		// the retention period so any blocks after that are added as deleteable.
		if i > 0 && blocks[0].Meta().MaxTime-block.Meta().MaxTime > int64(db.opts.RetentionDuration) {
			for _, b := range blocks[i:] {
				deleteable[b.meta.ULID] = b
			}
			// 又成功进行了一次retention
			db.metrics.timeRetentionCount.Inc()
			break
		}
	}
	return deleteable
}

func (db *DB) beyondSizeRetention(blocks []*Block) (deleteable map[ulid.ULID]*Block) {
	// Size retention is disabled or no blocks to work with.
	if len(db.blocks) == 0 || db.opts.MaxBytes <= 0 {
		return
	}

	deleteable = make(map[ulid.ULID]*Block)
	blocksSize := int64(0)
	for i, block := range blocks {
		blocksSize += block.Size()
		if blocksSize > db.opts.MaxBytes {
			// Add this and all following blocks for deletion.
			// 将所有超过体积的block都进行retention
			for _, b := range blocks[i:] {
				deleteable[b.meta.ULID] = b
			}
			db.metrics.sizeRetentionCount.Inc()
			break
		}
	}
	return deleteable
}

// deleteBlocks closes and deletes blocks from the disk.
// When the map contains a non nil block object it means it is loaded in memory
// so needs to be closed first as it might need to wait for pending readers to complete.
// deleteBlocks关闭并且将blocks从磁盘中移除，当map包含一个非nil的block对象，这意味着它加载在内存中
// 因此需要首先关闭它，因为它可能需要等待pending reader读取完毕
func (db *DB) deleteBlocks(blocks map[ulid.ULID]*Block) error {
	for ulid, block := range blocks {
		if block != nil {
			if err := block.Close(); err != nil {
				level.Warn(db.logger).Log("msg", "closing block failed", "err", err)
			}
		}
		if err := os.RemoveAll(filepath.Join(db.dir, ulid.String())); err != nil {
			return errors.Wrapf(err, "delete obsolete block %s", ulid)
		}
	}
	return nil
}

// validateBlockSequence returns error if given block meta files indicate that some blocks overlaps within sequence.
// validateBlockSequence返回错误，如果给定的block的元文件表明一些block在序号上重合了
func validateBlockSequence(bs []*Block) error {
	if len(bs) <= 1 {
		return nil
	}

	var metas []BlockMeta
	for _, b := range bs {
		metas = append(metas, b.meta)
	}

	overlaps := OverlappingBlocks(metas)
	if len(overlaps) > 0 {
		// 两个block的time range有没有重合
		return errors.Errorf("block time ranges overlap: %s", overlaps)
	}

	return nil
}

// TimeRange specifies minTime and maxTime range.
type TimeRange struct {
	Min, Max int64
}

// Overlaps contains overlapping blocks aggregated by overlapping range.
type Overlaps map[TimeRange][]BlockMeta

// String returns human readable string form of overlapped blocks.
func (o Overlaps) String() string {
	var res []string
	for r, overlaps := range o {
		var groups []string
		for _, m := range overlaps {
			groups = append(groups, fmt.Sprintf(
				"<ulid: %s, mint: %d, maxt: %d, range: %s>",
				m.ULID.String(),
				m.MinTime,
				m.MaxTime,
				(time.Duration((m.MaxTime-m.MinTime)/1000)*time.Second).String(),
			))
		}
		res = append(res, fmt.Sprintf(
			"[mint: %d, maxt: %d, range: %s, blocks: %d]: %s",
			r.Min, r.Max,
			(time.Duration((r.Max-r.Min)/1000)*time.Second).String(),
			len(overlaps),
			strings.Join(groups, ", ")),
		)
	}
	return strings.Join(res, "\n")
}

// OverlappingBlocks returns all overlapping blocks from given meta files.
func OverlappingBlocks(bm []BlockMeta) Overlaps {
	if len(bm) <= 1 {
		return nil
	}
	var (
		overlaps [][]BlockMeta

		// pending contains not ended blocks in regards to "current" timestamp.
		pending = []BlockMeta{bm[0]}
		// continuousPending helps to aggregate same overlaps to single group.
		continuousPending = true
	)

	// We have here blocks sorted by minTime. We iterate over each block and treat its minTime as our "current" timestamp.
	// We check if any of the pending block finished (blocks that we have seen before, but their maxTime was still ahead current
	// timestamp). If not, it means they overlap with our current block. In the same time current block is assumed pending.
	for _, b := range bm[1:] {
		var newPending []BlockMeta

		for _, p := range pending {
			// "b.MinTime" is our current time.
			if b.MinTime >= p.MaxTime {
				continuousPending = false
				continue
			}

			// "p" overlaps with "b" and "p" is still pending.
			newPending = append(newPending, p)
		}

		// Our block "b" is now pending.
		pending = append(newPending, b)
		if len(newPending) == 0 {
			// No overlaps.
			continue
		}

		if continuousPending && len(overlaps) > 0 {
			overlaps[len(overlaps)-1] = append(overlaps[len(overlaps)-1], b)
			continue
		}
		overlaps = append(overlaps, append(newPending, b))
		// Start new pendings.
		continuousPending = true
	}

	// Fetch the critical overlapped time range foreach overlap groups.
	overlapGroups := Overlaps{}
	for _, overlap := range overlaps {

		minRange := TimeRange{Min: 0, Max: math.MaxInt64}
		for _, b := range overlap {
			if minRange.Max > b.MaxTime {
				minRange.Max = b.MaxTime
			}

			if minRange.Min < b.MinTime {
				minRange.Min = b.MinTime
			}
		}
		overlapGroups[minRange] = overlap
	}

	return overlapGroups
}

func (db *DB) String() string {
	return "HEAD"
}

// Blocks returns the databases persisted blocks.
func (db *DB) Blocks() []*Block {
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	return db.blocks
}

// Head returns the databases's head.
func (db *DB) Head() *Head {
	return db.head
}

// Close the partition.
func (db *DB) Close() error {
	close(db.stopc)
	db.compactCancel()
	<-db.donec

	db.mtx.Lock()
	defer db.mtx.Unlock()

	var g errgroup.Group

	// blocks also contains all head blocks.
	for _, pb := range db.blocks {
		g.Go(pb.Close)
	}

	var merr tsdb_errors.MultiError

	merr.Add(g.Wait())

	if db.lockf != nil {
		merr.Add(db.lockf.Release())
	}
	merr.Add(db.head.Close())
	return merr.Err()
}

// DisableCompactions disables auto compactions.
func (db *DB) DisableCompactions() {
	db.autoCompactMtx.Lock()
	defer db.autoCompactMtx.Unlock()

	db.autoCompact = false
	level.Info(db.logger).Log("msg", "compactions disabled")
}

// EnableCompactions enables auto compactions.
func (db *DB) EnableCompactions() {
	db.autoCompactMtx.Lock()
	defer db.autoCompactMtx.Unlock()

	db.autoCompact = true
	level.Info(db.logger).Log("msg", "compactions enabled")
}

// Snapshot writes the current data to the directory. If withHead is set to true it
// will create a new block containing all data that's currently in the memory buffer/WAL.
func (db *DB) Snapshot(dir string, withHead bool) error {
	if dir == db.dir {
		return errors.Errorf("cannot snapshot into base directory")
	}
	if _, err := ulid.ParseStrict(dir); err == nil {
		return errors.Errorf("dir must not be a valid ULID")
	}

	db.cmtx.Lock()
	defer db.cmtx.Unlock()

	db.mtx.RLock()
	defer db.mtx.RUnlock()

	for _, b := range db.blocks {
		level.Info(db.logger).Log("msg", "snapshotting block", "block", b)

		if err := b.Snapshot(dir); err != nil {
			return errors.Wrapf(err, "error snapshotting block: %s", b.Dir())
		}
	}
	if !withHead {
		return nil
	}

	mint := db.head.MinTime()
	maxt := db.head.MaxTime()
	head := &rangeHead{
		head: db.head,
		mint: mint,
		maxt: maxt,
	}
	// Add +1 millisecond to block maxt because block intervals are half-open: [b.MinTime, b.MaxTime).
	// Because of this block intervals are always +1 than the total samples it includes.
	if _, err := db.compactor.Write(dir, head, mint, maxt+1, nil); err != nil {
		return errors.Wrap(err, "snapshot head block")
	}
	return nil
}

// Querier returns a new querier over the data partition for the given time range.
// A goroutine must not handle more than one open Querier.
// Querier返回一个给定时间段的数据的querier，一个goroutine不能处理超过一个打开的Querier
func (db *DB) Querier(mint, maxt int64) (Querier, error) {
	var blocks []BlockReader
	var blockMetas []BlockMeta

	db.mtx.RLock()
	defer db.mtx.RUnlock()

	for _, b := range db.blocks {
		if b.OverlapsClosedInterval(mint, maxt) {
			blocks = append(blocks, b)
			blockMetas = append(blockMetas, b.Meta())
		}
	}
	// 如果maxt大于head的MinTime，则也包含进来
	if maxt >= db.head.MinTime() {
		blocks = append(blocks, &rangeHead{
			head: db.head,
			mint: mint,
			maxt: maxt,
		})
	}

	blockQueriers := make([]Querier, 0, len(blocks))
	for _, b := range blocks {
		// 针对每个Block构建Querier
		q, err := NewBlockQuerier(b, mint, maxt)
		if err == nil {
			blockQueriers = append(blockQueriers, q)
			continue
		}
		// If we fail, all previously opened queriers must be closed.
		// 如果失败了，则必须关闭之前打开的queriers
		for _, q := range blockQueriers {
			q.Close()
		}
		return nil, errors.Wrapf(err, "open querier for block %s", b)
	}

	if len(OverlappingBlocks(blockMetas)) > 0 {
		return &verticalQuerier{
			querier: querier{
				blocks: blockQueriers,
			},
		}, nil
	}

	return &querier{
		blocks: blockQueriers,
	}, nil
}

func rangeForTimestamp(t int64, width int64) (maxt int64) {
	return (t/width)*width + width
}

// Delete implements deletion of metrics. It only has atomicity guarantees on a per-block basis.
func (db *DB) Delete(mint, maxt int64, ms ...labels.Matcher) error {
	db.cmtx.Lock()
	defer db.cmtx.Unlock()

	var g errgroup.Group

	db.mtx.RLock()
	defer db.mtx.RUnlock()

	for _, b := range db.blocks {
		if b.OverlapsClosedInterval(mint, maxt) {
			g.Go(func(b *Block) func() error {
				return func() error { return b.Delete(mint, maxt, ms...) }
			}(b))
		}
	}
	g.Go(func() error {
		return db.head.Delete(mint, maxt, ms...)
	})
	return g.Wait()
}

// CleanTombstones re-writes any blocks with tombstones.
func (db *DB) CleanTombstones() (err error) {
	db.cmtx.Lock()
	defer db.cmtx.Unlock()

	start := time.Now()
	defer db.metrics.tombCleanTimer.Observe(time.Since(start).Seconds())

	newUIDs := []ulid.ULID{}
	defer func() {
		// If any error is caused, we need to delete all the new directory created.
		if err != nil {
			for _, uid := range newUIDs {
				dir := filepath.Join(db.Dir(), uid.String())
				if err := os.RemoveAll(dir); err != nil {
					level.Error(db.logger).Log("msg", "failed to delete block after failed `CleanTombstones`", "dir", dir, "err", err)
				}
			}
		}
	}()

	db.mtx.RLock()
	blocks := db.blocks[:]
	db.mtx.RUnlock()

	for _, b := range blocks {
		if uid, er := b.CleanTombstones(db.Dir(), db.compactor); er != nil {
			err = errors.Wrapf(er, "clean tombstones: %s", b.Dir())
			return err
		} else if uid != nil { // New block was created.
			newUIDs = append(newUIDs, *uid)
		}
	}
	return errors.Wrap(db.reload(), "reload blocks")
}

func isBlockDir(fi os.FileInfo) bool {
	if !fi.IsDir() {
		return false
	}
	_, err := ulid.ParseStrict(fi.Name())
	return err == nil
}

func blockDirs(dir string) ([]string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var dirs []string

	for _, fi := range files {
		if isBlockDir(fi) {
			dirs = append(dirs, filepath.Join(dir, fi.Name()))
		}
	}
	return dirs, nil
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
	return filepath.Join(dir, fmt.Sprintf("%0.6d", i+1)), int(i + 1), nil
}

func closeAll(cs []io.Closer) error {
	var merr tsdb_errors.MultiError

	for _, c := range cs {
		merr.Add(c.Close())
	}
	return merr.Err()
}

func exponential(d, min, max time.Duration) time.Duration {
	d *= 2
	if d < min {
		d = min
	}
	if d > max {
		d = max
	}
	return d
}
