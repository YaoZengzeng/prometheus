// Copyright 2016 The Prometheus Authors
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

package scrape

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"sync"
	"time"
	"unsafe"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/version"

	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/pool"
	"github.com/prometheus/prometheus/pkg/relabel"
	"github.com/prometheus/prometheus/pkg/textparse"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/pkg/value"
	"github.com/prometheus/prometheus/storage"
)

var (
	targetIntervalLength = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "prometheus_target_interval_length_seconds",
			Help:       "Actual intervals between scrapes.",
			Objectives: map[float64]float64{0.01: 0.001, 0.05: 0.005, 0.5: 0.05, 0.90: 0.01, 0.99: 0.001},
		},
		[]string{"interval"},
	)
	targetReloadIntervalLength = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "prometheus_target_reload_length_seconds",
			Help:       "Actual interval to reload the scrape pool with a given configuration.",
			Objectives: map[float64]float64{0.01: 0.001, 0.05: 0.005, 0.5: 0.05, 0.90: 0.01, 0.99: 0.001},
		},
		[]string{"interval"},
	)
	targetScrapePools = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrape_pools_total",
			Help: "Total number of scrape pool creation atttempts.",
		},
	)
	targetScrapePoolsFailed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrape_pools_failed_total",
			Help: "Total number of scrape pool creations that failed.",
		},
	)
	targetScrapePoolReloads = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrape_pool_reloads_total",
			Help: "Total number of scrape loop reloads.",
		},
	)
	targetScrapePoolReloadsFailed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrape_pool_reloads_failed_total",
			Help: "Total number of failed scrape loop reloads.",
		},
	)
	targetSyncIntervalLength = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "prometheus_target_sync_length_seconds",
			Help:       "Actual interval to sync the scrape pool.",
			Objectives: map[float64]float64{0.01: 0.001, 0.05: 0.005, 0.5: 0.05, 0.90: 0.01, 0.99: 0.001},
		},
		[]string{"scrape_job"},
	)
	targetScrapePoolSyncsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrape_pool_sync_total",
			Help: "Total number of syncs that were executed on a scrape pool.",
		},
		[]string{"scrape_job"},
	)
	targetScrapeSampleLimit = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrapes_exceeded_sample_limit_total",
			Help: "Total number of scrapes that hit the sample limit and were rejected.",
		},
	)
	targetScrapeSampleDuplicate = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrapes_sample_duplicate_timestamp_total",
			Help: "Total number of samples rejected due to duplicate timestamps but different values",
		},
	)
	targetScrapeSampleOutOfOrder = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrapes_sample_out_of_order_total",
			Help: "Total number of samples rejected due to not being out of the expected order",
		},
	)
	targetScrapeSampleOutOfBounds = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrapes_sample_out_of_bounds_total",
			Help: "Total number of samples rejected due to timestamp falling outside of the time bounds",
		},
	)
	targetScrapeCacheFlushForced = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrapes_cache_flush_forced_total",
			Help: "How many times a scrape cache was flushed due to getting big while scrapes are failing.",
		},
	)
)

func init() {
	prometheus.MustRegister(targetIntervalLength)
	prometheus.MustRegister(targetReloadIntervalLength)
	prometheus.MustRegister(targetScrapePools)
	prometheus.MustRegister(targetScrapePoolsFailed)
	prometheus.MustRegister(targetScrapePoolReloads)
	prometheus.MustRegister(targetScrapePoolReloadsFailed)
	prometheus.MustRegister(targetSyncIntervalLength)
	prometheus.MustRegister(targetScrapePoolSyncsCounter)
	prometheus.MustRegister(targetScrapeSampleLimit)
	prometheus.MustRegister(targetScrapeSampleDuplicate)
	prometheus.MustRegister(targetScrapeSampleOutOfOrder)
	prometheus.MustRegister(targetScrapeSampleOutOfBounds)
	prometheus.MustRegister(targetScrapeCacheFlushForced)
}

// scrapePool manages scrapes for sets of targets.
// scrapePool管理对于一系列targets的抓取
type scrapePool struct {
	appendable Appendable
	logger     log.Logger

	mtx    sync.RWMutex
	config *config.ScrapeConfig
	client *http.Client
	// Targets and loops must always be synchronized to have the same
	// set of hashes.
	// Targets和loops必须总是被同步从而有着相同的哈希值
	// 维护一个activeTargets的列表
	activeTargets  map[uint64]*Target
	droppedTargets []*Target
	loops          map[uint64]loop
	cancel         context.CancelFunc

	// Constructor for new scrape loops. This is settable for testing convenience.
	// Constructor用于新的scrape loops，这仅仅用于方便测试
	newLoop func(scrapeLoopOptions) loop
}

type scrapeLoopOptions struct {
	target          *Target
	scraper         scraper
	limit           int
	honorLabels     bool
	honorTimestamps bool
	mrc             []*relabel.Config
}

const maxAheadTime = 10 * time.Minute

type labelsMutator func(labels.Labels) labels.Labels

// 一个ScrapeConfig创建一个ScrapePool
func newScrapePool(cfg *config.ScrapeConfig, app Appendable, jitterSeed uint64, logger log.Logger) (*scrapePool, error) {
	targetScrapePools.Inc()
	if logger == nil {
		logger = log.NewNopLogger()
	}

	// 创建target的client
	client, err := config_util.NewClientFromConfig(cfg.HTTPClientConfig, cfg.JobName)
	if err != nil {
		targetScrapePoolsFailed.Inc()
		return nil, errors.Wrap(err, "error creating HTTP client")
	}

	buffers := pool.New(1e3, 100e6, 3, func(sz int) interface{} { return make([]byte, 0, sz) })

	ctx, cancel := context.WithCancel(context.Background())
	sp := &scrapePool{
		cancel:        cancel,
		appendable:    app,
		config:        cfg,
		client:        client,
		activeTargets: map[uint64]*Target{},
		loops:         map[uint64]loop{},
		logger:        logger,
	}
	sp.newLoop = func(opts scrapeLoopOptions) loop {
		// Update the targets retrieval function for metadata to a new scrape cache.
		// 创建Scrape Cache用于缓存数据
		cache := newScrapeCache()
		opts.target.setMetadataStore(cache)

		return newScrapeLoop(
			ctx,
			opts.scraper,
			log.With(logger, "target", opts.target),
			buffers,
			func(l labels.Labels) labels.Labels {
				return mutateSampleLabels(l, opts.target, opts.honorLabels, opts.mrc)
			},
			func(l labels.Labels) labels.Labels { return mutateReportSampleLabels(l, opts.target) },
			func() storage.Appender {
				app, err := app.Appender()
				if err != nil {
					panic(err)
				}
				return appender(app, opts.limit)
			},
			cache,
			jitterSeed,
			opts.honorTimestamps,
		)
	}

	return sp, nil
}

func (sp *scrapePool) ActiveTargets() []*Target {
	sp.mtx.Lock()
	defer sp.mtx.Unlock()

	var tActive []*Target
	for _, t := range sp.activeTargets {
		tActive = append(tActive, t)
	}
	return tActive
}

func (sp *scrapePool) DroppedTargets() []*Target {
	sp.mtx.Lock()
	defer sp.mtx.Unlock()
	return sp.droppedTargets
}

// stop terminates all scrape loops and returns after they all terminated.
// stop结束所有的scrape loops并且在它们终止的时候返回
func (sp *scrapePool) stop() {
	sp.cancel()
	var wg sync.WaitGroup

	sp.mtx.Lock()
	defer sp.mtx.Unlock()

	for fp, l := range sp.loops {
		wg.Add(1)

		go func(l loop) {
			l.stop()
			wg.Done()
		}(l)

		delete(sp.loops, fp)
		delete(sp.activeTargets, fp)
	}
	wg.Wait()
	sp.client.CloseIdleConnections()
}

// reload the scrape pool with the given scrape configuration. The target state is preserved
// but all scrape loops are restarted with the new scrape configuration.
// This method returns after all scrape loops that were stopped have stopped scraping.
// 用给定的scrapte configuration加载scrape pool，目标state会被保留，但是所有的scrape loops都会用
// 新的scrape configuration进行重启，本方法返回直到所有的scrape loops应该被停止的都已经停止scraping了
func (sp *scrapePool) reload(cfg *config.ScrapeConfig) error {
	targetScrapePoolReloads.Inc()
	start := time.Now()

	sp.mtx.Lock()
	defer sp.mtx.Unlock()

	// 加载http client
	client, err := config_util.NewClientFromConfig(cfg.HTTPClientConfig, cfg.JobName)
	if err != nil {
		targetScrapePoolReloadsFailed.Inc()
		return errors.Wrap(err, "error creating HTTP client")
	}
	sp.config = cfg
	oldClient := sp.client
	sp.client = client

	var (
		wg              sync.WaitGroup
		interval        = time.Duration(sp.config.ScrapeInterval)
		timeout         = time.Duration(sp.config.ScrapeTimeout)
		limit           = int(sp.config.SampleLimit)
		honorLabels     = sp.config.HonorLabels
		honorTimestamps = sp.config.HonorTimestamps
		mrc             = sp.config.MetricRelabelConfigs
	)

	for fp, oldLoop := range sp.loops {
		var (
			t       = sp.activeTargets[fp]
			s       = &targetScraper{Target: t, client: sp.client, timeout: timeout}
			// 创建newLoop
			newLoop = sp.newLoop(scrapeLoopOptions{
				target:          t,
				scraper:         s,
				limit:           limit,
				honorLabels:     honorLabels,
				honorTimestamps: honorTimestamps,
				mrc:             mrc,
			})
		)
		wg.Add(1)

		go func(oldLoop, newLoop loop) {
			// 停止oldLoop
			oldLoop.stop()
			wg.Done()

			// 运行newLoop
			go newLoop.run(interval, timeout, nil)
		}(oldLoop, newLoop)

		sp.loops[fp] = newLoop
	}

	wg.Wait()
	oldClient.CloseIdleConnections()
	targetReloadIntervalLength.WithLabelValues(interval.String()).Observe(
		time.Since(start).Seconds(),
	)
	return nil
}

// Sync converts target groups into actual scrape targets and synchronizes
// the currently running scraper with the resulting set and returns all scraped and dropped targets.
// Sync将target groups转换为真正的scrape targets并且同步当前正在运行的scraper和resulting set
// 返回所有的scraped和dropped targets
func (sp *scrapePool) Sync(tgs []*targetgroup.Group) {
	start := time.Now()

	var all []*Target
	sp.mtx.Lock()
	sp.droppedTargets = []*Target{}
	// 遍历targetgroup数组
	for _, tg := range tgs {
		// 根据scrape config对target进行过滤处理
		targets, err := targetsFromGroup(tg, sp.config)
		if err != nil {
			level.Error(sp.logger).Log("msg", "creating targets failed", "err", err)
			continue
		}
		for _, t := range targets {
			// 如果经过处理后的target的labels数目大于0，则将它保留，否则如果labels数目小于等于0，而discovered labels大于0，则drop
			if t.Labels().Len() > 0 {
				all = append(all, t)
			} else if t.DiscoveredLabels().Len() > 0 {
				sp.droppedTargets = append(sp.droppedTargets, t)
			}
		}
	}
	sp.mtx.Unlock()
	// 对所有活跃的targets进行同步
	sp.sync(all)

	targetSyncIntervalLength.WithLabelValues(sp.config.JobName).Observe(
		time.Since(start).Seconds(),
	)
	targetScrapePoolSyncsCounter.WithLabelValues(sp.config.JobName).Inc()
}

// sync takes a list of potentially duplicated targets, deduplicates them, starts
// scrape loops for new targets, and stops scrape loops for disappeared targets.
// It returns after all stopped scrape loops terminated.
// sync拿着一系列可能重合的targets，首先去重，接着为新的targets启动scrape loops，并且对于已经消失的
// targets停止scrape loops，返回时，所有停止的scrape loops都已经结束了
func (sp *scrapePool) sync(targets []*Target) {
	sp.mtx.Lock()
	defer sp.mtx.Unlock()

	var (
		uniqueTargets   = map[uint64]struct{}{}
		interval        = time.Duration(sp.config.ScrapeInterval)
		timeout         = time.Duration(sp.config.ScrapeTimeout)
		limit           = int(sp.config.SampleLimit)
		honorLabels     = sp.config.HonorLabels
		honorTimestamps = sp.config.HonorTimestamps
		mrc             = sp.config.MetricRelabelConfigs
	)

	// 遍历targets
	for _, t := range targets {
		t := t
		// 获取target的哈希值
		hash := t.hash()
		// 用uniqueTargets记录当前的targets
		uniqueTargets[hash] = struct{}{}

		if _, ok := sp.activeTargets[hash]; !ok {
			// 创建targetScraper
			s := &targetScraper{Target: t, client: sp.client, timeout: timeout}
			// 创建一个新的Loop
			l := sp.newLoop(scrapeLoopOptions{
				target:          t,
				scraper:         s,
				limit:           limit,
				honorLabels:     honorLabels,
				honorTimestamps: honorTimestamps,
				mrc:             mrc,
			})

			// 设置相应的activeTargets和loops
			sp.activeTargets[hash] = t
			sp.loops[hash] = l

			// 每个target对应一个scrape loop
			go l.run(interval, timeout, nil)
		} else {
			// Need to keep the most updated labels information
			// for displaying it in the Service Discovery web page.
			// 需要保持最新的labels信息，用于展示在Service Discovery的web page上
			sp.activeTargets[hash].SetDiscoveredLabels(t.DiscoveredLabels())
		}
	}

	var wg sync.WaitGroup

	// Stop and remove old targets and scraper loops.
	// 停止并且移除老的targets和scraper loops
	for hash := range sp.activeTargets {
		if _, ok := uniqueTargets[hash]; !ok {
			wg.Add(1)
			go func(l loop) {

				// 停止loop
				l.stop()

				wg.Done()
			}(sp.loops[hash])

			// 从loops和activeTargets中删除
			delete(sp.loops, hash)
			delete(sp.activeTargets, hash)
		}
	}

	// Wait for all potentially stopped scrapers to terminate.
	// This covers the case of flapping targets. If the server is under high load, a new scraper
	// may be active and tries to insert. The old scraper that didn't terminate yet could still
	// be inserting a previous sample set.
	// 等待所有潜在的stopped scrapers终止
	// 这覆盖了flapping targets的情况，如果服务器处于高负载，一个新的scraper可能active并且试着insert
	// 一个老的scraper还没终止的话，仍然可以插入之前的sample set
	wg.Wait()
}

func mutateSampleLabels(lset labels.Labels, target *Target, honor bool, rc []*relabel.Config) labels.Labels {
	lb := labels.NewBuilder(lset)

	if honor {
		for _, l := range target.Labels() {
			if !lset.Has(l.Name) {
				lb.Set(l.Name, l.Value)
			}
		}
	} else {
		for _, l := range target.Labels() {
			lv := lset.Get(l.Name)
			if lv != "" {
				lb.Set(model.ExportedLabelPrefix+l.Name, lv)
			}
			lb.Set(l.Name, l.Value)
		}
	}

	for _, l := range lb.Labels() {
		if l.Value == "" {
			lb.Del(l.Name)
		}
	}

	res := lb.Labels()

	if len(rc) > 0 {
		res = relabel.Process(res, rc...)
	}

	return res
}

func mutateReportSampleLabels(lset labels.Labels, target *Target) labels.Labels {
	lb := labels.NewBuilder(lset)

	for _, l := range target.Labels() {
		lv := lset.Get(l.Name)
		if lv != "" {
			lb.Set(model.ExportedLabelPrefix+l.Name, lv)
		}
		lb.Set(l.Name, l.Value)
	}

	return lb.Labels()
}

// appender returns an appender for ingested samples from the target.
func appender(app storage.Appender, limit int) storage.Appender {
	app = &timeLimitAppender{
		Appender: app,
		maxTime:  timestamp.FromTime(time.Now().Add(maxAheadTime)),
	}

	// The limit is applied after metrics are potentially dropped via relabeling.
	if limit > 0 {
		app = &limitAppender{
			Appender: app,
			limit:    limit,
		}
	}
	return app
}

// A scraper retrieves samples and accepts a status report at the end.
// scraper获取samples并且在最后接收一个status report
type scraper interface {
	scrape(ctx context.Context, w io.Writer) (string, error)
	report(start time.Time, dur time.Duration, err error)
	offset(interval time.Duration, jitterSeed uint64) time.Duration
}

// targetScraper implements the scraper interface for a target.
// targetScraper实现了一个target的scraper接口
type targetScraper struct {
	*Target

	client  *http.Client
	req     *http.Request
	timeout time.Duration

	gzipr *gzip.Reader
	buf   *bufio.Reader
}

const acceptHeader = `application/openmetrics-text; version=0.0.1,text/plain;version=0.0.4;q=0.5,*/*;q=0.1`

var userAgentHeader = fmt.Sprintf("Prometheus/%s", version.Version)

// 对目标进行scrape，抓取的数据写入w，返回内容的类型
func (s *targetScraper) scrape(ctx context.Context, w io.Writer) (string, error) {
	if s.req == nil {
		// 创建一个http request并且保存在s.req中
		req, err := http.NewRequest("GET", s.URL().String(), nil)
		if err != nil {
			return "", err
		}
		req.Header.Add("Accept", acceptHeader)
		req.Header.Add("Accept-Encoding", "gzip")
		req.Header.Set("User-Agent", userAgentHeader)
		req.Header.Set("X-Prometheus-Scrape-Timeout-Seconds", fmt.Sprintf("%f", s.timeout.Seconds()))

		// 直接写死s.req
		s.req = req
	}

	// 调用s.client.Do
	// 向target发起请求，获取时序数据
	resp, err := s.client.Do(s.req.WithContext(ctx))
	if err != nil {
		return "", err
	}
	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()

	// 如果返回码不为http.StatusOK，则报错
	if resp.StatusCode != http.StatusOK {
		return "", errors.Errorf("server returned HTTP status %s", resp.Status)
	}

	if resp.Header.Get("Content-Encoding") != "gzip" {
		// 若content不是用gzip进行编码，则直接写入
		_, err = io.Copy(w, resp.Body)
		if err != nil {
			return "", err
		}
		return resp.Header.Get("Content-Type"), nil
	}

	// 如果是用gzip进行了压缩
	if s.gzipr == nil {
		s.buf = bufio.NewReader(resp.Body)
		s.gzipr, err = gzip.NewReader(s.buf)
		if err != nil {
			return "", err
		}
	} else {
		// 用resp.Body重置s.buf
		s.buf.Reset(resp.Body)
		if err = s.gzipr.Reset(s.buf); err != nil {
			return "", err
		}
	}

	// 从s.gzipr写入w
	_, err = io.Copy(w, s.gzipr)
	s.gzipr.Close()
	if err != nil {
		return "", err
	}
	return resp.Header.Get("Content-Type"), nil
}

// A loop can run and be stopped again. It must not be reused after it was stopped.
// loop可以run也可以被stop，但是它在stop之后一定不能再被重用
type loop interface {
	run(interval, timeout time.Duration, errc chan<- error)
	stop()
}

type cacheEntry struct {
	ref      uint64
	lastIter uint64
	hash     uint64
	lset     labels.Labels
}

type scrapeLoop struct {
	scraper         scraper
	l               log.Logger
	cache           *scrapeCache
	lastScrapeSize  int
	buffers         *pool.Pool
	jitterSeed      uint64
	honorTimestamps bool

	appender            func() storage.Appender
	sampleMutator       labelsMutator
	reportSampleMutator labelsMutator

	ctx       context.Context
	scrapeCtx context.Context
	cancel    func()
	stopped   chan struct{}
}

// scrapeCache tracks mappings of exposed metric strings to label sets and
// storage references. Additionally, it tracks staleness of series between
// scrapes.
// scrapeCache追踪exposed metric strings到label sets和storage references的映射
// 另外，它也追踪scrapes之间的series的过期
type scrapeCache struct {
	// 当前scrape的次数
	iter uint64 // Current scrape iteration.

	// How many series and metadata entries there were at the last success.
	successfulCount int

	// Parsed string to an entry with information about the actual label set
	// and its storage reference.
	series map[string]*cacheEntry

	// Cache of dropped metric strings and their iteration. The iteration must
	// be a pointer so we can update it without setting a new entry with an unsafe
	// string in addDropped().
	droppedSeries map[string]*uint64

	// seriesCur and seriesPrev store the labels of series that were seen
	// in the current and previous scrape.
	// We hold two maps and swap them out to save allocations.
	seriesCur  map[uint64]labels.Labels
	seriesPrev map[uint64]labels.Labels

	metaMtx  sync.Mutex
	metadata map[string]*metaEntry
}

// metaEntry holds meta information about a metric.
// metaEntry维护了一个metric的元数据信息
type metaEntry struct {
	lastIter uint64 // Last scrape iteration the entry was observed at.
	typ      textparse.MetricType
	help     string
	unit     string
}

func newScrapeCache() *scrapeCache {
	return &scrapeCache{
		series:        map[string]*cacheEntry{},
		droppedSeries: map[string]*uint64{},
		seriesCur:     map[uint64]labels.Labels{},
		seriesPrev:    map[uint64]labels.Labels{},
		metadata:      map[string]*metaEntry{},
	}
}

func (c *scrapeCache) iterDone(flushCache bool) {
	c.metaMtx.Lock()
	count := len(c.series) + len(c.droppedSeries) + len(c.metadata)
	c.metaMtx.Unlock()

	if flushCache {
		c.successfulCount = count
	} else if count > c.successfulCount*2+1000 {
		// If a target had varying labels in scrapes that ultimately failed,
		// the caches would grow indefinitely. Force a flush when this happens.
		// We use the heuristic that this is a doubling of the cache size
		// since the last scrape, and allow an additional 1000 in case
		// initial scrapes all fail.
		flushCache = true
		targetScrapeCacheFlushForced.Inc()
	}

	if flushCache {
		// All caches may grow over time through series churn
		// or multiple string representations of the same metric. Clean up entries
		// that haven't appeared in the last scrape.
		for s, e := range c.series {
			if c.iter != e.lastIter {
				delete(c.series, s)
			}
		}
		for s, iter := range c.droppedSeries {
			if c.iter != *iter {
				delete(c.droppedSeries, s)
			}
		}
		c.metaMtx.Lock()
		for m, e := range c.metadata {
			// Keep metadata around for 10 scrapes after its metric disappeared.
			if c.iter-e.lastIter > 10 {
				delete(c.metadata, m)
			}
		}
		c.metaMtx.Unlock()

		c.iter++
	}

	// Swap current and previous series.
	c.seriesPrev, c.seriesCur = c.seriesCur, c.seriesPrev

	// We have to delete every single key in the map.
	for k := range c.seriesCur {
		delete(c.seriesCur, k)
	}
}

func (c *scrapeCache) get(met string) (*cacheEntry, bool) {
	e, ok := c.series[met]
	if !ok {
		return nil, false
	}
	e.lastIter = c.iter
	return e, true
}

func (c *scrapeCache) addRef(met string, ref uint64, lset labels.Labels, hash uint64) {
	if ref == 0 {
		return
	}
	c.series[met] = &cacheEntry{ref: ref, lastIter: c.iter, lset: lset, hash: hash}
}

func (c *scrapeCache) addDropped(met string) {
	iter := c.iter
	c.droppedSeries[met] = &iter
}

func (c *scrapeCache) getDropped(met string) bool {
	iterp, ok := c.droppedSeries[met]
	if ok {
		*iterp = c.iter
	}
	return ok
}

func (c *scrapeCache) trackStaleness(hash uint64, lset labels.Labels) {
	c.seriesCur[hash] = lset
}

func (c *scrapeCache) forEachStale(f func(labels.Labels) bool) {
	for h, lset := range c.seriesPrev {
		if _, ok := c.seriesCur[h]; !ok {
			if !f(lset) {
				break
			}
		}
	}
}

func (c *scrapeCache) setType(metric []byte, t textparse.MetricType) {
	c.metaMtx.Lock()

	e, ok := c.metadata[yoloString(metric)]
	if !ok {
		e = &metaEntry{typ: textparse.MetricTypeUnknown}
		c.metadata[string(metric)] = e
	}
	e.typ = t
	e.lastIter = c.iter

	c.metaMtx.Unlock()
}

func (c *scrapeCache) setHelp(metric, help []byte) {
	c.metaMtx.Lock()

	e, ok := c.metadata[yoloString(metric)]
	if !ok {
		e = &metaEntry{typ: textparse.MetricTypeUnknown}
		c.metadata[string(metric)] = e
	}
	if e.help != yoloString(help) {
		e.help = string(help)
	}
	e.lastIter = c.iter

	c.metaMtx.Unlock()
}

func (c *scrapeCache) setUnit(metric, unit []byte) {
	c.metaMtx.Lock()

	e, ok := c.metadata[yoloString(metric)]
	if !ok {
		e = &metaEntry{typ: textparse.MetricTypeUnknown}
		c.metadata[string(metric)] = e
	}
	if e.unit != yoloString(unit) {
		e.unit = string(unit)
	}
	e.lastIter = c.iter

	c.metaMtx.Unlock()
}

func (c *scrapeCache) getMetadata(metric string) (MetricMetadata, bool) {
	c.metaMtx.Lock()
	defer c.metaMtx.Unlock()

	m, ok := c.metadata[metric]
	if !ok {
		return MetricMetadata{}, false
	}
	return MetricMetadata{
		Metric: metric,
		Type:   m.typ,
		Help:   m.help,
		Unit:   m.unit,
	}, true
}

// 所有的元数据都存放在scrapeCache中
func (c *scrapeCache) listMetadata() []MetricMetadata {
	c.metaMtx.Lock()
	defer c.metaMtx.Unlock()

	res := make([]MetricMetadata, 0, len(c.metadata))

	for m, e := range c.metadata {
		res = append(res, MetricMetadata{
			Metric: m,
			Type:   e.typ,
			Help:   e.help,
			Unit:   e.unit,
		})
	}
	return res
}

func newScrapeLoop(ctx context.Context,
	sc scraper,
	l log.Logger,
	buffers *pool.Pool,
	sampleMutator labelsMutator,
	reportSampleMutator labelsMutator,
	appender func() storage.Appender,
	cache *scrapeCache,
	jitterSeed uint64,
	honorTimestamps bool,
) *scrapeLoop {
	if l == nil {
		l = log.NewNopLogger()
	}
	if buffers == nil {
		buffers = pool.New(1e3, 1e6, 3, func(sz int) interface{} { return make([]byte, 0, sz) })
	}
	if cache == nil {
		cache = newScrapeCache()
	}
	sl := &scrapeLoop{
		scraper:             sc,
		buffers:             buffers,
		cache:               cache,
		appender:            appender,
		sampleMutator:       sampleMutator,
		reportSampleMutator: reportSampleMutator,
		stopped:             make(chan struct{}),
		jitterSeed:          jitterSeed,
		l:                   l,
		ctx:                 ctx,
		honorTimestamps:     honorTimestamps,
	}
	sl.scrapeCtx, sl.cancel = context.WithCancel(ctx)

	return sl
}

func (sl *scrapeLoop) run(interval, timeout time.Duration, errc chan<- error) {
	select {
	case <-time.After(sl.scraper.offset(interval, sl.jitterSeed)):
		// Continue after a scraping offset.
		// 在一个scraping offset之后开始执行
	case <-sl.scrapeCtx.Done():
		close(sl.stopped)
		return
	}

	var last time.Time

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

mainLoop:
	for {
		select {
		case <-sl.ctx.Done():
			close(sl.stopped)
			return
		case <-sl.scrapeCtx.Done():
			break mainLoop
		default:
		}

		var (
			start             = time.Now()
			scrapeCtx, cancel = context.WithTimeout(sl.ctx, timeout)
		)

		// Only record after the first scrape.
		// 只在第一个scrape之后开始记录
		if !last.IsZero() {
			targetIntervalLength.WithLabelValues(interval.String()).Observe(
				time.Since(last).Seconds(),
			)
		}

		// 根据上次抓取的大小，确定缓存
		b := sl.buffers.Get(sl.lastScrapeSize).([]byte)
		buf := bytes.NewBuffer(b)

		// 调用scraper进行抓取
		contentType, scrapeErr := sl.scraper.scrape(scrapeCtx, buf)
		cancel()

		if scrapeErr == nil {
			b = buf.Bytes()
			// NOTE: There were issues with misbehaving clients in the past
			// that occasionally returned empty results. We don't want those
			// to falsely reset our buffer size.
			// 可能会有一些misbehaving的clients，它会偶尔返回空的结果，我们不希望这些错误
			// 地设置我们的buffer size
			if len(b) > 0 {
				sl.lastScrapeSize = len(b)
			}
		} else {
			level.Debug(sl.l).Log("msg", "Scrape failed", "err", scrapeErr.Error())
			if errc != nil {
				errc <- scrapeErr
			}
		}

		// A failed scrape is the same as an empty scrape,
		// we still call sl.append to trigger stale markers.
		// 一次失败的scrape和一次空的scrape是一样的
		// 我们仍然调用sl.append来触发stale markers
		total, added, appErr := sl.append(b, contentType, start)
		if appErr != nil {
			level.Warn(sl.l).Log("msg", "append failed", "err", appErr)
			// The append failed, probably due to a parse error or sample limit.
			// Call sl.append again with an empty scrape to trigger stale markers.
			// append失败，可能是因为一个parse error或者sample limit
			// 用empty scrape再次调用sl.append来触发stale markers
			if _, _, err := sl.append([]byte{}, "", start); err != nil {
				level.Warn(sl.l).Log("msg", "append failed", "err", err)
			}
		}

		// 将b放入缓存
		sl.buffers.Put(b)

		if scrapeErr == nil {
			scrapeErr = appErr
		}

		if err := sl.report(start, time.Since(start), total, added, scrapeErr); err != nil {
			level.Warn(sl.l).Log("msg", "appending scrape report failed", "err", err)
		}
		last = start

		select {
		case <-sl.ctx.Done():
			close(sl.stopped)
			return
		case <-sl.scrapeCtx.Done():
			break mainLoop
		// 每隔interval秒scrape一次
		case <-ticker.C:
		}
	}

	// 关闭sl.stopped
	close(sl.stopped)

	sl.endOfRunStaleness(last, ticker, interval)
}

func (sl *scrapeLoop) endOfRunStaleness(last time.Time, ticker *time.Ticker, interval time.Duration) {
	// Scraping has stopped. We want to write stale markers but
	// the target may be recreated, so we wait just over 2 scrape intervals
	// before creating them.
	// If the context is canceled, we presume the server is shutting down
	// and will restart where is was. We do not attempt to write stale markers
	// in this case.
	// Scraping已经结束了，我们想要写stale markers，但是target可能被重建了，因此我们在创建
	// 他们之前等待两个scrape intervals
	// 如果context被取消，我们假设server被关闭了并且还会被重启，在这种情况下，我们不会试着写入
	// stale markers

	if last.IsZero() {
		// There never was a scrape, so there will be no stale markers.
		return
	}

	// Wait for when the next scrape would have been, record its timestamp.
	var staleTime time.Time
	select {
	case <-sl.ctx.Done():
		return
	case <-ticker.C:
		staleTime = time.Now()
	}

	// Wait for when the next scrape would have been, if the target was recreated
	// samples should have been ingested by now.
	select {
	case <-sl.ctx.Done():
		return
	case <-ticker.C:
	}

	// Wait for an extra 10% of the interval, just to be safe.
	select {
	case <-sl.ctx.Done():
		return
	case <-time.After(interval / 10):
	}

	// Call sl.append again with an empty scrape to trigger stale markers.
	// If the target has since been recreated and scraped, the
	// stale markers will be out of order and ignored.
	if _, _, err := sl.append([]byte{}, "", staleTime); err != nil {
		level.Error(sl.l).Log("msg", "stale append failed", "err", err)
	}
	if err := sl.reportStale(staleTime); err != nil {
		level.Error(sl.l).Log("msg", "stale report failed", "err", err)
	}
}

// Stop the scraping. May still write data and stale markers after it has
// returned. Cancel the context to stop all writes.
func (sl *scrapeLoop) stop() {
	sl.cancel()
	<-sl.stopped
}

type sample struct {
	metric labels.Labels
	t      int64
	v      float64
}

//lint:ignore U1000 staticcheck falsely reports that samples is unused.
type samples []sample

func (s samples) Len() int      { return len(s) }
func (s samples) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (s samples) Less(i, j int) bool {
	d := labels.Compare(s[i].metric, s[j].metric)
	if d < 0 {
		return true
	} else if d > 0 {
		return false
	}
	return s[i].t < s[j].t
}

// append一方面将抓取到的数据缓存到cache中，另一方面将数据写入tsdb
func (sl *scrapeLoop) append(b []byte, contentType string, ts time.Time) (total, added int, err error) {
	var (
		// 获取appender
		app            = sl.appender()
		p              = textparse.New(b, contentType)
		defTime        = timestamp.FromTime(ts)
		numOutOfOrder  = 0
		numDuplicates  = 0
		numOutOfBounds = 0
	)
	var sampleLimitErr error

loop:
	for {
		var et textparse.Entry
		if et, err = p.Next(); err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}
		switch et {
		// 根据不同的类型设置类型，Help以及Entry的具体内容
		case textparse.EntryType:
			sl.cache.setType(p.Type())
			continue
		case textparse.EntryHelp:
			sl.cache.setHelp(p.Help())
			continue
		case textparse.EntryUnit:
			sl.cache.setUnit(p.Unit())
			continue
		case textparse.EntryComment:
			continue
		default:
		}
		total++

		t := defTime
		met, tp, v := p.Series()
		if !sl.honorTimestamps {
			tp = nil
		}
		if tp != nil {
			t = *tp
		}

		if sl.cache.getDropped(yoloString(met)) {
			continue
		}
		ce, ok := sl.cache.get(yoloString(met))
		if ok {
			// 写入tsdb
			switch err = app.AddFast(ce.lset, ce.ref, t, v); err {
			case nil:
				if tp == nil {
					sl.cache.trackStaleness(ce.hash, ce.lset)
				}
			case storage.ErrNotFound:
				ok = false
			case storage.ErrOutOfOrderSample:
				numOutOfOrder++
				level.Debug(sl.l).Log("msg", "Out of order sample", "series", string(met))
				targetScrapeSampleOutOfOrder.Inc()
				continue
			case storage.ErrDuplicateSampleForTimestamp:
				numDuplicates++
				level.Debug(sl.l).Log("msg", "Duplicate sample for timestamp", "series", string(met))
				targetScrapeSampleDuplicate.Inc()
				continue
			case storage.ErrOutOfBounds:
				numOutOfBounds++
				level.Debug(sl.l).Log("msg", "Out of bounds metric", "series", string(met))
				targetScrapeSampleOutOfBounds.Inc()
				continue
			case errSampleLimit:
				// Keep on parsing output if we hit the limit, so we report the correct
				// total number of samples scraped.
				sampleLimitErr = err
				added++
				continue
			default:
				break loop
			}
		}
		if !ok {
			var lset labels.Labels

			mets := p.Metric(&lset)
			hash := lset.Hash()

			// Hash label set as it is seen local to the target. Then add target labels
			// and relabeling and store the final label set.
			lset = sl.sampleMutator(lset)

			// The label set may be set to nil to indicate dropping.
			if lset == nil {
				sl.cache.addDropped(mets)
				continue
			}

			var ref uint64
			ref, err = app.Add(lset, t, v)
			switch err {
			case nil:
			case storage.ErrOutOfOrderSample:
				err = nil
				numOutOfOrder++
				level.Debug(sl.l).Log("msg", "Out of order sample", "series", string(met))
				targetScrapeSampleOutOfOrder.Inc()
				continue
			case storage.ErrDuplicateSampleForTimestamp:
				err = nil
				numDuplicates++
				level.Debug(sl.l).Log("msg", "Duplicate sample for timestamp", "series", string(met))
				targetScrapeSampleDuplicate.Inc()
				continue
			case storage.ErrOutOfBounds:
				err = nil
				numOutOfBounds++
				level.Debug(sl.l).Log("msg", "Out of bounds metric", "series", string(met))
				targetScrapeSampleOutOfBounds.Inc()
				continue
			case errSampleLimit:
				sampleLimitErr = err
				added++
				continue
			default:
				level.Debug(sl.l).Log("msg", "unexpected error", "series", string(met), "err", err)
				break loop
			}
			if tp == nil {
				// Bypass staleness logic if there is an explicit timestamp.
				sl.cache.trackStaleness(hash, lset)
			}
			sl.cache.addRef(mets, ref, lset, hash)
		}
		added++
	}
	if sampleLimitErr != nil {
		if err == nil {
			err = sampleLimitErr
		}
		// We only want to increment this once per scrape, so this is Inc'd outside the loop.
		targetScrapeSampleLimit.Inc()
	}
	if numOutOfOrder > 0 {
		level.Warn(sl.l).Log("msg", "Error on ingesting out-of-order samples", "num_dropped", numOutOfOrder)
	}
	if numDuplicates > 0 {
		level.Warn(sl.l).Log("msg", "Error on ingesting samples with different value but same timestamp", "num_dropped", numDuplicates)
	}
	if numOutOfBounds > 0 {
		level.Warn(sl.l).Log("msg", "Error on ingesting samples that are too old or are too far into the future", "num_dropped", numOutOfBounds)
	}
	if err == nil {
		sl.cache.forEachStale(func(lset labels.Labels) bool {
			// Series no longer exposed, mark it stale.
			_, err = app.Add(lset, defTime, math.Float64frombits(value.StaleNaN))
			switch err {
			case storage.ErrOutOfOrderSample, storage.ErrDuplicateSampleForTimestamp:
				// Do not count these in logging, as this is expected if a target
				// goes away and comes back again with a new scrape loop.
				err = nil
			}
			return err == nil
		})
	}
	if err != nil {
		app.Rollback()
		return total, added, err
	}
	// 写入tsdb
	if err := app.Commit(); err != nil {
		return total, added, err
	}

	// Only perform cache cleaning if the scrape was not empty.
	// An empty scrape (usually) is used to indicate a failed scrape.
	sl.cache.iterDone(len(b) > 0)

	return total, added, nil
}

func yoloString(b []byte) string {
	return *((*string)(unsafe.Pointer(&b)))
}

// The constants are suffixed with the invalid \xff unicode rune to avoid collisions
// with scraped metrics in the cache.
const (
	scrapeHealthMetricName       = "up" + "\xff"
	scrapeDurationMetricName     = "scrape_duration_seconds" + "\xff"
	scrapeSamplesMetricName      = "scrape_samples_scraped" + "\xff"
	samplesPostRelabelMetricName = "scrape_samples_post_metric_relabeling" + "\xff"
)

func (sl *scrapeLoop) report(start time.Time, duration time.Duration, scraped, appended int, err error) error {
	sl.scraper.report(start, duration, err)

	ts := timestamp.FromTime(start)

	var health float64
	if err == nil {
		// 如果err为nil，则设置health为1
		health = 1
	}
	app := sl.appender()

	if err := sl.addReportSample(app, scrapeHealthMetricName, ts, health); err != nil {
		app.Rollback()
		return err
	}
	if err := sl.addReportSample(app, scrapeDurationMetricName, ts, duration.Seconds()); err != nil {
		app.Rollback()
		return err
	}
	if err := sl.addReportSample(app, scrapeSamplesMetricName, ts, float64(scraped)); err != nil {
		app.Rollback()
		return err
	}
	if err := sl.addReportSample(app, samplesPostRelabelMetricName, ts, float64(appended)); err != nil {
		app.Rollback()
		return err
	}
	return app.Commit()
}

func (sl *scrapeLoop) reportStale(start time.Time) error {
	ts := timestamp.FromTime(start)
	app := sl.appender()

	stale := math.Float64frombits(value.StaleNaN)

	if err := sl.addReportSample(app, scrapeHealthMetricName, ts, stale); err != nil {
		app.Rollback()
		return err
	}
	if err := sl.addReportSample(app, scrapeDurationMetricName, ts, stale); err != nil {
		app.Rollback()
		return err
	}
	if err := sl.addReportSample(app, scrapeSamplesMetricName, ts, stale); err != nil {
		app.Rollback()
		return err
	}
	if err := sl.addReportSample(app, samplesPostRelabelMetricName, ts, stale); err != nil {
		app.Rollback()
		return err
	}
	return app.Commit()
}

func (sl *scrapeLoop) addReportSample(app storage.Appender, s string, t int64, v float64) error {
	ce, ok := sl.cache.get(s)
	if ok {
		err := app.AddFast(ce.lset, ce.ref, t, v)
		switch err {
		case nil:
			return nil
		case storage.ErrNotFound:
			// Try an Add.
		case storage.ErrOutOfOrderSample, storage.ErrDuplicateSampleForTimestamp:
			// Do not log here, as this is expected if a target goes away and comes back
			// again with a new scrape loop.
			return nil
		default:
			return err
		}
	}
	lset := labels.Labels{
		// The constants are suffixed with the invalid \xff unicode rune to avoid collisions
		// with scraped metrics in the cache.
		// We have to drop it when building the actual metric.
		labels.Label{Name: labels.MetricName, Value: s[:len(s)-1]},
	}

	hash := lset.Hash()
	lset = sl.reportSampleMutator(lset)

	ref, err := app.Add(lset, t, v)
	switch err {
	case nil:
		sl.cache.addRef(s, ref, lset, hash)
		return nil
	case storage.ErrOutOfOrderSample, storage.ErrDuplicateSampleForTimestamp:
		return nil
	default:
		return err
	}
}
