// Copyright 2013 The Prometheus Authors
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
	"fmt"
	"hash/fnv"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"

	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/relabel"
	"github.com/prometheus/prometheus/pkg/textparse"
	"github.com/prometheus/prometheus/pkg/value"
	"github.com/prometheus/prometheus/storage"
)

// TargetHealth describes the health state of a target.
// TargetHealth描述了一个target的健康状况
type TargetHealth string

// The possible health states of a target based on the last performed scrape.
const (
	HealthUnknown TargetHealth = "unknown"
	HealthGood    TargetHealth = "up"
	HealthBad     TargetHealth = "down"
)

// Target refers to a singular HTTP or HTTPS endpoint.
// Target代表一个HTTP或者HTTPS endpoint
type Target struct {
	// Labels before any processing.
	// 在进行relabel处理之前的Labels
	// 其实已经添加了job, __scheme__, __metrics_path__这些额外的labels
	discoveredLabels labels.Labels
	// Any labels that are added to this target and its metrics.
	// 任何增加到这个target以及它的metrics的labels
	labels labels.Labels
	// Additional URL parmeters that are part of the target URL.
	// target URL中额外的URL parameters
	params url.Values

	mtx                sync.RWMutex
	lastError          error
	lastScrape         time.Time
	lastScrapeDuration time.Duration
	health             TargetHealth
	metadata           metricMetadataStore
}

// NewTarget creates a reasonably configured target for querying.
// NewTarget创建一个合理配置的target用于查询
func NewTarget(labels, discoveredLabels labels.Labels, params url.Values) *Target {
	return &Target{
		labels:           labels,
		discoveredLabels: discoveredLabels,
		params:           params,
		health:           HealthUnknown,
	}
}

func (t *Target) String() string {
	return t.URL().String()
}

type metricMetadataStore interface {
	listMetadata() []MetricMetadata
	getMetadata(metric string) (MetricMetadata, bool)
}

// MetricMetadata is a piece of metadata for a metric.
// MetricMetadata是一个metric的一系列元数据
type MetricMetadata struct {
	Metric string
	Type   textparse.MetricType
	Help   string
	Unit   string
}

func (t *Target) MetadataList() []MetricMetadata {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	if t.metadata == nil {
		return nil
	}
	return t.metadata.listMetadata()
}

// Metadata returns type and help metadata for the given metric.
func (t *Target) Metadata(metric string) (MetricMetadata, bool) {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	if t.metadata == nil {
		return MetricMetadata{}, false
	}
	return t.metadata.getMetadata(metric)
}

func (t *Target) setMetadataStore(s metricMetadataStore) {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	// 设置target的metadata
	t.metadata = s
}

// hash returns an identifying hash for the target.
// hash返回一个target的identifying hash
func (t *Target) hash() uint64 {
	h := fnv.New64a()
	// 哈希值主要依据labels和url进行计算获得
	h.Write([]byte(fmt.Sprintf("%016d", t.labels.Hash())))
	h.Write([]byte(t.URL().String()))

	return h.Sum64()
}

// offset returns the time until the next scrape cycle for the target.
// It includes the global server jitterSeed for scrapes from multiple Prometheus to try to be at different times.
func (t *Target) offset(interval time.Duration, jitterSeed uint64) time.Duration {
	now := time.Now().UnixNano()

	// Base is a pinned to absolute time, no matter how often offset is called.
	var (
		base   = int64(interval) - now%int64(interval)
		offset = (t.hash() ^ jitterSeed) % uint64(interval)
		next   = base + int64(offset)
	)

	if next > int64(interval) {
		next -= int64(interval)
	}
	return time.Duration(next)
}

// Labels returns a copy of the set of all public labels of the target.
// Labels返回target的所有public labels集合的拷贝，剔除所有以__开头的labels
func (t *Target) Labels() labels.Labels {
	lset := make(labels.Labels, 0, len(t.labels))
	for _, l := range t.labels {
		if !strings.HasPrefix(l.Name, model.ReservedLabelPrefix) {
			lset = append(lset, l)
		}
	}
	return lset
}

// DiscoveredLabels returns a copy of the target's labels before any processing.
// DiscoveredLabels返回在没有经过任何处理的targets的labels的一份拷贝
func (t *Target) DiscoveredLabels() labels.Labels {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	lset := make(labels.Labels, len(t.discoveredLabels))
	copy(lset, t.discoveredLabels)
	return lset
}

// SetDiscoveredLabels sets new DiscoveredLabels
// SetDiscoveredLabels设置新的DiscoveredLabels
func (t *Target) SetDiscoveredLabels(l labels.Labels) {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	t.discoveredLabels = l
}

// URL returns a copy of the target's URL.
// URL返回一个target的URL的拷贝
func (t *Target) URL() *url.URL {
	params := url.Values{}

	for k, v := range t.params {
		params[k] = make([]string, len(v))
		copy(params[k], v)
	}
	for _, l := range t.labels {
		// 跳过所有不是以__param_开头的labels
		if !strings.HasPrefix(l.Name, model.ParamLabelPrefix) {
			continue
		}
		// 去除label name中的__param_
		ks := l.Name[len(model.ParamLabelPrefix):]

		if len(params[ks]) > 0 {
			// 设置prams的第一个值为l.Value
			params[ks][0] = l.Value
		} else {
			params[ks] = []string{l.Value}
		}
	}

	return &url.URL{
		Scheme:   t.labels.Get(model.SchemeLabel),
		Host:     t.labels.Get(model.AddressLabel),
		Path:     t.labels.Get(model.MetricsPathLabel),
		RawQuery: params.Encode(),
	}
}

// report用于记录当前target的状态
func (t *Target) report(start time.Time, dur time.Duration, err error) {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	if err == nil {
		t.health = HealthGood
	} else {
		t.health = HealthBad
	}

	t.lastError = err
	t.lastScrape = start
	t.lastScrapeDuration = dur
}

// LastError returns the error encountered during the last scrape.
func (t *Target) LastError() error {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	return t.lastError
}

// LastScrape returns the time of the last scrape.
func (t *Target) LastScrape() time.Time {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	return t.lastScrape
}

// LastScrapeDuration returns how long the last scrape of the target took.
func (t *Target) LastScrapeDuration() time.Duration {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	return t.lastScrapeDuration
}

// Health returns the last known health state of the target.
func (t *Target) Health() TargetHealth {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	return t.health
}

// Targets is a sortable list of targets.
type Targets []*Target

func (ts Targets) Len() int           { return len(ts) }
func (ts Targets) Less(i, j int) bool { return ts[i].URL().String() < ts[j].URL().String() }
func (ts Targets) Swap(i, j int)      { ts[i], ts[j] = ts[j], ts[i] }

var errSampleLimit = errors.New("sample limit exceeded")

// limitAppender limits the number of total appended samples in a batch.
type limitAppender struct {
	storage.Appender

	limit int
	i     int
}

func (app *limitAppender) Add(lset labels.Labels, t int64, v float64) (uint64, error) {
	if !value.IsStaleNaN(v) {
		app.i++
		if app.i > app.limit {
			return 0, errSampleLimit
		}
	}
	ref, err := app.Appender.Add(lset, t, v)
	if err != nil {
		return 0, err
	}
	return ref, nil
}

func (app *limitAppender) AddFast(lset labels.Labels, ref uint64, t int64, v float64) error {
	if !value.IsStaleNaN(v) {
		app.i++
		if app.i > app.limit {
			return errSampleLimit
		}
	}
	err := app.Appender.AddFast(lset, ref, t, v)
	return err
}

type timeLimitAppender struct {
	storage.Appender

	maxTime int64
}

func (app *timeLimitAppender) Add(lset labels.Labels, t int64, v float64) (uint64, error) {
	if t > app.maxTime {
		return 0, storage.ErrOutOfBounds
	}

	ref, err := app.Appender.Add(lset, t, v)
	if err != nil {
		return 0, err
	}
	return ref, nil
}

func (app *timeLimitAppender) AddFast(lset labels.Labels, ref uint64, t int64, v float64) error {
	if t > app.maxTime {
		return storage.ErrOutOfBounds
	}
	err := app.Appender.AddFast(lset, ref, t, v)
	return err
}

// populateLabels builds a label set from the given label set and scrape configuration.
// It returns a label set before relabeling was applied as the second return value.
// Returns the original discovered label set found before relabelling was applied if the target is dropped during relabeling.
// populateLabels从给定的label set和scrape configuration构建一个label set
// 它在relabeling应用之前返回一个label set作为第二个返回参数
// 返回在relabeling应用之前的original discovered label，如果target在relabeling的时候被丢弃了
func populateLabels(lset labels.Labels, cfg *config.ScrapeConfig) (res, orig labels.Labels, err error) {
	// Copy labels into the labelset for the target if they are not set already.
	// 将labels拷贝到target的labelset，如果它们还没设置的话
	scrapeLabels := []labels.Label{
		{Name: model.JobLabel, Value: cfg.JobName},
		{Name: model.MetricsPathLabel, Value: cfg.MetricsPath},
		{Name: model.SchemeLabel, Value: cfg.Scheme},
	}
	lb := labels.NewBuilder(lset)

	for _, l := range scrapeLabels {
		// 如果lset中没有的话，将根据配置文件生成的scrapeLabels写入
		if lv := lset.Get(l.Name); lv == "" {
			lb.Set(l.Name, l.Value)
		}
	}
	// Encode scrape query parameters as labels.
	// 将scrape query parameters作为labels进行编码
	for k, v := range cfg.Params {
		if len(v) > 0 {
			lb.Set(model.ParamLabelPrefix+k, v[0])
		}
	}

	preRelabelLabels := lb.Labels()
	// 进行relabel处理
	lset = relabel.Process(preRelabelLabels, cfg.RelabelConfigs...)

	// Check if the target was dropped.
	if lset == nil {
		// lset为nil说明target被丢弃了
		return nil, preRelabelLabels, nil
	}
	// 查看__address__ label是否被设置
	if v := lset.Get(model.AddressLabel); v == "" {
		return nil, nil, errors.New("no address")
	}

	lb = labels.NewBuilder(lset)

	// addPort checks whether we should add a default port to the address.
	// If the address is not valid, we don't append a port either.
	// addPort检查我们是否需要给地址增加一个默认的端口
	// 如果地址不合法，我们同样不添加端口
	addPort := func(s string) bool {
		// If we can split, a port exists and we don't have to add one.
		// 如果我们可以split，说明端口存在，我们不需要再为它添加一个
		if _, _, err := net.SplitHostPort(s); err == nil {
			return false
		}
		// If adding a port makes it valid, the previous error
		// was not due to an invalid address and we can append a port.
		// 如果增加一个端口就让SplitHostPort合法了，说明之前的错误并不是因为地址不合法
		// 因此我们可以增加一个port
		_, _, err := net.SplitHostPort(s + ":1234")
		return err == nil
	}
	addr := lset.Get(model.AddressLabel)
	// If it's an address with no trailing port, infer it based on the used scheme.
	// 如果一个地址没有端口，则根据它的scheme进行推测
	if addPort(addr) {
		// Addresses reaching this point are already wrapped in [] if necessary.
		// 如果有必要的话，根据scheme为地址添加一个端口
		switch lset.Get(model.SchemeLabel) {
		case "http", "":
			addr = addr + ":80"
		case "https":
			addr = addr + ":443"
		default:
			return nil, nil, errors.Errorf("invalid scheme: %q", cfg.Scheme)
		}
		lb.Set(model.AddressLabel, addr)
	}

	if err := config.CheckTargetAddress(model.LabelValue(addr)); err != nil {
		return nil, nil, err
	}

	// Meta labels are deleted after relabelling. Other internal labels propagate to
	// the target which decides whether they will be part of their label set.
	// Meta labels在relabelling之后都会被删除
	for _, l := range lset {
		if strings.HasPrefix(l.Name, model.MetaLabelPrefix) {
			lb.Del(l.Name)
		}
	}

	// Default the instance label to the target address.
	// 默认将instance label设置为target address
	if v := lset.Get(model.InstanceLabel); v == "" {
		lb.Set(model.InstanceLabel, addr)
	}

	res = lb.Labels()
	for _, l := range res {
		// Check label values are valid, drop the target if not.
		// 检查label values是否合法，不合法则丢弃target
		if !model.LabelValue(l.Value).IsValid() {
			return nil, nil, errors.Errorf("invalid label value for %q: %q", l.Name, l.Value)
		}
	}
	// 第一个返回值是relabel之后的labels，第二个参数为relabel之前的参数
	return res, preRelabelLabels, nil
}

// targetsFromGroup builds targets based on the given TargetGroup and config.
// targetsFromGroup基于给定的TargetGroup和config构建targets
func targetsFromGroup(tg *targetgroup.Group, cfg *config.ScrapeConfig) ([]*Target, error) {
	targets := make([]*Target, 0, len(tg.Targets))

	for i, tlset := range tg.Targets {
		// 一个target的labels包含所在target group的公共labels以及其独有的label set
		lbls := make([]labels.Label, 0, len(tlset)+len(tg.Labels))

		// 添加某个target特定的labels
		for ln, lv := range tlset {
			lbls = append(lbls, labels.Label{Name: string(ln), Value: string(lv)})
		}
		// 增加group共有的labels
		for ln, lv := range tg.Labels {
			if _, ok := tlset[ln]; !ok {
				lbls = append(lbls, labels.Label{Name: string(ln), Value: string(lv)})
			}
		}

		lset := labels.New(lbls...)

		// 进行relabel
		lbls, origLabels, err := populateLabels(lset, cfg)
		if err != nil {
			return nil, errors.Wrapf(err, "instance %d in group %s", i, tg)
		}
		if lbls != nil || origLabels != nil {
			// 如果在被relabel之前或之后，有一个不为nil，则加入targets
			// 一个target由自发现的label，加经过relabel处理后的labels，和cfg.Params组成
			targets = append(targets, NewTarget(lbls, origLabels, cfg.Params))
		}
	}
	return targets, nil
}
