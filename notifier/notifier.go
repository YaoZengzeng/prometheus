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

package notifier

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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
	"github.com/prometheus/prometheus/pkg/relabel"
)

const (
	alertPushEndpoint = "/api/v1/alerts"
	contentTypeJSON   = "application/json"
)

// String constants for instrumentation.
const (
	namespace         = "prometheus"
	subsystem         = "notifications"
	alertmanagerLabel = "alertmanager"
)

var userAgent = fmt.Sprintf("Prometheus/%s", version.Version)

// Alert is a generic representation of an alert in the Prometheus eco-system.
// Alert是一个alert在Prometheus的生态环境中的通用表示
type Alert struct {
	// Label value pairs for purpose of aggregation, matching, and disposition
	// dispatching. This must minimally include an "alertname" label.
	// Label value pairs用于聚合，匹配以及disposition dispatching
	// 它必须至少包含一个"alertname"的label
	Labels labels.Labels `json:"labels"`

	// Extra key/value information which does not define alert identity.
	// 额外的键值信息，不会定义alert identity
	Annotations labels.Labels `json:"annotations"`

	// The known time range for this alert. Both ends are optional.
	StartsAt     time.Time `json:"startsAt,omitempty"`
	EndsAt       time.Time `json:"endsAt,omitempty"`
	GeneratorURL string    `json:"generatorURL,omitempty"`
}

// Name returns the name of the alert. It is equivalent to the "alertname" label.
func (a *Alert) Name() string {
	return a.Labels.Get(labels.AlertName)
}

// Hash returns a hash over the alert. It is equivalent to the alert labels hash.
func (a *Alert) Hash() uint64 {
	return a.Labels.Hash()
}

func (a *Alert) String() string {
	s := fmt.Sprintf("%s[%s]", a.Name(), fmt.Sprintf("%016x", a.Hash())[:7])
	if a.Resolved() {
		return s + "[resolved]"
	}
	return s + "[active]"
}

// Resolved returns true iff the activity interval ended in the past.
func (a *Alert) Resolved() bool {
	return a.ResolvedAt(time.Now())
}

// ResolvedAt returns true iff the activity interval ended before
// the given timestamp.
func (a *Alert) ResolvedAt(ts time.Time) bool {
	if a.EndsAt.IsZero() {
		return false
	}
	return !a.EndsAt.After(ts)
}

// Manager is responsible for dispatching alert notifications to an
// alert manager service.
// Manager负责将alert notifications分发给一个alert manager service
type Manager struct {
	queue []*Alert
	opts  *Options

	metrics *alertMetrics

	more   chan struct{}
	mtx    sync.RWMutex
	ctx    context.Context
	cancel func()

	alertmanagers map[string]*alertmanagerSet
	logger        log.Logger
}

// Options are the configurable parameters of a Handler.
type Options struct {
	QueueCapacity  int
	ExternalLabels labels.Labels
	RelabelConfigs []*relabel.Config
	// Used for sending HTTP requests to the Alertmanager.
	// Do用于发送HTTP请求给AlertManager
	Do func(ctx context.Context, client *http.Client, req *http.Request) (*http.Response, error)

	Registerer prometheus.Registerer
}

type alertMetrics struct {
	latency                 *prometheus.SummaryVec
	errors                  *prometheus.CounterVec
	sent                    *prometheus.CounterVec
	dropped                 prometheus.Counter
	queueLength             prometheus.GaugeFunc
	queueCapacity           prometheus.Gauge
	alertmanagersDiscovered prometheus.GaugeFunc
}

func newAlertMetrics(r prometheus.Registerer, queueCap int, queueLen, alertmanagersDiscovered func() float64) *alertMetrics {
	m := &alertMetrics{
		latency: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "latency_seconds",
			Help:      "Latency quantiles for sending alert notifications.",
		},
			[]string{alertmanagerLabel},
		),
		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "errors_total",
			Help:      "Total number of errors sending alert notifications.",
		},
			[]string{alertmanagerLabel},
		),
		sent: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "sent_total",
			Help:      "Total number of alerts sent.",
		},
			[]string{alertmanagerLabel},
		),
		dropped: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "dropped_total",
			Help:      "Total number of alerts dropped due to errors when sending to Alertmanager.",
		}),
		queueLength: prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "queue_length",
			Help:      "The number of alert notifications in the queue.",
		}, queueLen),
		queueCapacity: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "queue_capacity",
			Help:      "The capacity of the alert notifications queue.",
		}),
		alertmanagersDiscovered: prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "prometheus_notifications_alertmanagers_discovered",
			Help: "The number of alertmanagers discovered and active.",
		}, alertmanagersDiscovered),
	}

	m.queueCapacity.Set(float64(queueCap))

	if r != nil {
		r.MustRegister(
			m.latency,
			m.errors,
			m.sent,
			m.dropped,
			m.queueLength,
			m.queueCapacity,
			m.alertmanagersDiscovered,
		)
	}

	return m
}

// do函数作为默认的请求发送函数
func do(ctx context.Context, client *http.Client, req *http.Request) (*http.Response, error) {
	if client == nil {
		client = http.DefaultClient
	}
	return client.Do(req.WithContext(ctx))
}

// NewManager is the manager constructor.
func NewManager(o *Options, logger log.Logger) *Manager {
	ctx, cancel := context.WithCancel(context.Background())

	if o.Do == nil {
		o.Do = do
	}
	if logger == nil {
		logger = log.NewNopLogger()
	}

	n := &Manager{
		queue:  make([]*Alert, 0, o.QueueCapacity),
		ctx:    ctx,
		cancel: cancel,
		more:   make(chan struct{}, 1),
		opts:   o,
		logger: logger,
	}

	queueLenFunc := func() float64 { return float64(n.queueLen()) }
	alertmanagersDiscoveredFunc := func() float64 { return float64(len(n.Alertmanagers())) }

	n.metrics = newAlertMetrics(
		o.Registerer,
		o.QueueCapacity,
		queueLenFunc,
		alertmanagersDiscoveredFunc,
	)

	return n
}

// ApplyConfig updates the status state as the new config requires.
// ApplyConfig根据新的配置的需求更新状态
func (n *Manager) ApplyConfig(conf *config.Config) error {
	n.mtx.Lock()
	defer n.mtx.Unlock()

	n.opts.ExternalLabels = conf.GlobalConfig.ExternalLabels
	n.opts.RelabelConfigs = conf.AlertingConfig.AlertRelabelConfigs

	amSets := make(map[string]*alertmanagerSet)

	// 创建新的AlertmanagerSet
	for _, cfg := range conf.AlertingConfig.AlertmanagerConfigs {
		// 根据config创建Alertmanager Set
		ams, err := newAlertmanagerSet(cfg, n.logger)
		if err != nil {
			return err
		}

		ams.metrics = n.metrics

		// The config hash is used for the map lookup identifier.
		// config hash用于map lookup的标识符
		b, err := json.Marshal(cfg)
		if err != nil {
			return err
		}
		// 将config的json编码的md5值作为索引
		amSets[fmt.Sprintf("%x", md5.Sum(b))] = ams
	}

	n.alertmanagers = amSets

	return nil
}

// maxBatchSize为64
const maxBatchSize = 64

func (n *Manager) queueLen() int {
	n.mtx.RLock()
	defer n.mtx.RUnlock()

	return len(n.queue)
}

// 从manager中取出maxBatchSize个Alert，如果不足的话，取出全部
func (n *Manager) nextBatch() []*Alert {
	n.mtx.Lock()
	defer n.mtx.Unlock()

	var alerts []*Alert

	if len(n.queue) > maxBatchSize {
		alerts = append(make([]*Alert, 0, maxBatchSize), n.queue[:maxBatchSize]...)
		n.queue = n.queue[maxBatchSize:]
	} else {
		alerts = append(make([]*Alert, 0, len(n.queue)), n.queue...)
		n.queue = n.queue[:0]
	}

	return alerts
}

// Run dispatches notifications continuously.
// Run持续地分发notifications
func (n *Manager) Run(tsets <-chan map[string][]*targetgroup.Group) {

	for {
		select {
		case <-n.ctx.Done():
			return
		case ts := <-tsets:
			// 从tsets中加载新的targetgroup
			n.reload(ts)
		case <-n.more:
			// 说明队列中有item需要分发
		}
		alerts := n.nextBatch()

		if !n.sendAll(alerts...) {
			n.metrics.dropped.Add(float64(len(alerts)))
		}
		// If the queue still has items left, kick off the next iteration.
		// 如果queue中还有items，则启动下一轮迭代
		if n.queueLen() > 0 {
			n.setMore()
		}
	}
}

func (n *Manager) reload(tgs map[string][]*targetgroup.Group) {
	n.mtx.Lock()
	defer n.mtx.Unlock()

	for id, tgroup := range tgs {
		am, ok := n.alertmanagers[id]
		if !ok {
			level.Error(n.logger).Log("msg", "couldn't sync alert manager set", "err", fmt.Sprintf("invalid id:%v", id))
			continue
		}
		am.sync(tgroup)
	}
}

// Send queues the given notification requests for processing.
// Panics if called on a handler that is not running.
// Send将给定的notification requests进行排队处理
// Panics如果调用的handler不在运行
func (n *Manager) Send(alerts ...*Alert) {
	n.mtx.Lock()
	defer n.mtx.Unlock()

	// Attach external labels before relabelling and sending.
	// 在进行relabelling和sending之前，先关联external labels
	for _, a := range alerts {
		lb := labels.NewBuilder(a.Labels)

		for _, l := range n.opts.ExternalLabels {
			if a.Labels.Get(l.Name) == "" {
				lb.Set(l.Name, l.Value)
			}
		}

		a.Labels = lb.Labels()
	}

	alerts = n.relabelAlerts(alerts)

	// Queue capacity should be significantly larger than a single alert
	// batch could be.
	// Queue capacity应该比单个的alert batch要大
	if d := len(alerts) - n.opts.QueueCapacity; d > 0 {
		// 丢弃前d个alerts
		alerts = alerts[d:]

		level.Warn(n.logger).Log("msg", "Alert batch larger than queue capacity, dropping alerts", "num_dropped", d)
		n.metrics.dropped.Add(float64(d))
	}

	// If the queue is full, remove the oldest alerts in favor
	// of newer ones.
	// 如果队列满了，则移除旧的alerts
	if d := (len(n.queue) + len(alerts)) - n.opts.QueueCapacity; d > 0 {
		n.queue = n.queue[d:]

		level.Warn(n.logger).Log("msg", "Alert notification queue full, dropping alerts", "num_dropped", d)
		n.metrics.dropped.Add(float64(d))
	}
	n.queue = append(n.queue, alerts...)

	// Notify sending goroutine that there are alerts to be processed.
	// 通知发送的goroutine有alerts需要处理
	n.setMore()
}

func (n *Manager) relabelAlerts(alerts []*Alert) []*Alert {
	var relabeledAlerts []*Alert

	for _, alert := range alerts {
		labels := relabel.Process(alert.Labels, n.opts.RelabelConfigs...)
		if labels != nil {
			// 如果relabel的结果不为nil,则进行重新赋值并且append到relabelAlerts
			alert.Labels = labels
			relabeledAlerts = append(relabeledAlerts, alert)
		}
	}
	return relabeledAlerts
}

// setMore signals that the alert queue has items.
// setMore表示alert queue中有内容了
func (n *Manager) setMore() {
	// If we cannot send on the channel, it means the signal already exists
	// and has not been consumed yet.
	// 如果我们不能发送到channel，则表明信号已经存在了，还没有被消费
	select {
	case n.more <- struct{}{}:
	default:
	}
}

// Alertmanagers returns a slice of Alertmanager URLs.
// Alertmanagers返回一系列的Alertmanager URLs
func (n *Manager) Alertmanagers() []*url.URL {
	n.mtx.RLock()
	amSets := n.alertmanagers
	n.mtx.RUnlock()

	var res []*url.URL

	for _, ams := range amSets {
		ams.mtx.RLock()
		for _, am := range ams.ams {
			res = append(res, am.url())
		}
		ams.mtx.RUnlock()
	}

	return res
}

// DroppedAlertmanagers returns a slice of Alertmanager URLs.
func (n *Manager) DroppedAlertmanagers() []*url.URL {
	n.mtx.RLock()
	amSets := n.alertmanagers
	n.mtx.RUnlock()

	var res []*url.URL

	for _, ams := range amSets {
		ams.mtx.RLock()
		for _, dam := range ams.droppedAms {
			res = append(res, dam.url())
		}
		ams.mtx.RUnlock()
	}

	return res
}

// sendAll sends the alerts to all configured Alertmanagers concurrently.
// It returns true if the alerts could be sent successfully to at least one Alertmanager.
// sendAll并行地将alerts发送给所有配置好的Alertmanagers
// 它返回true，如果alerts至少成功地发送给了一个Alertmanager
func (n *Manager) sendAll(alerts ...*Alert) bool {
	begin := time.Now()

	// 将alerts进行marshal
	b, err := json.Marshal(alerts)
	if err != nil {
		level.Error(n.logger).Log("msg", "Encoding alerts failed", "err", err)
		return false
	}

	n.mtx.RLock()
	amSets := n.alertmanagers
	n.mtx.RUnlock()

	var (
		wg         sync.WaitGroup
		numSuccess uint64
	)
	for _, ams := range amSets {
		ams.mtx.RLock()

		for _, am := range ams.ams {
			wg.Add(1)

			ctx, cancel := context.WithTimeout(n.ctx, time.Duration(ams.cfg.Timeout))
			defer cancel()

			go func(ams *alertmanagerSet, am alertmanager) {
				// 获取alertmanager的url
				u := am.url().String()

				if err := n.sendOne(ctx, ams.client, u, b); err != nil {
					level.Error(n.logger).Log("alertmanager", u, "count", len(alerts), "msg", "Error sending alert", "err", err)
					n.metrics.errors.WithLabelValues(u).Inc()
				} else {
					atomic.AddUint64(&numSuccess, 1)
				}
				n.metrics.latency.WithLabelValues(u).Observe(time.Since(begin).Seconds())
				n.metrics.sent.WithLabelValues(u).Add(float64(len(alerts)))

				wg.Done()
			}(ams, am)
		}
		ams.mtx.RUnlock()
	}
	wg.Wait()

	return numSuccess > 0
}

func (n *Manager) sendOne(ctx context.Context, c *http.Client, url string, b []byte) error {
	req, err := http.NewRequest("POST", url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Content-Type", contentTypeJSON)
	// 发送HTTP请求
	resp, err := n.opts.Do(ctx, c, req)
	if err != nil {
		return err
	}
	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()

	// Any HTTP status 2xx is OK.
	// 任何HTTP的状态为2xx都是正确的
	if resp.StatusCode/100 != 2 {
		return errors.Errorf("bad response status %s", resp.Status)
	}
	return err
}

// Stop shuts down the notification handler.
// Stop关闭notification handler
func (n *Manager) Stop() {
	level.Info(n.logger).Log("msg", "Stopping notification manager...")
	n.cancel()
}

// alertmanager holds Alertmanager endpoint information.
// alertmanager维护了Alertmanager的endpoint信息
type alertmanager interface {
	url() *url.URL
}

type alertmanagerLabels struct{ labels.Labels }

const pathLabel = "__alerts_path__"

func (a alertmanagerLabels) url() *url.URL {
	return &url.URL{
		Scheme: a.Get(model.SchemeLabel),
		Host:   a.Get(model.AddressLabel),
		Path:   a.Get(pathLabel),
	}
}

// alertmanagerSet contains a set of Alertmanagers discovered via a group of service
// discovery definitions that have a common configuration on how alerts should be sent.
// alertmanagerSet包含了一系列发现的Alertmanagers，通过一系列的service discovery的定义
// 有着共同的定义，alerts应该如何发送
type alertmanagerSet struct {
	cfg    *config.AlertmanagerConfig
	client *http.Client

	metrics *alertMetrics

	mtx        sync.RWMutex
	ams        []alertmanager
	droppedAms []alertmanager
	logger     log.Logger
}

func newAlertmanagerSet(cfg *config.AlertmanagerConfig, logger log.Logger) (*alertmanagerSet, error) {
	client, err := config_util.NewClientFromConfig(cfg.HTTPClientConfig, "alertmanager")
	if err != nil {
		return nil, err
	}
	s := &alertmanagerSet{
		client: client,
		cfg:    cfg,
		logger: logger,
	}
	return s, nil
}

// sync extracts a deduplicated set of Alertmanager endpoints from a list
// of target groups definitions.
// sync从一系列的target groups definitions中抽取出一系列重复的Alertmanager endpoints
func (s *alertmanagerSet) sync(tgs []*targetgroup.Group) {
	allAms := []alertmanager{}
	allDroppedAms := []alertmanager{}

	for _, tg := range tgs {
		// 从targetgroup构建alertmanager
		ams, droppedAms, err := alertmanagerFromGroup(tg, s.cfg)
		if err != nil {
			level.Error(s.logger).Log("msg", "Creating discovered Alertmanagers failed", "err", err)
			continue
		}
		allAms = append(allAms, ams...)
		allDroppedAms = append(allDroppedAms, droppedAms...)
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()
	// Set new Alertmanagers and deduplicate them along their unique URL.
	// 设置新的Alertmanagers并且根据URL进行去重
	s.ams = []alertmanager{}
	s.droppedAms = []alertmanager{}
	s.droppedAms = append(s.droppedAms, allDroppedAms...)
	seen := map[string]struct{}{}

	for _, am := range allAms {
		us := am.url().String()
		if _, ok := seen[us]; ok {
			continue
		}

		// This will initialize the Counters for the AM to 0.
		s.metrics.sent.WithLabelValues(us)
		s.metrics.errors.WithLabelValues(us)

		seen[us] = struct{}{}
		s.ams = append(s.ams, am)
	}
}

func postPath(pre string) string {
	return path.Join("/", pre, alertPushEndpoint)
}

// alertmanagersFromGroup extracts a list of alertmanagers from a target group
// and an associated AlertmanagerConfig.
// alertmanagersFromGroup从target group和相关的AlertmanagerConfig中抽取一系列的alertmanagers
func alertmanagerFromGroup(tg *targetgroup.Group, cfg *config.AlertmanagerConfig) ([]alertmanager, []alertmanager, error) {
	var res []alertmanager
	var droppedAlertManagers []alertmanager

	for _, tlset := range tg.Targets {
		lbls := make([]labels.Label, 0, len(tlset)+2+len(tg.Labels))

		for ln, lv := range tlset {
			lbls = append(lbls, labels.Label{Name: string(ln), Value: string(lv)})
		}
		// Set configured scheme as the initial scheme label for overwrite.
		// 设置初始的scheme label
		lbls = append(lbls, labels.Label{Name: model.SchemeLabel, Value: cfg.Scheme})
		lbls = append(lbls, labels.Label{Name: pathLabel, Value: postPath(cfg.PathPrefix)})

		// Combine target labels with target group labels.
		// 将target labels和target group labels融合
		for ln, lv := range tg.Labels {
			if _, ok := tlset[ln]; !ok {
				lbls = append(lbls, labels.Label{Name: string(ln), Value: string(lv)})
			}
		}

		// 进行relabel
		lset := relabel.Process(labels.New(lbls...), cfg.RelabelConfigs...)
		if lset == nil {
			droppedAlertManagers = append(droppedAlertManagers, alertmanagerLabels{lbls})
			continue
		}

		lb := labels.NewBuilder(lset)

		// addPort checks whether we should add a default port to the address.
		// If the address is not valid, we don't append a port either.
		// addPort检测我们是否应该给地址增加一个默认的端口
		// 如果地址是不合法的，我们也不会增加一个端口
		addPort := func(s string) bool {
			// If we can split, a port exists and we don't have to add one.
			if _, _, err := net.SplitHostPort(s); err == nil {
				return false
			}
			// If adding a port makes it valid, the previous error
			// was not due to an invalid address and we can append a port.
			_, _, err := net.SplitHostPort(s + ":1234")
			return err == nil
		}
		addr := lset.Get(model.AddressLabel)
		// If it's an address with no trailing port, infer it based on the used scheme.
		// 如果一个地址没有trailing port，根据scheme进行推测
		if addPort(addr) {
			// Addresses reaching this point are already wrapped in [] if necessary.
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
		// Meta labels会在relabelling之后删除，其他的internal labels传播到target，决定他们是否
		// 会成为label set的一部分
		for _, l := range lset {
			if strings.HasPrefix(l.Name, model.MetaLabelPrefix) {
				lb.Del(l.Name)
			}
		}

		res = append(res, alertmanagerLabels{lset})
	}
	return res, droppedAlertManagers, nil
}
