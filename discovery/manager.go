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

package discovery

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"

	sd_config "github.com/prometheus/prometheus/discovery/config"
	"github.com/prometheus/prometheus/discovery/targetgroup"

	"github.com/prometheus/prometheus/discovery/azure"
	"github.com/prometheus/prometheus/discovery/consul"
	"github.com/prometheus/prometheus/discovery/dns"
	"github.com/prometheus/prometheus/discovery/ec2"
	"github.com/prometheus/prometheus/discovery/file"
	"github.com/prometheus/prometheus/discovery/gce"
	"github.com/prometheus/prometheus/discovery/kubernetes"
	"github.com/prometheus/prometheus/discovery/marathon"
	"github.com/prometheus/prometheus/discovery/openstack"
	"github.com/prometheus/prometheus/discovery/triton"
	"github.com/prometheus/prometheus/discovery/zookeeper"
)

var (
	failedConfigs = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "prometheus_sd_configs_failed_total",
			// 加载失败的service discovery configurations的数目
			Help: "Total number of service discovery configurations that failed to load.",
		},
		[]string{"name"},
	)
	discoveredTargets = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "prometheus_sd_discovered_targets",
			// 当前的discovered targets的数目
			Help: "Current number of discovered targets.",
		},
		[]string{"name", "config"},
	)
	receivedUpdates = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			// 从SD providers收到的update events的数目
			Name: "prometheus_sd_received_updates_total",
			Help: "Total number of update events received from the SD providers.",
		},
		[]string{"name"},
	)
	delayedUpdates = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "prometheus_sd_updates_delayed_total",
			// 不能立即发送的update events的数目
			Help: "Total number of update events that couldn't be sent immediately.",
		},
		[]string{"name"},
	)
	sentUpdates = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "prometheus_sd_updates_total",
			// 发送给SD consumers的update events的数目
			Help: "Total number of update events sent to the SD consumers.",
		},
		[]string{"name"},
	)
)

func init() {
	prometheus.MustRegister(failedConfigs, discoveredTargets, receivedUpdates, delayedUpdates, sentUpdates)
}

// Discoverer provides information about target groups. It maintains a set
// of sources from which TargetGroups can originate. Whenever a discovery provider
// detects a potential change, it sends the TargetGroup through its channel.
// Discoverer提供关于target groups的信息，它维护了一系列能够产生TargetGroups的源
// 当一个discovery provider检测到一个潜在的change，它就会通过channel发送TargetGroup
//
// Discoverer does not know if an actual change happened.
// It does guarantee that it sends the new TargetGroup whenever a change happens.
// Discoverer不知道是否真的发生了改变，但是它确保在change发生的时候发送新的TargetGroup
//
// Discoverers should initially send a full set of all discoverable TargetGroups.
// Discoverers应该在初始的时候发送一系列可发现的TargetGroups
type Discoverer interface {
	// Run hands a channel to the discovery provider (Consul, DNS etc) through which it can send
	// updated target groups.
	// Run提供一个channel给discovery provider（Consul, DNS等），通过它能够发送更新的target groups
	// Must returns if the context gets canceled. It should not close the update
	// channel on returning.
	// 必须返回如果context被取消了，在返回的时候不应该关闭update channel
	Run(ctx context.Context, up chan<- []*targetgroup.Group)
}

type poolKey struct {
	// poolKey由setName和providerName组成
	setName  string
	provider string
}

// provider holds a Discoverer instance, its configuration and its subscribers.
// provider维护了一个Discoverer实例，它的配置以及subscribers
type provider struct {
	name   string
	d      Discoverer
	subs   []string
	config interface{}
}

// NewManager is the Discovery Manager constructor.
func NewManager(ctx context.Context, logger log.Logger, options ...func(*Manager)) *Manager {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	mgr := &Manager{
		logger:         logger,
		syncCh:         make(chan map[string][]*targetgroup.Group),
		targets:        make(map[poolKey]map[string]*targetgroup.Group),
		discoverCancel: []context.CancelFunc{},
		ctx:            ctx,
		updatert:       5 * time.Second,
		// triggerSend的大小为1
		triggerSend:    make(chan struct{}, 1),
	}
	// 应用options
	for _, option := range options {
		option(mgr)
	}
	return mgr
}

// Name sets the name of the manager.
func Name(n string) func(*Manager) {
	return func(m *Manager) {
		m.mtx.Lock()
		defer m.mtx.Unlock()
		m.name = n
	}
}

// Manager maintains a set of discovery providers and sends each update to a map channel.
// Targets are grouped by the target set name.
// Manager维护了一系列的discovery providers并且将每个更新发送到一个map channel
// Targets根据target set name被聚合
type Manager struct {
	logger         log.Logger
	name           string
	mtx            sync.RWMutex
	ctx            context.Context
	discoverCancel []context.CancelFunc

	// Some Discoverers(eg. k8s) send only the updates for a given target group
	// so we use map[tg.Source]*targetgroup.Group to know which group to update.
	// 例如k8s的Discoverers只发送给定的target group的updates
	// 因此我们使用map[tg.Source]*targetgroup.Group来知道更新哪个group
	targets map[poolKey]map[string]*targetgroup.Group
	// providers keeps track of SD providers.
	// providers用于追踪SD providers
	// provider中包含了Discoverer接口
	providers []*provider
	// The sync channel sends the updates as a map where the key is the job value from the scrape config.
	// sync channel将更新作为一个map发送，其中key是scrape config中的job value
	syncCh chan map[string][]*targetgroup.Group

	// How long to wait before sending updates to the channel. The variable
	// should only be modified in unit tests.
	// 在向channel发送updates之前等待的时间，只有在unit tests的时候才修改这个值
	updatert time.Duration

	// The triggerSend channel signals to the manager that new updates have been received from providers.
	// The triggerSend channel用于通知manager，从providers收到了新的updates
	triggerSend chan struct{}
}

// Run starts the background processing
// Run启动后台处理
func (m *Manager) Run() error {
	go m.sender()
	for range m.ctx.Done() {
		m.cancelDiscoverers()
		return m.ctx.Err()
	}
	return nil
}

// SyncCh returns a read only channel used by all the clients to receive target updates.
// SyncCh返回一个只读的channel，用于所有的clients接收target updates
func (m *Manager) SyncCh() <-chan map[string][]*targetgroup.Group {
	return m.syncCh
}

// ApplyConfig removes all running discovery providers and starts new ones using the provided config.
// ApplyConfig移除所有正在运行的discovery providers并且用提供的config启动一个新的
func (m *Manager) ApplyConfig(cfg map[string]sd_config.ServiceDiscoveryConfig) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	// setName其实对应的就是jobName
	for pk := range m.targets {
		// 如果不在cfg中的就标记删除
		if _, ok := cfg[pk.setName]; !ok {
			discoveredTargets.DeleteLabelValues(m.name, pk.setName)
		}
	}
	// 当重新应用config时，调用m.cancelDiscoverers()
	m.cancelDiscoverers()
	for name, scfg := range cfg {
		// 注册providers
		m.registerProviders(scfg, name)
		discoveredTargets.WithLabelValues(m.name, name).Set(0)
	}
	// 启动各个provider
	for _, prov := range m.providers {
		m.startProvider(m.ctx, prov)
	}

	return nil
}

// StartCustomProvider is used for sdtool. Only use this if you know what you're doing.
func (m *Manager) StartCustomProvider(ctx context.Context, name string, worker Discoverer) {
	p := &provider{
		name: name,
		d:    worker,
		subs: []string{name},
	}
	m.providers = append(m.providers, p)
	m.startProvider(ctx, p)
}

func (m *Manager) startProvider(ctx context.Context, p *provider) {
	level.Debug(m.logger).Log("msg", "Starting provider", "provider", p.name, "subs", fmt.Sprintf("%v", p.subs))
	ctx, cancel := context.WithCancel(ctx)
	// updates是发送[]*targetgroup.Group的管道
	updates := make(chan []*targetgroup.Group)

	// 收集discoverCancel
	m.discoverCancel = append(m.discoverCancel, cancel)

	// 运行discoverer
	go p.d.Run(ctx, updates)
	go m.updater(ctx, p, updates)
}

func (m *Manager) updater(ctx context.Context, p *provider, updates chan []*targetgroup.Group) {
	for {
		select {
		case <-ctx.Done():
			return
		case tgs, ok := <-updates:
			receivedUpdates.WithLabelValues(m.name).Inc()
			if !ok {
				// channel关闭了，则返回
				level.Debug(m.logger).Log("msg", "discoverer channel closed", "provider", p.name)
				return
			}

			for _, s := range p.subs {
				// setName和对应的provider Name构成一个group
				// 都是全量推送的
				m.updateGroup(poolKey{setName: s, provider: p.name}, tgs)
			}

			select {
			// 触发发送
			case m.triggerSend <- struct{}{}:
			default:
			}
		}
	}
}

func (m *Manager) sender() {
	ticker := time.NewTicker(m.updatert)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		// 有的discoverers发送更新太频繁，因此我们用ticker来限流
		case <-ticker.C: // Some discoverers send updates too often so we throttle these with the ticker.
			select {
			case <-m.triggerSend:
				sentUpdates.WithLabelValues(m.name).Inc()
				select {
				case m.syncCh <- m.allGroups():
				default:
					delayedUpdates.WithLabelValues(m.name).Inc()
					// discovery receiver的channel已经满了，因此会在下一个周期触发
					level.Debug(m.logger).Log("msg", "discovery receiver's channel was full so will retry the next cycle")
					select {
					// 如果m.syncCh当前满了，仍然要给m.triggerSend填满，从而在下一个循环能够马上再次向m.syncCh发送最新的groups
					case m.triggerSend <- struct{}{}:
					default:
					}
				}
			default:
			}
		}
	}
}

func (m *Manager) cancelDiscoverers() {
	for _, c := range m.discoverCancel {
		c()
	}
	// 重置m.targets，m.providers以及m.discoverCancel
	m.targets = make(map[poolKey]map[string]*targetgroup.Group)
	m.providers = nil
	m.discoverCancel = nil
}

func (m *Manager) updateGroup(poolKey poolKey, tgs []*targetgroup.Group) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	for _, tg := range tgs {
		if tg != nil { // Some Discoverers send nil target group so need to check for it to avoid panics.
			// 有些Discoverer发送nil target group，因此需要检查，防止panic
			if _, ok := m.targets[poolKey]; !ok {
				m.targets[poolKey] = make(map[string]*targetgroup.Group)
			}
			// 对于pod来说，一个target group代表了一个pod，貌似并没有删除pod的过程
			m.targets[poolKey][tg.Source] = tg
		}
	}
}

func (m *Manager) allGroups() map[string][]*targetgroup.Group {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	tSets := map[string][]*targetgroup.Group{}
	for pkey, tsets := range m.targets {
		var n int
		// 遍历map[string]*targetgroup.Group
		for _, tg := range tsets {
			// Even if the target group 'tg' is empty we still need to send it to the 'Scrape manager'
			// to signal that it needs to stop all scrape loops for this target set.
			// 即使target group 'tg'为空，我们也要将它发送给'Scrape manager'，用于通知它停止这个target set的所有scrape loops
			// 感觉会有重复的target?
			tSets[pkey.setName] = append(tSets[pkey.setName], tg)
			n += len(tg.Targets)
		}
		discoveredTargets.WithLabelValues(m.name, pkey.setName).Set(float64(n))
	}
	return tSets
}

func (m *Manager) registerProviders(cfg sd_config.ServiceDiscoveryConfig, setName string) {
	var added bool
	add := func(cfg interface{}, newDiscoverer func() (Discoverer, error)) {
		// t是config的类型
		t := reflect.TypeOf(cfg).String()
		for _, p := range m.providers {
			// 如果cf和已经存在的provider的配置一样
			if reflect.DeepEqual(cfg, p.config) {
				p.subs = append(p.subs, setName)
				added = true
				return
			}
		}

		// 同样的配置用同一个discoverer进行抓取
		// 创建新的discoverer
		d, err := newDiscoverer()
		if err != nil {
			level.Error(m.logger).Log("msg", "Cannot create service discovery", "err", err, "type", t)
			failedConfigs.WithLabelValues(m.name).Inc()
			return
		}

		provider := provider{
			name:   fmt.Sprintf("%s/%d", t, len(m.providers)),
			d:      d,
			config: cfg,
			subs:   []string{setName},
		}
		m.providers = append(m.providers, &provider)
		added = true
	}

	// 遍历各种SD config，增加provider
	for _, c := range cfg.DNSSDConfigs {
		add(c, func() (Discoverer, error) {
			return dns.NewDiscovery(*c, log.With(m.logger, "discovery", "dns")), nil
		})
	}
	for _, c := range cfg.FileSDConfigs {
		add(c, func() (Discoverer, error) {
			return file.NewDiscovery(c, log.With(m.logger, "discovery", "file")), nil
		})
	}
	for _, c := range cfg.ConsulSDConfigs {
		add(c, func() (Discoverer, error) {
			return consul.NewDiscovery(c, log.With(m.logger, "discovery", "consul"))
		})
	}
	for _, c := range cfg.MarathonSDConfigs {
		add(c, func() (Discoverer, error) {
			return marathon.NewDiscovery(*c, log.With(m.logger, "discovery", "marathon"))
		})
	}
	for _, c := range cfg.KubernetesSDConfigs {
		add(c, func() (Discoverer, error) {
			return kubernetes.New(log.With(m.logger, "discovery", "k8s"), c)
		})
	}
	for _, c := range cfg.ServersetSDConfigs {
		add(c, func() (Discoverer, error) {
			return zookeeper.NewServersetDiscovery(c, log.With(m.logger, "discovery", "zookeeper"))
		})
	}
	for _, c := range cfg.NerveSDConfigs {
		add(c, func() (Discoverer, error) {
			return zookeeper.NewNerveDiscovery(c, log.With(m.logger, "discovery", "nerve"))
		})
	}
	for _, c := range cfg.EC2SDConfigs {
		add(c, func() (Discoverer, error) {
			return ec2.NewDiscovery(c, log.With(m.logger, "discovery", "ec2")), nil
		})
	}
	for _, c := range cfg.OpenstackSDConfigs {
		add(c, func() (Discoverer, error) {
			return openstack.NewDiscovery(c, log.With(m.logger, "discovery", "openstack"))
		})
	}
	for _, c := range cfg.GCESDConfigs {
		add(c, func() (Discoverer, error) {
			return gce.NewDiscovery(*c, log.With(m.logger, "discovery", "gce"))
		})
	}
	for _, c := range cfg.AzureSDConfigs {
		add(c, func() (Discoverer, error) {
			return azure.NewDiscovery(c, log.With(m.logger, "discovery", "azure")), nil
		})
	}
	for _, c := range cfg.TritonSDConfigs {
		add(c, func() (Discoverer, error) {
			return triton.New(log.With(m.logger, "discovery", "triton"), c)
		})
	}
	if len(cfg.StaticConfigs) > 0 {
		add(setName, func() (Discoverer, error) {
			// 直接创建Static Provider并添加
			return &StaticProvider{TargetGroups: cfg.StaticConfigs}, nil
		})
	}
	if !added {
		// Add an empty target group to force the refresh of the corresponding
		// scrape pool and to notify the receiver that this target set has no
		// current targets.
		// 增加一个空的target group用于强行刷新对应的scrape pool并且告诉receiver，这个target set
		// 当前没有targets
		// It can happen because the combined set of SD configurations is empty
		// or because we fail to instantiate all the SD configurations.
		// 这可能在SD configurations都为空或者我们初始化所有的SD configurations失败的情况
		add(setName, func() (Discoverer, error) {
			return &StaticProvider{TargetGroups: []*targetgroup.Group{{}}}, nil
		})
	}
}

// StaticProvider holds a list of target groups that never change.
// StaticProvider维护一系列不会改变的target groups
type StaticProvider struct {
	TargetGroups []*targetgroup.Group
}

// Run implements the Worker interface.
// Run实现了Discoverer接口
func (sd *StaticProvider) Run(ctx context.Context, ch chan<- []*targetgroup.Group) {
	// We still have to consider that the consumer exits right away in which case
	// the context will be canceled.
	select {
	case ch <- sd.TargetGroups:
	case <-ctx.Done():
	}
	close(ch)
}
