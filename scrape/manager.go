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
	"encoding"
	"fmt"
	"hash/fnv"
	"net"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"

	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

// Appendable returns an Appender.
type Appendable interface {
	Appender() (storage.Appender, error)
}

// NewManager is the Manager constructor
func NewManager(logger log.Logger, app Appendable) *Manager {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	return &Manager{
		append:        app,
		logger:        logger,
		scrapeConfigs: make(map[string]*config.ScrapeConfig),
		scrapePools:   make(map[string]*scrapePool),
		graceShut:     make(chan struct{}),
		triggerReload: make(chan struct{}, 1),
	}
}

// Manager maintains a set of scrape pools and manages start/stop cycles
// when receiving new target groups form the discovery manager.
// Manager维护了一系列的scrape pools并且管理从discovery manager收到新的target groups
// 时的start/stop cycles
type Manager struct {
	logger    log.Logger
	append    Appendable
	graceShut chan struct{}

	// 全局的jitterSeed seed用于在设置了HA的时候分布scrape workload
	jitterSeed    uint64     // Global jitterSeed seed is used to spread scrape workload across HA setup.
	mtxScrape     sync.Mutex // Guards the fields below.
	// scrapeConfigs用job作为key，其实是配置文件中写明的配置
	scrapeConfigs map[string]*config.ScrapeConfig
	scrapePools   map[string]*scrapePool
	// 全量的target集合
	targetSets    map[string][]*targetgroup.Group

	triggerReload chan struct{}
}

// Run receives and saves target set updates and triggers the scraping loops reloading.
// Reloading happens in the background so that it doesn't block receiving targets updates.
// Run接收并且保存target set的更新并且触发scraping loops的重新加载
// Reloading在后台发生因此并不会阻塞targets updates
func (m *Manager) Run(tsets <-chan map[string][]*targetgroup.Group) error {
	// 生成一个goroutine进行reloader()
	go m.reloader()
	for {
		select {
		case ts := <-tsets:
			// 用从channel中获取的target sets更新
			m.updateTsets(ts)

			select {
			// target set更新，触发reload
			case m.triggerReload <- struct{}{}:
			default:
			}

		case <-m.graceShut:
			return nil
		}
	}
}

func (m *Manager) reloader() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.graceShut:
			return
		case <-ticker.C:
			select {
			// 至少五秒，触发一次reload
			case <-m.triggerReload:
				m.reload()
			case <-m.graceShut:
				return
			}
		}
	}
}

func (m *Manager) reload() {
	m.mtxScrape.Lock()
	var wg sync.WaitGroup
	// 每个job对应一个scrape pool
	for setName, groups := range m.targetSets {
		// 如果targetSets在scrapePools中不存在对应的scrapePool，则创建之
		if _, ok := m.scrapePools[setName]; !ok {
			scrapeConfig, ok := m.scrapeConfigs[setName]
			if !ok {
				// 如果一个target set在scrape Pools和scrape Configs中都不存在，则直接跳过
				level.Error(m.logger).Log("msg", "error reloading target set", "err", "invalid config id:"+setName)
				continue
			}
			// 新建ScrapePool
			sp, err := newScrapePool(scrapeConfig, m.append, m.jitterSeed, log.With(m.logger, "scrape_pool", setName))
			if err != nil {
				level.Error(m.logger).Log("msg", "error creating new scrape pool", "err", err, "scrape_pool", setName)
				continue
			}
			m.scrapePools[setName] = sp
		}

		wg.Add(1)
		// Run the sync in parallel as these take a while and at high load can't catch up.
		// 同步地运行sync
		go func(sp *scrapePool, groups []*targetgroup.Group) {
			sp.Sync(groups)
			wg.Done()
		}(m.scrapePools[setName], groups)

	}
	m.mtxScrape.Unlock()
	wg.Wait()
}

// setJitterSeed calculates a global jitterSeed per server relying on extra label set.
// setJitterSeed依据extra label set为每个server计算global jitterSeed
func (m *Manager) setJitterSeed(labels labels.Labels) error {
	h := fnv.New64a()
	hostname, err := getFqdn()
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(h, "%s%s", hostname, labels.String()); err != nil {
		return err
	}
	m.jitterSeed = h.Sum64()
	return nil
}

// Stop cancels all running scrape pools and blocks until all have exited.
func (m *Manager) Stop() {
	m.mtxScrape.Lock()
	defer m.mtxScrape.Unlock()

	for _, sp := range m.scrapePools {
		sp.stop()
	}
	close(m.graceShut)
}

func (m *Manager) updateTsets(tsets map[string][]*targetgroup.Group) {
	m.mtxScrape.Lock()
	m.targetSets = tsets
	m.mtxScrape.Unlock()
}

// ApplyConfig resets the manager's target providers and job configurations as defined by the new cfg.
// ApplyConfig根据新的cfg重置了manager的target providers和job configurations
func (m *Manager) ApplyConfig(cfg *config.Config) error {
	m.mtxScrape.Lock()
	defer m.mtxScrape.Unlock()

	c := make(map[string]*config.ScrapeConfig)
	for _, scfg := range cfg.ScrapeConfigs {
		// 用job name作为key
		c[scfg.JobName] = scfg
	}
	m.scrapeConfigs = c

	if err := m.setJitterSeed(cfg.GlobalConfig.ExternalLabels); err != nil {
		return err
	}

	// Cleanup and reload pool if the configuration has changed.
	// 清除或者重载pool，如果configuration发生改变的话
	var failed bool
	// 遍历pools，如果pool不存在则停止并删除,如果配置发生改变的话，则重载
	for name, sp := range m.scrapePools {
		if cfg, ok := m.scrapeConfigs[name]; !ok {
			// 如果scrape pool不在scrape config中就删除
			sp.stop()
			delete(m.scrapePools, name)
		} else if !reflect.DeepEqual(sp.config, cfg) {
			// 如果job依然存在，但是配置发生了改变，则调用sp.reload重新加载
			err := sp.reload(cfg)
			if err != nil {
				level.Error(m.logger).Log("msg", "error reloading scrape pool", "err", err, "scrape_pool", name)
				failed = true
			}
		}
	}

	// 不创建新的scrap pool?

	if failed {
		return errors.New("failed to apply the new configuration")
	}
	return nil
}

// TargetsAll returns active and dropped targets grouped by job_name.
func (m *Manager) TargetsAll() map[string][]*Target {
	m.mtxScrape.Lock()
	defer m.mtxScrape.Unlock()

	targets := make(map[string][]*Target, len(m.scrapePools))
	for tset, sp := range m.scrapePools {
		targets[tset] = append(sp.ActiveTargets(), sp.DroppedTargets()...)

	}
	return targets
}

// TargetsActive returns the active targets currently being scraped.
// TargetsActive返回当前正在被抓取的active targets
func (m *Manager) TargetsActive() map[string][]*Target {
	m.mtxScrape.Lock()
	defer m.mtxScrape.Unlock()

	targets := make(map[string][]*Target, len(m.scrapePools))
	for tset, sp := range m.scrapePools {
		targets[tset] = sp.ActiveTargets()
	}
	return targets
}

// TargetsDropped returns the dropped targets during relabelling.
func (m *Manager) TargetsDropped() map[string][]*Target {
	m.mtxScrape.Lock()
	defer m.mtxScrape.Unlock()

	targets := make(map[string][]*Target, len(m.scrapePools))
	for tset, sp := range m.scrapePools {
		targets[tset] = sp.DroppedTargets()
	}
	return targets
}

// getFqdn returns a FQDN if it's possible, otherwise falls back to hostname.
func getFqdn() (string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return "", err
	}

	ips, err := net.LookupIP(hostname)
	if err != nil {
		// Return the system hostname if we can't look up the IP address.
		return hostname, nil
	}

	lookup := func(ipStr encoding.TextMarshaler) (string, error) {
		ip, err := ipStr.MarshalText()
		if err != nil {
			return "", err
		}
		hosts, err := net.LookupAddr(string(ip))
		if err != nil || len(hosts) == 0 {
			return "", err
		}
		return hosts[0], nil
	}

	for _, addr := range ips {
		if ip := addr.To4(); ip != nil {
			if fqdn, err := lookup(ip); err == nil {
				return fqdn, nil
			}

		}

		if ip := addr.To16(); ip != nil {
			if fqdn, err := lookup(ip); err == nil {
				return fqdn, nil
			}

		}
	}
	return hostname, nil
}
