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

package kubernetes

import (
	"context"
	"net"
	"strconv"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/util/strutil"
)

// Pod discovers new pod targets.
type Pod struct {
	informer cache.SharedInformer
	store    cache.Store
	logger   log.Logger
	queue    *workqueue.Type
}

// NewPod creates a new pod discovery.
// NewPod创建一个新的pod discovery
func NewPod(l log.Logger, pods cache.SharedInformer) *Pod {
	if l == nil {
		l = log.NewNopLogger()
	}
	p := &Pod{
		informer: pods,
		store:    pods.GetStore(),
		logger:   l,
		queue:    workqueue.NewNamed("pod"),
	}
	p.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(o interface{}) {
			eventCount.WithLabelValues("pod", "add").Inc()
			p.enqueue(o)
		},
		DeleteFunc: func(o interface{}) {
			eventCount.WithLabelValues("pod", "delete").Inc()
			p.enqueue(o)
		},
		UpdateFunc: func(_, o interface{}) {
			eventCount.WithLabelValues("pod", "update").Inc()
			p.enqueue(o)
		},
	})
	return p
}

func (p *Pod) enqueue(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		return
	}

	p.queue.Add(key)
}

// Run implements the Discoverer interface.
// Run实现了Discoverer接口
func (p *Pod) Run(ctx context.Context, ch chan<- []*targetgroup.Group) {
	defer p.queue.ShutDown()

	if !cache.WaitForCacheSync(ctx.Done(), p.informer.HasSynced) {
		level.Error(p.logger).Log("msg", "pod informer unable to sync cache")
		return
	}

	go func() {
		// 对队列进行处理
		for p.process(ctx, ch) {
		}
	}()

	// Block until the target provider is explicitly canceled.
	// 阻塞直到target provider被显式地canceled
	<-ctx.Done()
}

func (p *Pod) process(ctx context.Context, ch chan<- []*targetgroup.Group) bool {
	keyObj, quit := p.queue.Get()
	if quit {
		return false
	}
	defer p.queue.Done(keyObj)
	key := keyObj.(string)

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return true
	}

	o, exists, err := p.store.GetByKey(key)
	if err != nil {
		return true
	}
	if !exists {
		// 如果pod已经不存在了，则将相应的targetgroup.Group设置为空，即仅仅只有Source字段
		send(ctx, p.logger, RolePod, ch, &targetgroup.Group{Source: podSourceFromNamespaceAndName(namespace, name)})
		return true
	}
	// 从队列中拿出keyObj，再从缓存中取出对象
	eps, err := convertToPod(o)
	if err != nil {
		level.Error(p.logger).Log("msg", "converting to Pod object failed", "err", err)
		return true
	}
	// p.buildPod根据eps创建一个target.Group
	send(ctx, p.logger, RolePod, ch, p.buildPod(eps))
	return true
}

func convertToPod(o interface{}) (*apiv1.Pod, error) {
	pod, ok := o.(*apiv1.Pod)
	if ok {
		return pod, nil
	}

	return nil, errors.Errorf("received unexpected object: %v", o)
}

const (
	// metaLabelPrefix为"__meta_kubernetes_"
	podNameLabel                  = metaLabelPrefix + "pod_name"
	podIPLabel                    = metaLabelPrefix + "pod_ip"
	// "__meta_kubernetes_pod_container_name"
	podContainerNameLabel         = metaLabelPrefix + "pod_container_name"
	podContainerPortNameLabel     = metaLabelPrefix + "pod_container_port_name"
	podContainerPortNumberLabel   = metaLabelPrefix + "pod_container_port_number"
	podContainerPortProtocolLabel = metaLabelPrefix + "pod_container_port_protocol"
	podReadyLabel                 = metaLabelPrefix + "pod_ready"
	podPhaseLabel                 = metaLabelPrefix + "pod_phase"
	podLabelPrefix                = metaLabelPrefix + "pod_label_"
	podLabelPresentPrefix         = metaLabelPrefix + "pod_labelpresent_"
	podAnnotationPrefix           = metaLabelPrefix + "pod_annotation_"
	podAnnotationPresentPrefix    = metaLabelPrefix + "pod_annotationpresent_"
	podNodeNameLabel              = metaLabelPrefix + "pod_node_name"
	podHostIPLabel                = metaLabelPrefix + "pod_host_ip"
	podUID                        = metaLabelPrefix + "pod_uid"
	podControllerKind             = metaLabelPrefix + "pod_controller_kind"
	podControllerName             = metaLabelPrefix + "pod_controller_name"
)

// GetControllerOf returns a pointer to a copy of the controllerRef if controllee has a controller
// https://github.com/kubernetes/apimachinery/blob/cd2cae2b39fa57e8063fa1f5f13cfe9862db3d41/pkg/apis/meta/v1/controller_ref.go
func GetControllerOf(controllee metav1.Object) *metav1.OwnerReference {
	for _, ref := range controllee.GetOwnerReferences() {
		if ref.Controller != nil && *ref.Controller {
			return &ref
		}
	}
	return nil
}

func podLabels(pod *apiv1.Pod) model.LabelSet {
	ls := model.LabelSet{
		// 增加pod的name, ip, ready, phase, node name, host ip以及pod uid
		podNameLabel:     lv(pod.ObjectMeta.Name),
		podIPLabel:       lv(pod.Status.PodIP),
		podReadyLabel:    podReady(pod),
		podPhaseLabel:    lv(string(pod.Status.Phase)),
		podNodeNameLabel: lv(pod.Spec.NodeName),
		podHostIPLabel:   lv(pod.Status.HostIP),
		podUID:           lv(string(pod.ObjectMeta.UID)),
	}

	createdBy := GetControllerOf(pod)
	if createdBy != nil {
		if createdBy.Kind != "" {
			ls[podControllerKind] = lv(createdBy.Kind)
		}
		if createdBy.Name != "" {
			ls[podControllerName] = lv(createdBy.Name)
		}
	}

	// 在label中添加pod的labels和annotations
	for k, v := range pod.Labels {
		ln := strutil.SanitizeLabelName(k)
		// podLabelPrefix为"__meta_kubernetes_pod_label_"
		ls[model.LabelName(podLabelPrefix+ln)] = lv(v)
		ls[model.LabelName(podLabelPresentPrefix+ln)] = presentValue
	}

	for k, v := range pod.Annotations {
		ln := strutil.SanitizeLabelName(k)
		// podAnnotationPrefix为"__meta_kubernetes_pod_annotation_"
		ls[model.LabelName(podAnnotationPrefix+ln)] = lv(v)
		ls[model.LabelName(podAnnotationPresentPrefix+ln)] = presentValue
	}

	return ls
}

func (p *Pod) buildPod(pod *apiv1.Pod) *targetgroup.Group {
	// 根据pod创建target group
	tg := &targetgroup.Group{
		Source: podSource(pod),
	}
	// PodIP can be empty when a pod is starting or has been evicted.
	// 当pod正在启动或者已经被驱逐的时候，PodIP可能为空
	if len(pod.Status.PodIP) == 0 {
		return tg
	}

	// 获取pod的一系列labels
	tg.Labels = podLabels(pod)
	// 设置"__meta_kubernetes_namespace"为pod的namespace
	tg.Labels[namespaceLabel] = lv(pod.Namespace)

	for _, c := range pod.Spec.Containers {
		// If no ports are defined for the container, create an anonymous
		// target per container.
		// 如果容器没有定义ports，为每个容器创建匿名的target
		if len(c.Ports) == 0 {
			// We don't have a port so we just set the address label to the pod IP.
			// The user has to add a port manually.
			// 我们没有port，因此将address label设置为pod IP，用户需要手动添加port
			tg.Targets = append(tg.Targets, model.LabelSet{
				model.AddressLabel:    lv(pod.Status.PodIP),
				// "__meta_kubernetes_pod_container_name"
				podContainerNameLabel: lv(c.Name),
			})
			continue
		}
		// Otherwise create one target for each container/port combination.
		// 否则，为每个container/port的组合创建一个target
		for _, port := range c.Ports {
			ports := strconv.FormatUint(uint64(port.ContainerPort), 10)
			addr := net.JoinHostPort(pod.Status.PodIP, ports)

			tg.Targets = append(tg.Targets, model.LabelSet{
				model.AddressLabel:            lv(addr),
				podContainerNameLabel:         lv(c.Name),
				// "__meta_kubernetes_pod_container_port_number"
				podContainerPortNumberLabel:   lv(ports),
				// "__meta_kubernetes_pod_container_port_name"
				podContainerPortNameLabel:     lv(port.Name),
				podContainerPortProtocolLabel: lv(string(port.Protocol)),
			})
		}
	}

	return tg
}

func podSource(pod *apiv1.Pod) string {
	return podSourceFromNamespaceAndName(pod.Namespace, pod.Name)
}

func podSourceFromNamespaceAndName(namespace, name string) string {
	return "pod/" + namespace + "/" + name
}

func podReady(pod *apiv1.Pod) model.LabelValue {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == apiv1.PodReady {
			return lv(strings.ToLower(string(cond.Status)))
		}
	}
	return lv(strings.ToLower(string(apiv1.ConditionUnknown)))
}
