// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gate

import "context"

// A Gate controls the maximum number of concurrently running and waiting queries.
// Gate用于控制最大并发数目的running和waiting的queries
type Gate struct {
	ch chan struct{}
}

// NewGate returns a query gate that limits the number of queries
// being concurrently executed.
// NewGate返回一个query gate，它用于限制在并发执行的queries的数目
func New(length int) *Gate {
	return &Gate{
		ch: make(chan struct{}, length),
	}
}

// Start blocks until the gate has a free spot or the context is done.
// Start一直阻塞直到gate中有空闲的spot或者context已经结束
func (g *Gate) Start(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case g.ch <- struct{}{}:
		return nil
	}
}

// Done releases a single spot in the gate.
// Done释放gate中的一个spot
func (g *Gate) Done() {
	select {
	case <-g.ch:
	default:
		panic("gate.Done: more operations done than started")
	}
}
