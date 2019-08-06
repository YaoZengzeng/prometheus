// Copyright 2014 The Prometheus Authors
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

package storage

import (
	"context"
	"errors"

	"github.com/prometheus/prometheus/pkg/labels"
)

// The errors exposed.
var (
	ErrNotFound                    = errors.New("not found")
	ErrOutOfOrderSample            = errors.New("out of order sample")
	ErrDuplicateSampleForTimestamp = errors.New("duplicate sample for timestamp")
	ErrOutOfBounds                 = errors.New("out of bounds")
)

// Storage ingests and manages samples, along with various indexes. All methods
// are goroutine-safe. Storage implements storage.SampleAppender.
// Storage吸收并且管理samples，以及各种的indexes
type Storage interface {
	Queryable

	// StartTime returns the oldest timestamp stored in the storage.
	// StartTime返回storage中存储的最老的时间戳
	StartTime() (int64, error)

	// Appender returns a new appender against the storage.
	// Appender返回对于存储的一个新的appender
	Appender() (Appender, error)

	// Close closes the storage and all its underlying resources.
	// Close关闭storage以及所有的底层资源
	Close() error
}

// A Queryable handles queries against a storage.
// Queryable处理对于一个storage的queries
type Queryable interface {
	// Querier returns a new Querier on the storage.
	// Querier返回对于storage的一个新的Querier
	Querier(ctx context.Context, mint, maxt int64) (Querier, error)
}

// Querier provides reading access to time series data.
// Querier提供了对于时序数据的读访问
type Querier interface {
	// Select returns a set of series that matches the given label matchers.
	// Select返回一系列匹配给定的label matchers的series
	Select(*SelectParams, ...*labels.Matcher) (SeriesSet, Warnings, error)

	// LabelValues returns all potential values for a label name.
	LabelValues(name string) ([]string, Warnings, error)

	// LabelNames returns all the unique label names present in the block in sorted order.
	LabelNames() ([]string, Warnings, error)

	// Close releases the resources of the Querier.
	// Close释放一个Querier的资源
	Close() error
}

// SelectParams specifies parameters passed to data selections.
// SelectParams指定了用于数据选择的参数
type SelectParams struct {
	Start int64 // Start time in milliseconds for this select.
	End   int64 // End time in milliseconds for this select.

	// 以毫秒计数的查询步长
	Step int64  // Query step size in milliseconds.
	Func string // String representation of surrounding function or aggregation.
}

// QueryableFunc is an adapter to allow the use of ordinary functions as
// Queryables. It follows the idea of http.HandlerFunc.
type QueryableFunc func(ctx context.Context, mint, maxt int64) (Querier, error)

// Querier calls f() with the given parameters.
func (f QueryableFunc) Querier(ctx context.Context, mint, maxt int64) (Querier, error) {
	return f(ctx, mint, maxt)
}

// Appender provides batched appends against a storage.
// Appender提供对于一个storage的批量appends方式
type Appender interface {
	Add(l labels.Labels, t int64, v float64) (uint64, error)

	AddFast(l labels.Labels, ref uint64, t int64, v float64) error

	// Commit submits the collected samples and purges the batch.
	// Commit提交收集到的samples并且移除batch
	Commit() error

	Rollback() error
}

// SeriesSet contains a set of series.
// SeriesSet包含一系列的series
type SeriesSet interface {
	Next() bool
	At() Series
	Err() error
}

// Series represents a single time series.
// Series代表单个的时间序列
type Series interface {
	// Labels returns the complete set of labels identifying the series.
	// Labels返回用于标识series的完整的labels集合
	Labels() labels.Labels

	// Iterator returns a new iterator of the data of the series.
	// Iterator返回一个时序数据的新的iterator
	Iterator() SeriesIterator
}

// SeriesIterator iterates over the data of a time series.
// SeriesIterator遍历一个时序的数据
type SeriesIterator interface {
	// Seek advances the iterator forward to the value at or after
	// the given timestamp.
	// Seek将iterator移动到给定时间戳或者给定时间戳以后
	Seek(t int64) bool
	// At returns the current timestamp/value pair.
	At() (t int64, v float64)
	// Next advances the iterator by one.
	Next() bool
	// Err returns the current error.
	Err() error
}

type Warnings []error
