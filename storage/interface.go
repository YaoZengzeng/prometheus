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
// Storage摄取并且管理samples，有着各种各样的indexes
// 所有的方法都是线程安全的，Storage实现了storage.SampleAppender
type Storage interface {
	// Queryable返回一个Querier
	Queryable

	// StartTime returns the oldest timestamp stored in the storage.
	// StartTime返回在storage中存储的最老的timestamp
	StartTime() (int64, error)

	// Appender returns a new appender against the storage.
	// Appender返回storage的一个新的appender
	Appender() (Appender, error)

	// Close closes the storage and all its underlying resources.
	// Close关闭存储以及所有底层的资源
	Close() error
}

// A Queryable handles queries against a storage.
// Queryable处理对于一个storage的queries
type Queryable interface {
	// Querier returns a new Querier on the storage.
	// Querier返回storage的一个新的Querier
	Querier(ctx context.Context, mint, maxt int64) (Querier, error)
}

// Querier provides reading access to time series data.
// Querier提供了对于时序数据的读权限
type Querier interface {
	// Select returns a set of series that matches the given label matchers.
	// Select返回和给定的label matchers匹配的一系列series
	Select(*SelectParams, ...*labels.Matcher) (SeriesSet, Warnings, error)

	// LabelValues returns all potential values for a label name.
	// LabelValues返回一个label name所有可能的values
	LabelValues(name string) ([]string, error)

	// LabelNames returns all the unique label names present in the block in sorted order.
	// LabelNames按序返回当前block里面所有唯一的label names
	LabelNames() ([]string, error)

	// Close releases the resources of the Querier.
	// Close释放Querier相关的资源
	Close() error
}

// SelectParams specifies parameters passed to data selections.
// SelectParams指定进行数据筛选的参数
type SelectParams struct {
	// 数据筛选的起始和截止时间，单位是毫秒
	Start int64 // Start time in milliseconds for this select.
	End   int64 // End time in milliseconds for this select.

	// 以毫秒计的Query step size
	Step int64  // Query step size in milliseconds.
	// surrounding function或者aggregation的字符串表示
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
// Appender提供了对于一个storage的batched appends
type Appender interface {
	Add(l labels.Labels, t int64, v float64) (uint64, error)

	AddFast(l labels.Labels, ref uint64, t int64, v float64) error

	// Commit submits the collected samples and purges the batch.
	// Commit提交收集到的samples并且清除batch
	Commit() error

	Rollback() error
}

// SeriesSet contains a set of series.
// SeriesSet包含了一系列的series
type SeriesSet interface {
	Next() bool
	At() Series
	Err() error
}

// Series represents a single time series.
// Series代表单个的time series
type Series interface {
	// Labels returns the complete set of labels identifying the series.
	// Labels返回标识这个series的完整的labels
	Labels() labels.Labels

	// Iterator returns a new iterator of the data of the series.
	// Iterator返回series相关数据的新的iterator
	Iterator() SeriesIterator
}

// SeriesIterator iterates over the data of a time series.
// SeriesIterator遍历一个time series的数据
type SeriesIterator interface {
	// Seek advances the iterator forward to the value at or after
	// the given timestamp.
	// Seek将iterator移动到给定的timestamp所在或者之后的值
	Seek(t int64) bool
	// At returns the current timestamp/value pair.
	// Af返回当前的timestamp或者value对
	At() (t int64, v float64)
	// Next advances the iterator by one.
	// Next将iterator加一
	Next() bool
	// Err returns the current error.
	// Err返回当前的error
	Err() error
}

type Warnings []error
