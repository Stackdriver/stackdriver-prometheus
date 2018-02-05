/*
Copyright 2018 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package retrieval

import (
	"errors"
	"fmt"

	"github.com/gogo/protobuf/proto"
	dto "github.com/prometheus/client_model/go"
)

// The errors exposed.
var (
	ErrNotFound                    = errors.New("not found")
	ErrOutOfOrderSample            = errors.New("out of order sample")
	ErrDuplicateSampleForTimestamp = errors.New("duplicate sample for timestamp")
	ErrOutOfBounds                 = errors.New("out of bounds")
)

const (
	NoTimestamp = 0
)

type MetricFamily struct {
	*dto.MetricFamily
	// MetricResetTimestampMs must have one element for each
	// MetricFamily.Metric. Elements must be initialized to NoTimestamp if
	// the value is unknown.
	MetricResetTimestampMs []int64
}

// Clone returns a new object that is a deep copy of the target object.
func (f *MetricFamily) Clone() (o *MetricFamily) {
	o = &MetricFamily{
		MetricFamily:           proto.Clone(f.MetricFamily).(*dto.MetricFamily),
		MetricResetTimestampMs: make([]int64, len(f.MetricResetTimestampMs)),
	}
	o.MetricResetTimestampMs = f.MetricResetTimestampMs
	return
}

func (f *MetricFamily) String() string {
	return fmt.Sprintf("MetricFamily<dto.MetricFamily: %v MetricResetTimestampMs: %v>",
		f.MetricFamily, f.MetricResetTimestampMs)
}

type Appender interface {
	Add(metricFamily *MetricFamily) error
}
