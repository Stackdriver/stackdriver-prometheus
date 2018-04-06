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
	"sort"

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
	//	TargetLabels           labels.Labels
}

func (f *MetricFamily) String() string {
	return fmt.Sprintf("MetricFamily<dto.MetricFamily: %v MetricResetTimestampMs: %v>",
		f.MetricFamily, f.MetricResetTimestampMs)
}

// SeparatorByte is a byte that cannot occur in valid UTF-8 sequences and is
// used to separate label names, label values, and other strings from each other
// when calculating their combined hash value (aka signature aka fingerprint).
const SeparatorByte byte = 255

// ByName implements sort.Interface for []*MetricFamily based on the Name field.
type LabelPairsByName []*dto.LabelPair

func (f LabelPairsByName) Len() int           { return len(f) }
func (f LabelPairsByName) Swap(i, j int)      { f[i], f[j] = f[j], f[i] }
func (f LabelPairsByName) Less(i, j int) bool { return f[i].GetName() < f[j].GetName() }

func (f *MetricFamily) Fingerprint() uint64 {
	h := hashNew()
	h = hashAdd(h, f.GetName())
	for _, metric := range f.GetMetric() {
		sort.Sort(LabelPairsByName(metric.GetLabel()))
		for _, label := range metric.GetLabel() {
			h = hashAddByte(h, SeparatorByte)
			h = hashAdd(h, label.GetName())
			h = hashAddByte(h, SeparatorByte)
			h = hashAdd(h, label.GetValue())
		}
	}
	return h
}

type Appender interface {
	Add(metricFamily *MetricFamily) error
}
