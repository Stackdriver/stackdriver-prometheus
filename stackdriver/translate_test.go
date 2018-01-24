/*
Copyright 2017 Google Inc.

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

package stackdriver

import (
	"bytes"
	"math"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/go-kit/kit/log"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
)

var metricTypeGauge = dto.MetricType_GAUGE
var metricTypeCounter = dto.MetricType_COUNTER
var metricTypeHistogram = dto.MetricType_HISTOGRAM

var testMetricName = "test_name"
var gaugeMetricName = "gauge_metric"
var floatMetricName = "float_metric"
var testMetricHistogram = "test_histogram"
var testMetricDescription = "Description 1"
var testMetricHistogramDescription = "Description 2"

var testResourceMappings = []ResourceMap{
	{
		// The empty label map matches every metric possible.
		// Right now this must be gke_container. See TODO in translateFamily().
		Type:     "gke_container",
		LabelMap: map[string]string{},
	},
}

var metrics = []*dto.MetricFamily{
	{
		Name: &testMetricName,
		Type: &metricTypeCounter,
		Help: &testMetricDescription,
		Metric: []*dto.Metric{
			{
				Label: []*dto.LabelPair{
					{
						Name:  stringPtr("labelName"),
						Value: stringPtr("labelValue1"),
					},
				},
				Counter: &dto.Counter{Value: floatPtr(42.0)},
			},
			{
				Label: []*dto.LabelPair{
					{
						Name:  stringPtr("labelName"),
						Value: stringPtr("labelValue2"),
					},
				},
				Counter: &dto.Counter{Value: floatPtr(106.0)},
			},
		},
	},
	{
		Name: stringPtr(gaugeMetricName),
		Type: &metricTypeGauge,
		Metric: []*dto.Metric{
			{
				Label: []*dto.LabelPair{
					{
						Name:  stringPtr("labelName"),
						Value: stringPtr("falseValue"),
					},
				},
				Gauge: &dto.Gauge{Value: floatPtr(0.00001)},
			},
			{
				Label: []*dto.LabelPair{
					{
						Name:  stringPtr("labelName"),
						Value: stringPtr("trueValue"),
					},
				},
				Gauge: &dto.Gauge{Value: floatPtr(1.2)},
			},
		},
	},
	{
		Name: stringPtr(floatMetricName),
		Type: &metricTypeCounter,
		Metric: []*dto.Metric{
			{
				Counter: &dto.Counter{Value: floatPtr(123.17)},
			},
		},
	},
	{
		Name: stringPtr(processStartTimeMetric),
		Type: &metricTypeGauge,
		Metric: []*dto.Metric{
			{
				Gauge: &dto.Gauge{Value: floatPtr(1234567890.0)},
			},
		},
	},
	{
		Name: &testMetricHistogram,
		Type: &metricTypeHistogram,
		Help: &testMetricHistogramDescription,
		Metric: []*dto.Metric{
			{
				Histogram: &dto.Histogram{
					SampleCount: intPtr(5),
					SampleSum:   floatPtr(13),
					Bucket: []*dto.Bucket{
						{
							CumulativeCount: intPtr(1),
							UpperBound:      floatPtr(1),
						},
						{
							CumulativeCount: intPtr(4),
							UpperBound:      floatPtr(3),
						},
						{
							CumulativeCount: intPtr(4),
							UpperBound:      floatPtr(5),
						},
						{
							CumulativeCount: intPtr(5),
							UpperBound:      floatPtr(math.Inf(1)),
						},
					},
				},
			},
		},
	},
}

func TestToCreateTimeSeriesRequest(t *testing.T) {
	const epsilon = float64(0.001)
	sampleTime := time.Unix(1234, 567)
	output := &bytes.Buffer{}
	translator := NewTranslator(log.NewLogfmtLogger(output),
		clock.Clock(clock.NewFakeClock(sampleTime)), "metrics.prefix", testResourceMappings)
	request := translator.ToCreateTimeSeriesRequest(metrics)
	if request == nil {
		t.Fatalf("Failed with error %v", output.String())
	} else if output.Len() > 0 {
		t.Logf("succeeded with messages %v", output.String())
	}

	ts := request.TimeSeries
	assert.Equal(t, 7, len(ts))

	// First two counter values.
	for i := 0; i <= 1; i++ {
		metric := ts[i]
		assert.Equal(t, "metrics.prefix/test_name", metric.Metric.Type)
		assert.Equal(t, "DOUBLE", metric.ValueType)
		assert.Equal(t, "CUMULATIVE", metric.MetricKind)

		assert.Equal(t, 1, len(metric.Points))
		assert.Equal(t, "2009-02-13T23:31:30Z", metric.Points[0].Interval.StartTime)

		labels := metric.Metric.Labels
		assert.Equal(t, 1, len(labels))

		if labels["labelName"] == "labelValue1" {
			assert.Equal(t, float64(42), *(metric.Points[0].Value.DoubleValue))
		} else if labels["labelName"] == "labelValue2" {
			assert.Equal(t, float64(106), *(metric.Points[0].Value.DoubleValue))
		} else {
			t.Errorf("Wrong label labelName value %s", labels["labelName"])
		}
	}

	// Then two gauge values.
	for i := 2; i <= 3; i++ {
		metric := ts[i]
		assert.Equal(t, "metrics.prefix/gauge_metric", metric.Metric.Type)
		assert.Equal(t, "DOUBLE", metric.ValueType)
		assert.Equal(t, "GAUGE", metric.MetricKind)

		labels := metric.Metric.Labels
		assert.Equal(t, 1, len(labels))
		if labels["labelName"] == "falseValue" {
			assert.Equal(t, float64(0.00001), *(metric.Points[0].Value.DoubleValue))
		} else if labels["labelName"] == "trueValue" {
			assert.Equal(t, float64(1.2), *(metric.Points[0].Value.DoubleValue))
		} else {
			t.Errorf("Wrong label labelName value %s", labels["labelName"])
		}
	}

	// Then a single cumulative float value.
	metric := ts[4]
	assert.Equal(t, "metrics.prefix/float_metric", metric.Metric.Type)
	assert.Equal(t, "DOUBLE", metric.ValueType)
	assert.Equal(t, "CUMULATIVE", metric.MetricKind)
	assert.InEpsilon(t, 123.17, *(metric.Points[0].Value.DoubleValue), epsilon)
	assert.Equal(t, 1, len(metric.Points))
	assert.Equal(t, "2009-02-13T23:31:30Z", metric.Points[0].Interval.StartTime)

	// Then a single gauge float value.
	metric = ts[5]
	assert.Equal(t, "metrics.prefix/process_start_time_seconds", metric.Metric.Type)
	assert.Equal(t, "DOUBLE", metric.ValueType)
	assert.Equal(t, "GAUGE", metric.MetricKind)
	assert.Equal(t, 1, len(metric.Points))
	assert.Equal(t, float64(1234567890), *(metric.Points[0].Value.DoubleValue))

	// Histogram
	metric = ts[6]
	assert.Equal(t, "metrics.prefix/test_histogram", metric.Metric.Type)
	assert.Equal(t, "DISTRIBUTION", metric.ValueType)
	assert.Equal(t, "CUMULATIVE", metric.MetricKind)
	assert.Equal(t, 1, len(metric.Points))

	p := metric.Points[0]

	dist := p.Value.DistributionValue
	assert.NotNil(t, dist)
	assert.Equal(t, int64(5), dist.Count)
	assert.InEpsilon(t, 2.6, dist.Mean, epsilon)
	assert.InEpsilon(t, 11.25, dist.SumOfSquaredDeviation, epsilon)

	bounds := dist.BucketOptions.ExplicitBuckets.Bounds
	assert.Equal(t, 3, len(bounds))
	assert.InEpsilon(t, 1, bounds[0], epsilon)
	assert.InEpsilon(t, 3, bounds[1], epsilon)
	assert.InEpsilon(t, 5, bounds[2], epsilon)

	counts := dist.BucketCounts
	assert.Equal(t, 4, len(counts))
	assert.Equal(t, int64(1), counts[0])
	assert.Equal(t, int64(3), counts[1])
	assert.Equal(t, int64(0), counts[2])
	assert.Equal(t, int64(1), counts[3])
}

func TestUnknownMonitoredResource(t *testing.T) {
	resourceMappings := []ResourceMap{
		{
			// Right now this must be gke_container. See TODO in translateFamily().
			Type: "gke_container",
			LabelMap: map[string]string{
				"_kubernetes_label": "stackdriver_label",
			},
		},
	}
	metrics := []*dto.MetricFamily{
		{
			Name: &testMetricName,
			Type: &metricTypeCounter,
			Help: &testMetricDescription,
			Metric: []*dto.Metric{
				{
					Label: []*dto.LabelPair{
						{
							Name:  stringPtr("labelName"),
							Value: stringPtr("labelValue1"),
						},
					},
					Counter: &dto.Counter{Value: floatPtr(42.0)},
				},
			},
		},
	}
	sampleTime := time.Unix(1234, 567)

	output := &bytes.Buffer{}
	translator := NewTranslator(log.NewLogfmtLogger(output),
		clock.Clock(clock.NewFakeClock(sampleTime)), "metrics.prefix", resourceMappings)
	request := translator.ToCreateTimeSeriesRequest(metrics)
	if len(request.TimeSeries) > 0 {
		t.Fatalf("expected empty request, but got %v", request)
	}
	if !strings.Contains(output.String(), "cannot extract Stackdriver monitored resource") {
		t.Fatalf("missing \"cannot extract Stackdriver monitored resource\" from the output %v", output.String())
	}
}

func TestDropsInternalLabels(t *testing.T) {
	metrics := []*dto.MetricFamily{
		{
			Name: &testMetricName,
			Type: &metricTypeCounter,
			Help: &testMetricDescription,
			Metric: []*dto.Metric{
				{
					Label: []*dto.LabelPair{
						{
							Name:  stringPtr("keep"),
							Value: stringPtr("x"),
						},
						{
							Name:  stringPtr("_drop"),
							Value: stringPtr("y"),
						},
					},
					Counter: &dto.Counter{Value: floatPtr(42.0)},
				},
			},
		},
		{
			Name: stringPtr(processStartTimeMetric),
			Type: &metricTypeGauge,
			Metric: []*dto.Metric{
				{
					Gauge: &dto.Gauge{Value: floatPtr(1234567890.0)},
				},
			},
		},
	}
	sampleTime := time.Unix(1234, 567)

	output := &bytes.Buffer{}
	translator := NewTranslator(log.NewLogfmtLogger(output),
		clock.Clock(clock.NewFakeClock(sampleTime)), "metrics.prefix", testResourceMappings)
	request := translator.ToCreateTimeSeriesRequest(metrics)
	if request == nil {
		t.Fatalf("Failed with error %v", output.String())
	} else if output.Len() > 0 {
		t.Logf("succeeded with messages %v", output.String())
	}

	metric := request.TimeSeries[0]
	assert.Equal(t, "metrics.prefix/test_name", metric.Metric.Type)
	assert.Equal(t, "DOUBLE", metric.ValueType)
	assert.Equal(t, "CUMULATIVE", metric.MetricKind)

	assert.Equal(t, 1, len(metric.Points))
	assert.Equal(t, "2009-02-13T23:31:30Z", metric.Points[0].Interval.StartTime)

	labels := metric.Metric.Labels
	assert.Equal(t, 1, len(labels))

	if value, ok := labels["keep"]; !ok {
		t.Errorf("Expected \"keep\" label, found %v", labels)
	} else {
		assert.Equal(t, "x", value)
	}
	assert.Equal(t, float64(42), *(metric.Points[0].Value.DoubleValue))
}

func floatPtr(val float64) *float64 {
	ptr := val
	return &ptr
}

func intPtr(val uint64) *uint64 {
	ptr := val
	return &ptr
}

func stringPtr(val string) *string {
	ptr := val
	return &ptr
}
