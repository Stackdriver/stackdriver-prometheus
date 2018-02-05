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
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/golang/glog"
	"github.com/jkohen/prometheus/retrieval"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/pkg/timestamp"
	monitoring "google.golang.org/api/monitoring/v3"
)

var supportedMetricTypes = map[dto.MetricType]bool{
	dto.MetricType_COUNTER:   true,
	dto.MetricType_GAUGE:     true,
	dto.MetricType_HISTOGRAM: true,
}

const (
	falseValueEpsilon = 0.001
	maxLabelCount     = 10
)

type unsupportedTypeError struct {
	metricType dto.MetricType
}

func (e *unsupportedTypeError) Error() string {
	return e.metricType.String()
}

// Translator allows converting Prometheus samples to Stackdriver TimeSeries.
type Translator struct {
	logger           log.Logger
	metricsPrefix    string
	resourceMappings []ResourceMap
}

// NewTranslator creates a new Translator.
func NewTranslator(logger log.Logger, metricsPrefix string, resourceMappings []ResourceMap) *Translator {
	return &Translator{
		logger:           logger,
		metricsPrefix:    metricsPrefix,
		resourceMappings: resourceMappings,
	}
}

// ToCreateTimeSeriesRequest translates metrics in Prometheus format to Stackdriver format.
func (t *Translator) ToCreateTimeSeriesRequest(
	metrics []*retrieval.MetricFamily) *monitoring.CreateTimeSeriesRequest {

	// TODO(jkohen): See if it's possible for Prometheus to pass two points
	// for the same time series, which isn't accepted by the Stackdriver
	// Monitoring API.
	request := &monitoring.CreateTimeSeriesRequest{
		TimeSeries: []*monitoring.TimeSeries{},
	}
	for _, family := range metrics {
		tss, err := t.translateFamily(family)
		if err != nil {
			// Ignore unsupported type errors, they're just noise.
			if _, ok := err.(*unsupportedTypeError); !ok {
				level.Warn(t.logger).Log(
					"msg", "error while processing metric",
					"metric", family.GetName(),
					"err", err)
			}
		} else {
			request.TimeSeries = append(request.TimeSeries, tss...)
		}
	}
	return request
}

func (t *Translator) translateFamily(family *retrieval.MetricFamily) ([]*monitoring.TimeSeries, error) {
	var tss []*monitoring.TimeSeries
	if _, found := supportedMetricTypes[family.GetType()]; !found {
		return tss, &unsupportedTypeError{family.GetType()}
	}
	for i, metric := range family.GetMetric() {
		startTime := timestamp.Time(family.MetricResetTimestampMs[i])
		ts, err := t.translateOne(family.GetName(), family.GetType(), metric, startTime)
		if err != nil {
			// Metrics are usually independent, so just drop this one.
			level.Warn(t.logger).Log(
				"msg", "error while processing metric",
				"family", family.GetName(),
				"metric", metric,
				"err", err)
			continue
		}
		tss = append(tss, ts)
	}
	return tss, nil
}

// getMetricType creates metric type name base on the metric prefix, and metric name.
func getMetricType(metricsPrefix string, name string) string {
	return fmt.Sprintf("%s/%s", metricsPrefix, name)
}

// assumes that mType is Counter, Gauge or Histogram. Returns nil on error.
func (t *Translator) translateOne(name string,
	mType dto.MetricType,
	metric *dto.Metric,
	start time.Time) (*monitoring.TimeSeries, error) {
	monitoredResource := t.getMonitoredResource(metric)
	if monitoredResource == nil {
		return nil, errors.New("cannot extract Stackdriver monitored resource from metric")
	}
	interval := &monitoring.TimeInterval{
		EndTime: timestamp.Time(metric.GetTimestampMs()).UTC().Format(time.RFC3339Nano),
	}
	metricKind := extractMetricKind(mType)
	if metricKind == "CUMULATIVE" {
		interval.StartTime = start.UTC().Format(time.RFC3339Nano)
	}
	valueType := extractValueType(mType)
	point := &monitoring.Point{
		Interval: interval,
		Value: &monitoring.TypedValue{
			ForceSendFields: []string{},
		},
	}
	setValue(mType, valueType, metric, point)

	tsLabels := getMetricLabels(metric.GetLabel())
	if len(tsLabels) > maxLabelCount {
		return nil, fmt.Errorf(
			"dropping metric because it has more than %v labels, and Stackdriver would reject it",
			maxLabelCount)
	}
	return &monitoring.TimeSeries{
		Metric: &monitoring.Metric{
			Labels: tsLabels,
			Type:   getMetricType(t.metricsPrefix, name),
		},
		Resource:   monitoredResource,
		MetricKind: metricKind,
		ValueType:  valueType,
		Points:     []*monitoring.Point{point},
	}, nil
}

func setValue(mType dto.MetricType, valueType string, metric *dto.Metric, point *monitoring.Point) {
	if mType == dto.MetricType_GAUGE {
		setValueBaseOnSimpleType(metric.GetGauge().GetValue(), valueType, point)
	} else if mType == dto.MetricType_HISTOGRAM {
		point.Value.DistributionValue = convertToDistributionValue(metric.GetHistogram())
		point.ForceSendFields = append(point.ForceSendFields, "DistributionValue")
	} else {
		setValueBaseOnSimpleType(metric.GetCounter().GetValue(), valueType, point)
	}
}

func setValueBaseOnSimpleType(value float64, valueType string, point *monitoring.Point) {
	if valueType == "INT64" {
		val := int64(value)
		point.Value.Int64Value = &val
		point.ForceSendFields = append(point.ForceSendFields, "Int64Value")
	} else if valueType == "DOUBLE" {
		point.Value.DoubleValue = &value
		point.ForceSendFields = append(point.ForceSendFields, "DoubleValue")
	} else if valueType == "BOOL" {
		var val = math.Abs(value) > falseValueEpsilon
		point.Value.BoolValue = &val
		point.ForceSendFields = append(point.ForceSendFields, "BoolValue")
	} else {
		glog.Errorf("Value type '%s' is not supported yet.", valueType)
	}
}

func convertToDistributionValue(h *dto.Histogram) *monitoring.Distribution {
	count := int64(h.GetSampleCount())
	mean := float64(0)
	dev := float64(0)
	bounds := []float64{}
	values := []int64{}

	if count > 0 {
		mean = h.GetSampleSum() / float64(count)
	}

	prevVal := uint64(0)
	lower := float64(0)
	for _, b := range h.Bucket {
		upper := b.GetUpperBound()
		if !math.IsInf(b.GetUpperBound(), 1) {
			bounds = append(bounds, b.GetUpperBound())
		} else {
			upper = lower
		}
		val := b.GetCumulativeCount() - prevVal
		x := (lower + upper) / float64(2)
		dev += float64(val) * (x - mean) * (x - mean)

		values = append(values, int64(b.GetCumulativeCount()-prevVal))

		lower = b.GetUpperBound()
		prevVal = b.GetCumulativeCount()
	}

	return &monitoring.Distribution{
		Count: count,
		Mean:  mean,
		SumOfSquaredDeviation: dev,
		BucketOptions: &monitoring.BucketOptions{
			ExplicitBuckets: &monitoring.Explicit{
				Bounds: bounds,
			},
		},
		BucketCounts: values,
	}
}

// getMetricLabels returns a Stackdriver label map from the label.
//
// By convention it excludes any Prometheus labels with "_" prefix, which
// includes the labels that correspond to Stackdriver resource labels.
func getMetricLabels(labels []*dto.LabelPair) map[string]string {
	metricLabels := map[string]string{}
	for _, label := range labels {
		if strings.HasPrefix(string(label.GetName()), "_") {
			continue
		}
		metricLabels[label.GetName()] = label.GetValue()
	}
	return metricLabels
}

func extractMetricKind(mType dto.MetricType) string {
	if mType == dto.MetricType_COUNTER || mType == dto.MetricType_HISTOGRAM {
		return "CUMULATIVE"
	}
	return "GAUGE"
}

func extractValueType(mType dto.MetricType) string {
	if mType == dto.MetricType_HISTOGRAM {
		return "DISTRIBUTION"
	}
	return "DOUBLE"
}

func (t *Translator) getMonitoredResource(metric *dto.Metric) *monitoring.MonitoredResource {
	for _, resource := range t.resourceMappings {
		if labels := resource.Translate(metric); labels != nil {
			return &monitoring.MonitoredResource{
				Type:   resource.Type,
				Labels: labels,
			}
		}
	}
	return nil
}
