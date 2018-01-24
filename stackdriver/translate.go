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
	"fmt"
	"math"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/golang/glog"
	dto "github.com/prometheus/client_model/go"
	monitoring "google.golang.org/api/monitoring/v3"
)

const (
	// Built-in Prometheus metric exporting process start time.
	processStartTimeMetric = "process_start_time_seconds"
)

var supportedMetricTypes = map[dto.MetricType]bool{
	dto.MetricType_COUNTER:   true,
	dto.MetricType_GAUGE:     true,
	dto.MetricType_HISTOGRAM: true,
}

const falseValueEpsilon = 0.001

// Translator allows converting Prometheus samples to Stackdriver TimeSeries.
type Translator struct {
	logger           log.Logger
	clock            clock.Clock
	metricsPrefix    string
	resourceMappings []ResourceMap
}

// NewTranslator creates a new Translator.
func NewTranslator(logger log.Logger, clock clock.Clock, metricsPrefix string, resourceMappings []ResourceMap) *Translator {
	return &Translator{
		logger:           logger,
		clock:            clock,
		metricsPrefix:    metricsPrefix,
		resourceMappings: resourceMappings,
	}
}

// ToCreateTimeSeriesRequest translates metrics in Prometheus format to Stackdriver format.
func (t *Translator) ToCreateTimeSeriesRequest(
	metrics []*dto.MetricFamily) *monitoring.CreateTimeSeriesRequest {

	startTime, err := getStartTime(metrics)
	if err != nil {
		level.Error(t.logger).Log("sample_metric", metrics[0], "err", err)
		// Continue with the default startTime.
	}

	// TODO(jkohen): See if it's possible for Prometheus to pass two points
	// for the same time series, which isn't accepted by the Stackdriver
	// Monitoring API.
	request := &monitoring.CreateTimeSeriesRequest{
		TimeSeries: []*monitoring.TimeSeries{},
	}
	for _, family := range metrics {
		tss, err := t.translateFamily(family, startTime)
		if err != nil {
			level.Warn(t.logger).Log(
				"msg", "error while processing metric",
				"metric", family.GetName(),
				"err", err)
		} else {
			request.TimeSeries = append(request.TimeSeries, tss...)
		}
	}
	return request
}

func getStartTime(metrics []*dto.MetricFamily) (time.Time, error) {
	// For cumulative metrics we need to know process start time.
	for _, family := range metrics {
		if family.GetName() == processStartTimeMetric &&
			family.GetType() == dto.MetricType_GAUGE &&
			len(family.GetMetric()) == 1 {

			value := family.Metric[0].Gauge.GetValue()
			startSeconds := math.Trunc(value)
			startNanos := 1000000000 * (value - startSeconds)
			return time.Unix(int64(startSeconds), int64(startNanos)), nil
		}
	}
	// If the process start time is not specified, assuming it's
	// the unix 1 second, because Stackdriver can't handle
	// unix zero or unix negative number.
	return time.Unix(1, 0),
		fmt.Errorf("metric %s invalid or not defined, cumulative will be inaccurate",
			processStartTimeMetric)
}

func (t *Translator) translateFamily(family *dto.MetricFamily,
	startTime time.Time) ([]*monitoring.TimeSeries, error) {

	var tss []*monitoring.TimeSeries
	if _, found := supportedMetricTypes[family.GetType()]; !found {
		return tss, fmt.Errorf("Metric type %v of family %s not supported", family.GetType(), family.GetName())
	}
	for _, metric := range family.GetMetric() {
		ts, err := t.translateOne(family.GetName(), family.GetType(), metric, startTime)
		if err != nil {
			return nil, err
		}
		// TODO(jkohen): Remove once the whitelist goes away, or at
		// least make this controlled by a flag.
		if strings.HasPrefix(ts.Resource.Type, "k8s") {
			// The new k8s MonitoredResource types are still behind
			// a whitelist. Drop silently for now to avoid errors in
			// the logs.
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
		return nil,
			fmt.Errorf("cannot extract Stackdriver monitored resource from metric %v of family %s", metric, name)
	}
	interval := &monitoring.TimeInterval{
		EndTime: time.Now().UTC().Format(time.RFC3339),
	}
	metricKind := extractMetricKind(mType)
	if metricKind == "CUMULATIVE" {
		interval.StartTime = start.UTC().Format(time.RFC3339)
	}
	valueType := extractValueType(mType)
	point := &monitoring.Point{
		Interval: interval,
		Value: &monitoring.TypedValue{
			ForceSendFields: []string{},
		},
	}
	setValue(mType, valueType, metric, point)

	return &monitoring.TimeSeries{
		Metric: &monitoring.Metric{
			Labels: getMetricLabels(metric.GetLabel()),
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
// By convention it excludes any Prometheus labels with "_" prefix.
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
