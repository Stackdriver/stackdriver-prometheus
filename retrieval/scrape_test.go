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

package retrieval

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/stretchr/testify/require"
)

const (
	processStartTimeMs          = int64(1234567890432)
	processStartTimeSecondsText = "# TYPE process_start_time_seconds gauge\n" +
		"process_start_time_seconds 1234567890.4321 1234568000432\n"
)

// Implements resetPointMapper.
type fakeResetPointMap struct {
}

func (m *fakeResetPointMap) GetResetPoint(key ResetPointKey) (point *Point) {
	return &Point{Timestamp: time.Time{}, ResetValue: &PointValue{Counter: 1}}
}

func (m *fakeResetPointMap) AddResetPoint(key ResetPointKey, point Point) {
}

func (m *fakeResetPointMap) HasResetPoints() bool {
	// If always false, no points will be written.
	return true
}

// Implements resetPointMapper.
type simpleResetPointMap struct {
	m map[ResetPointKey]Point
}

func newSimpleResetPointMap() simpleResetPointMap {
	return simpleResetPointMap{
		m: map[ResetPointKey]Point{},
	}
}

func (m *simpleResetPointMap) GetResetPoint(key ResetPointKey) *Point {
	if point, ok := m.m[key]; ok {
		return &point
	}
	return nil
}

func (m *simpleResetPointMap) AddResetPoint(key ResetPointKey, point Point) {
	m.m[key] = point
}

func (m *simpleResetPointMap) HasResetPoints() bool {
	return len(m.m) > 0
}

func makeProcessStartTimeMetric() *MetricFamily {
	return &MetricFamily{
		MetricFamily: &dto.MetricFamily{
			Name: proto.String(processStartTimeMetricName),
			Type: dto.MetricType_GAUGE.Enum(),
			Metric: []*dto.Metric{
				{
					Gauge:       &dto.Gauge{Value: proto.Float64(1234567890.4321)},
					TimestampMs: proto.Int64(1234568000432),
				},
			},
		},
		MetricResetTimestampMs: []int64{
			NoTimestamp,
		},
	}
}

func TestNewScrapePool(t *testing.T) {
	var (
		app = &nopAppendable{}
		cfg = &config.ScrapeConfig{}
		sp  = newScrapePool(cfg, app, nil)
	)

	if a, ok := sp.appendable.(*nopAppendable); !ok || a != app {
		t.Fatalf("Wrong sample appender")
	}
	if sp.config != cfg {
		t.Fatalf("Wrong scrape config")
	}
	if sp.newLoop == nil {
		t.Fatalf("newLoop function not initialized")
	}
}

type testLoop struct {
	startFunc func(interval, timeout time.Duration, errc chan<- error)
	stopFunc  func()
}

func (l *testLoop) run(interval, timeout time.Duration, errc chan<- error) {
	l.startFunc(interval, timeout, errc)
}

func (l *testLoop) stop() {
	l.stopFunc()
}

func TestScrapePoolStop(t *testing.T) {
	sp := &scrapePool{
		targets: map[uint64]*Target{},
		loops:   map[uint64]loop{},
		cancel:  func() {},
	}
	var mtx sync.Mutex
	stopped := map[uint64]bool{}
	numTargets := 20

	// Stopping the scrape pool must call stop() on all scrape loops,
	// clean them and the respective targets up. It must wait until each loop's
	// stop function returned before returning itself.

	for i := 0; i < numTargets; i++ {
		t := &Target{
			labels: labels.FromStrings(model.AddressLabel, fmt.Sprintf("example.com:%d", i)),
		}
		l := &testLoop{}
		l.stopFunc = func() {
			time.Sleep(time.Duration(i*20) * time.Millisecond)

			mtx.Lock()
			stopped[t.hash()] = true
			mtx.Unlock()
		}

		sp.targets[t.hash()] = t
		sp.loops[t.hash()] = l
	}

	done := make(chan struct{})
	stopTime := time.Now()

	go func() {
		sp.stop()
		close(done)
	}()

	select {
	case <-time.After(5 * time.Second):
		t.Fatalf("scrapeLoop.stop() did not return as expected")
	case <-done:
		// This should have taken at least as long as the last target slept.
		if time.Since(stopTime) < time.Duration(numTargets*20)*time.Millisecond {
			t.Fatalf("scrapeLoop.stop() exited before all targets stopped")
		}
	}

	mtx.Lock()
	if len(stopped) != numTargets {
		t.Fatalf("Expected 20 stopped loops, got %d", len(stopped))
	}
	mtx.Unlock()

	if len(sp.targets) > 0 {
		t.Fatalf("Targets were not cleared on stopping: %d left", len(sp.targets))
	}
	if len(sp.loops) > 0 {
		t.Fatalf("Loops were not cleared on stopping: %d left", len(sp.loops))
	}
}

func TestScrapePoolReload(t *testing.T) {
	var mtx sync.Mutex
	numTargets := 20

	stopped := map[uint64]bool{}

	reloadCfg := &config.ScrapeConfig{
		ScrapeInterval: model.Duration(3 * time.Second),
		ScrapeTimeout:  model.Duration(2 * time.Second),
	}
	// On starting to run, new loops created on reload check whether their preceding
	// equivalents have been stopped.
	newLoop := func(_ *Target, s scraper) loop {
		l := &testLoop{}
		l.startFunc = func(interval, timeout time.Duration, errc chan<- error) {
			if interval != 3*time.Second {
				t.Errorf("Expected scrape interval %d but got %d", 3*time.Second, interval)
			}
			if timeout != 2*time.Second {
				t.Errorf("Expected scrape timeout %d but got %d", 2*time.Second, timeout)
			}
			mtx.Lock()
			if !stopped[s.(*targetScraper).hash()] {
				t.Errorf("Scrape loop for %v not stopped yet", s.(*targetScraper))
			}
			mtx.Unlock()
		}
		return l
	}
	sp := &scrapePool{
		appendable: &nopAppendable{},
		targets:    map[uint64]*Target{},
		loops:      map[uint64]loop{},
		newLoop:    newLoop,
		logger:     nil,
	}

	// Reloading a scrape pool with a new scrape configuration must stop all scrape
	// loops and start new ones. A new loop must not be started before the preceding
	// one terminated.

	for i := 0; i < numTargets; i++ {
		t := &Target{
			labels: labels.FromStrings(model.AddressLabel, fmt.Sprintf("example.com:%d", i)),
		}
		l := &testLoop{}
		l.stopFunc = func() {
			time.Sleep(time.Duration(i*20) * time.Millisecond)

			mtx.Lock()
			stopped[t.hash()] = true
			mtx.Unlock()
		}

		sp.targets[t.hash()] = t
		sp.loops[t.hash()] = l
	}
	done := make(chan struct{})

	beforeTargets := map[uint64]*Target{}
	for h, t := range sp.targets {
		beforeTargets[h] = t
	}

	reloadTime := time.Now()

	go func() {
		sp.reload(reloadCfg)
		close(done)
	}()

	select {
	case <-time.After(5 * time.Second):
		t.Fatalf("scrapeLoop.reload() did not return as expected")
	case <-done:
		// This should have taken at least as long as the last target slept.
		if time.Since(reloadTime) < time.Duration(numTargets*20)*time.Millisecond {
			t.Fatalf("scrapeLoop.stop() exited before all targets stopped")
		}
	}

	mtx.Lock()
	if len(stopped) != numTargets {
		t.Fatalf("Expected 20 stopped loops, got %d", len(stopped))
	}
	mtx.Unlock()

	if !reflect.DeepEqual(sp.targets, beforeTargets) {
		t.Fatalf("Reloading affected target states unexpectedly")
	}
	if len(sp.loops) != numTargets {
		t.Fatalf("Expected %d loops after reload but got %d", numTargets, len(sp.loops))
	}
}

func TestScrapePoolAppender(t *testing.T) {
	cfg := &config.ScrapeConfig{}
	app := &nopAppendable{}
	sp := newScrapePool(cfg, app, nil)

	wrapped := sp.appender()

	tl, ok := wrapped.(*timeLimitAppender)
	if !ok {
		t.Fatalf("Expected timeLimitAppender but got %T", wrapped)
	}
	if _, ok := tl.Appender.(nopAppender); !ok {
		t.Fatalf("Expected base appender but got %T", tl.Appender)
	}

	cfg.SampleLimit = 100

	wrapped = sp.appender()

	sl, ok := wrapped.(*limitAppender)
	if !ok {
		t.Fatalf("Expected limitAppender but got %T", wrapped)
	}
	tl, ok = sl.Appender.(*timeLimitAppender)
	if !ok {
		t.Fatalf("Expected limitAppender but got %T", sl.Appender)
	}
	if _, ok := tl.Appender.(nopAppender); !ok {
		t.Fatalf("Expected base appender but got %T", tl.Appender)
	}
}

func TestScrapeLoopStopBeforeRun(t *testing.T) {
	scraper := &testScraper{}

	sl := newScrapeLoop(context.Background(),
		scraper, &fakeResetPointMap{},
		nil, nil,
		nopMutator,
		nopMutator,
		nil,
	)

	// The scrape pool synchronizes on stopping scrape loops. However, new scrape
	// loops are started asynchronously. Thus it's possible, that a loop is stopped
	// again before having started properly.
	// Stopping not-yet-started loops must block until the run method was called and exited.
	// The run method must exit immediately.

	stopDone := make(chan struct{})
	go func() {
		sl.stop()
		close(stopDone)
	}()

	select {
	case <-stopDone:
		t.Fatalf("Stopping terminated before run exited successfully")
	case <-time.After(500 * time.Millisecond):
	}

	// Running the scrape loop must exit before calling the scraper even once.
	scraper.scrapeFunc = func(context.Context, io.Writer) error {
		t.Fatalf("scraper was called for terminated scrape loop")
		return nil
	}

	runDone := make(chan struct{})
	go func() {
		sl.run(1, 0, nil)
		close(runDone)
	}()

	select {
	case <-runDone:
	case <-time.After(1 * time.Second):
		t.Fatalf("Running terminated scrape loop did not exit")
	}

	select {
	case <-stopDone:
	case <-time.After(1 * time.Second):
		t.Fatalf("Stopping did not terminate after running exited")
	}
}

func nopMutator(l labels.Labels) labels.Labels { return l }

func TestScrapeLoopStop(t *testing.T) {
	var (
		signal   = make(chan struct{})
		appender = &collectResultAppender{}
		scraper  = &testScraper{}
		app      = func() Appender { return appender }
	)
	defer close(signal)

	sl := newScrapeLoop(context.Background(),
		scraper, &fakeResetPointMap{},
		nil, nil,
		nopMutator,
		nopMutator,
		app,
	)

	// Terminate loop after 2 scrapes.
	numScrapes := 0

	scraper.scrapeFunc = func(ctx context.Context, w io.Writer) error {
		numScrapes++
		if numScrapes == 2 {
			go sl.stop()
		}
		w.Write([]byte("metric_a 42\n"))
		return nil
	}

	go func() {
		sl.run(10*time.Millisecond, time.Hour, nil)
		signal <- struct{}{}
	}()

	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("Scrape wasn't stopped.")
	}

	// We expected 1 actual sample for each scrape plus 4 for report samples.
	// At least 2 scrapes were made.
	if len(appender.result) < 5*2 || len(appender.result)%5 != 0 {
		t.Fatalf("Expected at least 2 scrapes with 4 samples each, got %d samples", len(appender.result))
	}
	// All samples in a scrape must have the same timestmap.
	var ts int64
	for i, family := range appender.result {
		for _, metric := range family.Metric {
			if i%5 == 0 {
				ts = *metric.TimestampMs
			} else if *metric.TimestampMs != ts {
				t.Fatalf("Unexpected multiple timestamps within single scrape")
			}
		}
	}
}

func TestScrapeLoopRun(t *testing.T) {
	var (
		signal = make(chan struct{})
		errc   = make(chan error)

		scraper = &testScraper{}
		app     = func() Appender { return &nopAppender{} }
	)
	defer close(signal)

	ctx, cancel := context.WithCancel(context.Background())
	sl := newScrapeLoop(ctx,
		scraper, &fakeResetPointMap{},
		nil, nil,
		nopMutator,
		nopMutator,
		app,
	)

	// The loop must terminate during the initial offset if the context
	// is canceled.
	scraper.offsetDur = time.Hour

	go func() {
		sl.run(time.Second, time.Hour, errc)
		signal <- struct{}{}
	}()

	// Wait to make sure we are actually waiting on the offset.
	time.Sleep(1 * time.Second)

	cancel()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("Cancelation during initial offset failed")
	case err := <-errc:
		t.Fatalf("Unexpected error: %s", err)
	}

	// The provided timeout must cause cancelation of the context passed down to the
	// scraper. The scraper has to respect the context.
	scraper.offsetDur = 0

	block := make(chan struct{})
	scraper.scrapeFunc = func(ctx context.Context, _ io.Writer) error {
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}

	ctx, cancel = context.WithCancel(context.Background())
	sl = newScrapeLoop(ctx,
		scraper, &fakeResetPointMap{},
		nil, nil,
		nopMutator,
		nopMutator,
		app,
	)

	go func() {
		sl.run(time.Second, 100*time.Millisecond, errc)
		signal <- struct{}{}
	}()

	select {
	case err := <-errc:
		if err != context.DeadlineExceeded {
			t.Fatalf("Expected timeout error but got: %s", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("Expected timeout error but got none")
	}

	// We already caught the timeout error and are certainly in the loop.
	// Let the scrapes returns immediately to cause no further timeout errors
	// and check whether canceling the parent context terminates the loop.
	close(block)
	cancel()

	select {
	case <-signal:
		// Loop terminated as expected.
	case err := <-errc:
		t.Fatalf("Unexpected error: %s", err)
	case <-time.After(3 * time.Second):
		t.Fatalf("Loop did not terminate on context cancelation")
	}
}

func TestScrapeLoopAppend(t *testing.T) {
	app := &collectResultAppender{}

	sl := newScrapeLoop(context.Background(),
		nil, &fakeResetPointMap{},
		nil, nil,
		nopMutator,
		nopMutator,
		func() Appender { return app },
	)

	now := time.Now()
	_, _, err := sl.append([]byte(
		processStartTimeSecondsText+
			"metric_a 1\n"+
			"metric_b NaN\n"), now)
	if err != nil {
		t.Fatalf("Unexpected append error: %s", err)
	}
	result := app.Sorted()
	if len(result) < 3 || len(result[1].Metric) < 1 {
		t.Fatalf("expected 3 metrics; got %v", result)
	}
	// DeepEqual will report NaNs as being different, so replace with a different value.
	result[1].Metric[0].Untyped.Value = proto.Float64(42)
	want := []*MetricFamily{
		makeProcessStartTimeMetric(),
		untypedFromTriplet("metric_a", timestamp.FromTime(now), 1),
		untypedFromTriplet("metric_b", timestamp.FromTime(now), 42),
	}
	sort.Sort(ByName(want))
	if !reflect.DeepEqual(want, result) {
		t.Fatalf("Appended samples not as expected.\nWanted: %+v\nGot:    %+v", want, result)
	}
}

func TestScrapeLoopAppendMissingProcessStartTime(t *testing.T) {
	app := &collectResultAppender{}

	sl := newScrapeLoop(context.Background(),
		nil, &fakeResetPointMap{},
		nil, nil,
		nopMutator,
		nopMutator,
		func() Appender { return app },
	)

	now := time.Now()
	_, _, err := sl.append([]byte(
		"metric_a 1\n"+
			"metric_b 2\n"), now)
	if err != nil {
		t.Fatalf("Unexpected append error: %s", err)
	}
	want := []*MetricFamily{
		untypedFromTriplet("metric_a", timestamp.FromTime(now), 1),
		untypedFromTriplet("metric_b", timestamp.FromTime(now), 2),
	}
	// Unset the reset timestamp, because we don't pass processStartTimeMetricName in the input.
	for _, family := range want {
		for i, _ := range family.MetricResetTimestampMs {
			family.MetricResetTimestampMs[i] = NoTimestamp
		}
	}
	sort.Sort(ByName(want))
	if !reflect.DeepEqual(want, app.Sorted()) {
		t.Fatalf("Appended samples not as expected. Wanted: %+v Got: %+v", want, app.Sorted())
	}
}

func TestScrapeLoopMutator(t *testing.T) {
	app := &collectResultAppender{}

	sl := newScrapeLoop(context.Background(),
		nil, &fakeResetPointMap{},
		nil, nil,
		func(lset labels.Labels) labels.Labels {
			lb := labels.NewBuilder(lset)
			lb.Set("new_label", "foo")
			return lb.Labels()
		},
		nopMutator,
		func() Appender { return app },
	)

	now := time.Now()
	_, _, err := sl.append([]byte(
		processStartTimeSecondsText+
			"metric_a 1\n"+
			"metric_b 2\n"), now)
	if err != nil {
		t.Fatalf("Unexpected append error: %s", err)
	}
	result := app.Sorted()
	want := []*MetricFamily{
		addMetricLabels(makeProcessStartTimeMetric(), "new_label", "foo"),
		addMetricLabels(
			untypedFromTriplet("metric_a", timestamp.FromTime(now), 1), "new_label", "foo"),
		addMetricLabels(
			untypedFromTriplet("metric_b", timestamp.FromTime(now), 2), "new_label", "foo"),
	}
	sort.Sort(ByName(want))
	if !reflect.DeepEqual(want, result) {
		t.Fatalf("Appended samples not as expected. Wanted: %+v Got: %+v", want, result)
	}
}

func TestScrapeLoopMutatorDeletesMetric(t *testing.T) {
	app := &collectResultAppender{}

	sl := newScrapeLoop(context.Background(),
		nil, &fakeResetPointMap{},
		nil, nil,
		func(lset labels.Labels) labels.Labels {
			var lb labels.Labels
			for _, label := range lset {
				if label.Name == "delete" {
					if label.Value == "label" {
						// delete this label
						continue
					} else if label.Value == "metric" {
						// delete whole metric
						return nil
					}
				}
				lb = append(lb, label)
			}
			return lb
		},
		nopMutator,
		func() Appender { return app },
	)

	now := time.Now()
	_, _, err := sl.append([]byte(
		processStartTimeSecondsText+
			"metric_a{delete=\"metric\"} 1\n"+
			"metric_b{keep=\"x\"} 2\n"+
			"metric_b{delete=\"label\",keep=\"y\"} 3\n"), now)
	if err != nil {
		t.Fatalf("Unexpected append error: %s", err)
	}
	// DeepEqual will report NaNs as being different, so replace with a different value.
	result := app.Sorted()
	want := []*MetricFamily{
		{
			MetricFamily: &dto.MetricFamily{
				Name: proto.String("metric_b"),
				Type: dto.MetricType_UNTYPED.Enum(),
				Metric: []*dto.Metric{
					&dto.Metric{
						Untyped: &dto.Untyped{
							Value: proto.Float64(2),
						},
						TimestampMs: proto.Int64(timestamp.FromTime(now)),
						Label: []*dto.LabelPair{
							{
								Name:  proto.String("keep"),
								Value: proto.String("x"),
							},
						},
					},
					&dto.Metric{
						Untyped: &dto.Untyped{
							Value: proto.Float64(3),
						},
						TimestampMs: proto.Int64(timestamp.FromTime(now)),
						Label: []*dto.LabelPair{
							{
								Name:  proto.String("keep"),
								Value: proto.String("y"),
							},
						},
					},
				},
			},
			MetricResetTimestampMs: []int64{
				NoTimestamp,
				NoTimestamp,
			},
		},
	}
	sort.Sort(ByName(want))
	if !reflect.DeepEqual(want, result) {
		t.Fatalf("Appended samples not as expected. Wanted: %+v Got: %+v", want, result)
	}
}

func TestScrapeLoopAppendSampleLimit(t *testing.T) {
	resApp := &collectResultAppender{}
	app := &limitAppender{Appender: resApp, limit: 1}

	sl := newScrapeLoop(context.Background(),
		nil, &fakeResetPointMap{},
		nil, nil,
		nopMutator,
		nopMutator,
		func() Appender { return app },
	)

	// Get the value of the Counter before performing the append.
	beforeMetric := dto.Metric{}
	err := targetScrapeSampleLimit.Write(&beforeMetric)
	if err != nil {
		t.Fatal(err)
	}
	beforeMetricValue := beforeMetric.GetCounter().GetValue()

	now := time.Now()
	_, _, err = sl.append([]byte(
		processStartTimeSecondsText+
			"metric_a 1\n"+
			"metric_b 1\n"+
			"metric_c 1\n"), now)
	if err != errSampleLimit {
		t.Fatalf("Did not see expected sample limit error: %s", err)
	}

	// Check that the Counter has been incremented a simgle time for the scrape,
	// not multiple times for each sample.
	metric := dto.Metric{}
	err = targetScrapeSampleLimit.Write(&metric)
	if err != nil {
		t.Fatal(err)
	}
	value := metric.GetCounter().GetValue()
	if (value - beforeMetricValue) != 1 {
		t.Fatalf("Unexpected change of sample limit metric: %f", (value - beforeMetricValue))
	}

	// And verify that we got the samples that fit under the limit. We
	// cannot do a deep comparison, because the order of the metrics in the
	// input is undefined.
	if len(resApp.result) != 1 {
		t.Fatalf("Appended samples not as expected. Wanted 1 sample, got: %+v", resApp.result)
	}
}

func TestScrapeLoopAppendNoStalenessIfTimestamp(t *testing.T) {
	app := &collectResultAppender{}
	sl := newScrapeLoop(context.Background(),
		nil, &fakeResetPointMap{},
		nil, nil,
		nopMutator,
		nopMutator,
		func() Appender { return app },
	)

	now := time.Now()
	_, _, err := sl.append([]byte(
		processStartTimeSecondsText+
			"metric_a 1 1000\n"), now)
	if err != nil {
		t.Fatalf("Unexpected append error: %s", err)
	}
	_, _, err = sl.append([]byte(""), now.Add(time.Second))
	if err != nil {
		t.Fatalf("Unexpected append error: %s", err)
	}

	want := []*MetricFamily{
		makeProcessStartTimeMetric(),
		untypedFromTriplet("metric_a", 1000, 1),
	}
	sort.Sort(ByName(want))
	if !reflect.DeepEqual(want, app.Sorted()) {
		t.Fatalf("Appended samples not as expected. Wanted: %+v Got: %+v", want, app.Sorted())
	}
}

func TestScrapeLoopRunReportsTargetDownOnScrapeError(t *testing.T) {
	var (
		scraper  = &testScraper{}
		appender = &collectResultAppender{}
		app      = func() Appender { return appender }
	)

	ctx, cancel := context.WithCancel(context.Background())
	sl := newScrapeLoop(ctx,
		scraper, &fakeResetPointMap{},
		nil, nil,
		nopMutator,
		nopMutator,
		app,
	)

	scraper.scrapeFunc = func(ctx context.Context, w io.Writer) error {
		cancel()
		return fmt.Errorf("scrape failed")
	}

	sl.run(10*time.Millisecond, time.Hour, nil)

	if appender.result == nil {
		t.Fatalf("missing 'up' value; got no results")
	}
	if appender.result[0].Metric == nil ||
		appender.result[0].Metric[0].GetGauge().Value == nil {
		t.Fatalf("missing 'up' value; MetricFamily is <%v>", appender.result[0].String())
	}
	if *appender.result[0].Metric[0].Gauge.Value != 0 {
		t.Fatalf("bad 'up' value; want 0, got %v", *appender.result[0].Metric[0].Gauge.Value)
	}
}

func TestScrapeLoopRunReportsTargetDownOnInvalidUTF8(t *testing.T) {
	var (
		scraper  = &testScraper{}
		appender = &collectResultAppender{}
		app      = func() Appender { return appender }
	)

	ctx, cancel := context.WithCancel(context.Background())
	sl := newScrapeLoop(ctx,
		scraper, &fakeResetPointMap{},
		nil, nil,
		nopMutator,
		nopMutator,
		app,
	)

	scraper.scrapeFunc = func(ctx context.Context, w io.Writer) error {
		cancel()
		w.Write([]byte("a{l=\"\xff\"} 1\n"))
		return nil
	}

	sl.run(10*time.Millisecond, time.Hour, nil)

	if appender.result == nil {
		t.Fatalf("missing 'up' value; got no results")
	}
	if appender.result[0].Metric == nil ||
		appender.result[0].Metric[0].GetGauge().Value == nil {
		t.Fatalf("missing 'up' value; MetricFamily is <%v>", appender.result[0].String())
	}
	if *appender.result[0].Metric[0].Gauge.Value != 0 {
		t.Fatalf("bad 'up' value; want 0, got %v", appender.result[0].Metric[0].Gauge.Value)
	}
}

type errorAppender struct {
	collectResultAppender
}

func (app *errorAppender) Add(metricFamily *MetricFamily) error {
	switch *metricFamily.Name {
	case "out_of_order":
		return ErrOutOfOrderSample
	case "amend":
		return ErrDuplicateSampleForTimestamp
	case "out_of_bounds":
		return ErrOutOfBounds
	default:
		return app.collectResultAppender.Add(metricFamily)
	}
}

func TestScrapeLoopAppendGracefullyIfAmendOrOutOfOrderOrOutOfBounds(t *testing.T) {
	app := &errorAppender{}

	sl := newScrapeLoop(context.Background(),
		nil, &fakeResetPointMap{},
		nil, nil,
		nopMutator,
		nopMutator,
		func() Appender { return app },
	)

	now := time.Unix(1, 0)
	_, _, err := sl.append([]byte(
		processStartTimeSecondsText+
			"out_of_order 1\n"+
			"amend 1\n"+
			"normal 1\n"+
			"out_of_bounds 1\n"), now)
	if err != nil {
		t.Fatalf("Unexpected append error: %s", err)
	}
	want := []*MetricFamily{
		makeProcessStartTimeMetric(),
		untypedFromTriplet("normal", timestamp.FromTime(now), 1),
	}
	sort.Sort(ByName(want))
	if !reflect.DeepEqual(want, app.Sorted()) {
		t.Fatalf("Appended samples not as expected. Wanted: %+v Got: %+v", want, app.Sorted())
	}
}

func TestScrapeLoopOutOfBoundsTimeError(t *testing.T) {
	app := &collectResultAppender{}
	sl := newScrapeLoop(context.Background(),
		nil, &fakeResetPointMap{},
		nil, nil,
		nopMutator,
		nopMutator,
		func() Appender {
			return &timeLimitAppender{
				Appender: app,
				maxTime:  timestamp.FromTime(time.Now().Add(10 * time.Minute)),
			}
		},
	)

	now := time.Now().Add(20 * time.Minute)
	total, added, err := sl.append([]byte(
		processStartTimeSecondsText+
			"normal 1\n"), now)
	if total != 2 {
		t.Error("expected 2 metrics")
		return
	}

	if added != 1 {
		t.Errorf("only metric %v should be added", processStartTimeMetricName)
	}

	if err != nil {
		t.Errorf("expect no error, got %s", err.Error())
	}
}

func TestPointExtractorWithoutProcessStartTime(t *testing.T) {
	app := &collectResultAppender{}

	resetPointMap := newSimpleResetPointMap()
	sl := newScrapeLoop(context.Background(),
		nil, &resetPointMap,
		nil, nil,
		nopMutator,
		nopMutator,
		func() Appender { return app },
	)

	now := time.Now()
	// Step #1. No resource tracker, reset time is unknown, so drop the point.
	{
		if _, _, err := sl.append([]byte(
			"# TYPE metric_a counter\n"+
				"metric_a 10\n"+
				"# TYPE metric_h histogram\n"+
				"metric_h{le=\"100\"} 1\n"+
				"metric_h{le=\"+Inf\"} 10\n"+
				"metric_h_count 10\n"+
				"metric_h_sum 123.1\n"), now); err != nil {
			t.Fatalf("Unexpected append error: %s", err)
		}
		want := []*MetricFamily{}
		sort.Sort(ByName(want))
		if !reflect.DeepEqual(want, app.Sorted()) {
			t.Fatalf("Appended samples not as expected.\nWanted: %+v\nGot:    %+v", want, app.Sorted())
		}
		app.Reset()
	}
	// Step #2. Reset time for metric_a is extracted from the point
	// tracker. New time series metric_b shows up, has reset time equal to
	// point time.
	{
		existingReset := now
		now = now.Add(10 * time.Second)
		if _, _, err := sl.append([]byte(
			"# TYPE metric_a counter\n"+
				"metric_a 20\n"+
				"# TYPE metric_b counter\n"+
				"metric_b 11\n"+
				"# TYPE metric_h histogram\n"+
				"metric_h{le=\"100\"} 11\n"+
				"metric_h{le=\"+Inf\"} 30\n"+
				"metric_h_count 30\n"+
				"metric_h_sum 223.1\n"), now); err != nil {
			t.Fatalf("Unexpected append error: %s", err)
		}
		want := []*MetricFamily{
			counterFromComponents("metric_a", timestamp.FromTime(now), timestamp.FromTime(existingReset), 10),
			counterFromComponents("metric_b", timestamp.FromTime(now), timestamp.FromTime(now), 11),
			histogramFromComponents("metric_h", timestamp.FromTime(now), timestamp.FromTime(existingReset), 10, 20, 100),
		}
		sort.Sort(ByName(want))
		if !reflect.DeepEqual(want, app.Sorted()) {
			t.Fatalf("Appended samples not as expected.\nWanted: %+v\nGot:    %+v", want, app.Sorted())
		}
		app.Reset()
	}
	// Step #3. All metrics were reset (the counter decreased). Their reset
	// timestamp should be the same as the point timestamp.
	{
		now = now.Add(10 * time.Second)
		if _, _, err := sl.append([]byte(
			"# TYPE metric_a counter\n"+
				"metric_a 10\n"+
				"# TYPE metric_b counter\n"+
				"metric_b 1\n"+
				"# TYPE metric_h histogram\n"+
				"metric_h{le=\"100\"} 1\n"+
				"metric_h{le=\"+Inf\"} 10\n"+
				"metric_h_count 10\n"+
				"metric_h_sum 23.1\n"), now); err != nil {
			t.Fatalf("Unexpected append error: %s", err)
		}
		want := []*MetricFamily{
			counterFromComponents("metric_a", timestamp.FromTime(now), timestamp.FromTime(now), 10),
			counterFromComponents("metric_b", timestamp.FromTime(now), timestamp.FromTime(now), 1),
			histogramFromComponents("metric_h", timestamp.FromTime(now), timestamp.FromTime(now), 1, 10, 23.1),
		}
		sort.Sort(ByName(want))
		if !reflect.DeepEqual(want, app.Sorted()) {
			t.Fatalf("Appended samples not as expected.\nWanted: %+v\nGot:    %+v", want, app.Sorted())
		}
		app.Reset()
	}
}

func TestPointExtractorWithProcessStartTime(t *testing.T) {
	app := &collectResultAppender{}

	resetPointMap := newSimpleResetPointMap()
	sl := newScrapeLoop(context.Background(),
		nil, &resetPointMap,
		nil, nil,
		nopMutator,
		nopMutator,
		func() Appender { return app },
	)

	now := time.Now()
	// Step #1. No resource tracker, reset time is extracted from process_start_time_seconds.
	{
		if _, _, err := sl.append([]byte(
			processStartTimeSecondsText+
				"# TYPE metric_a counter\n"+
				"metric_a 10\n"), now); err != nil {
			t.Fatalf("Unexpected append error: %s", err)
		}
		want := []*MetricFamily{
			makeProcessStartTimeMetric(),
			counterFromComponents("metric_a", timestamp.FromTime(now), processStartTimeMs, 10),
		}
		sort.Sort(ByName(want))
		if !reflect.DeepEqual(want, app.Sorted()) {
			t.Fatalf("Appended samples not as expected.\nWanted: %+v\nGot:    %+v", want, app.Sorted())
		}
		app.Reset()
	}
	// Step #2. Reset time for metric_a is extracted from the point
	// tracker. New time series metric_b shows up, has reset time equal to
	// point time.
	{
		now = now.Add(10 * time.Second)
		if _, _, err := sl.append([]byte(
			processStartTimeSecondsText+
				"# TYPE metric_a counter\n"+
				"metric_a 20\n"+
				"# TYPE metric_b counter\n"+
				"metric_b 11\n"), now); err != nil {
			t.Fatalf("Unexpected append error: %s", err)
		}
		want := []*MetricFamily{
			makeProcessStartTimeMetric(),
			counterFromComponents("metric_a", timestamp.FromTime(now), processStartTimeMs, 20),
			counterFromComponents("metric_b", timestamp.FromTime(now), timestamp.FromTime(now), 11),
		}
		sort.Sort(ByName(want))
		if !reflect.DeepEqual(want, app.Sorted()) {
			t.Fatalf("Appended samples not as expected.\nWanted: %+v\nGot:    %+v", want, app.Sorted())
		}
		app.Reset()
	}
	// Step #3. metric_a and metric_b were reset (the counter
	// decreased). Their reset timestamp should be the same as the point
	// timestamp.
	{
		now = now.Add(10 * time.Second)
		if _, _, err := sl.append([]byte(
			processStartTimeSecondsText+
				"# TYPE metric_a counter\n"+
				"metric_a 10\n"+
				"# TYPE metric_b counter\n"+
				"metric_b 1\n"), now); err != nil {
			t.Fatalf("Unexpected append error: %s", err)
		}
		want := []*MetricFamily{
			makeProcessStartTimeMetric(),
			counterFromComponents("metric_a", timestamp.FromTime(now), timestamp.FromTime(now), 10),
			counterFromComponents("metric_b", timestamp.FromTime(now), timestamp.FromTime(now), 1),
		}
		sort.Sort(ByName(want))
		if !reflect.DeepEqual(want, app.Sorted()) {
			t.Fatalf("Appended samples not as expected.\nWanted: %+v\nGot:    %+v", want, app.Sorted())
		}
		app.Reset()
	}
}

func TestTargetScraperScrapeOK(t *testing.T) {
	const (
		configTimeout   = 1500 * time.Millisecond
		expectedTimeout = "1.500000"
	)

	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			accept := r.Header.Get("Accept")
			if !strings.HasPrefix(accept, "text/plain;") {
				t.Errorf("Expected Accept header to prefer text/plain, got %q", accept)
			}

			timeout := r.Header.Get("X-Prometheus-Scrape-Timeout-Seconds")
			if timeout != expectedTimeout {
				t.Errorf("Expected scrape timeout header %q, got %q", expectedTimeout, timeout)
			}

			w.Header().Set("Content-Type", `text/plain; version=0.0.4`)
			w.Write([]byte("metric_a 1\nmetric_b 2\n"))
		}),
	)
	defer server.Close()

	serverURL, err := url.Parse(server.URL)
	if err != nil {
		panic(err)
	}

	ts := &targetScraper{
		Target: &Target{
			labels: labels.FromStrings(
				model.SchemeLabel, serverURL.Scheme,
				model.AddressLabel, serverURL.Host,
			),
		},
		client:  http.DefaultClient,
		timeout: configTimeout,
	}
	var buf bytes.Buffer

	if err := ts.scrape(context.Background(), &buf); err != nil {
		t.Fatalf("Unexpected scrape error: %s", err)
	}
	require.Equal(t, "metric_a 1\nmetric_b 2\n", buf.String())
}

func TestTargetScrapeScrapeCancel(t *testing.T) {
	block := make(chan struct{})

	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			<-block
		}),
	)
	defer server.Close()

	serverURL, err := url.Parse(server.URL)
	if err != nil {
		panic(err)
	}

	ts := &targetScraper{
		Target: &Target{
			labels: labels.FromStrings(
				model.SchemeLabel, serverURL.Scheme,
				model.AddressLabel, serverURL.Host,
			),
		},
		client: http.DefaultClient,
	}
	ctx, cancel := context.WithCancel(context.Background())

	errc := make(chan error)

	go func() {
		time.Sleep(1 * time.Second)
		cancel()
	}()

	go func() {
		if err := ts.scrape(ctx, ioutil.Discard); err != context.Canceled {
			errc <- fmt.Errorf("Expected context cancelation error but got: %s", err)
		}
		close(errc)
	}()

	select {
	case <-time.After(5 * time.Second):
		t.Fatalf("Scrape function did not return unexpectedly")
	case err := <-errc:
		if err != nil {
			t.Fatalf(err.Error())
		}
	}
	// If this is closed in a defer above the function the test server
	// does not terminate and the test doens't complete.
	close(block)
}

func TestTargetScrapeScrapeNotFound(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}),
	)
	defer server.Close()

	serverURL, err := url.Parse(server.URL)
	if err != nil {
		panic(err)
	}

	ts := &targetScraper{
		Target: &Target{
			labels: labels.FromStrings(
				model.SchemeLabel, serverURL.Scheme,
				model.AddressLabel, serverURL.Host,
			),
		},
		client: http.DefaultClient,
	}

	if err := ts.scrape(context.Background(), ioutil.Discard); !strings.Contains(err.Error(), "404") {
		t.Fatalf("Expected \"404 NotFound\" error but got: %s", err)
	}
}

// testScraper implements the scraper interface and allows setting values
// returned by its methods. It also allows setting a custom scrape function.
type testScraper struct {
	offsetDur time.Duration

	lastStart    time.Time
	lastDuration time.Duration
	lastError    error

	scrapeErr  error
	scrapeFunc func(context.Context, io.Writer) error
}

func (ts *testScraper) offset(interval time.Duration) time.Duration {
	return ts.offsetDur
}

func (ts *testScraper) report(start time.Time, duration time.Duration, err error) {
	ts.lastStart = start
	ts.lastDuration = duration
	ts.lastError = err
}

func (ts *testScraper) scrape(ctx context.Context, w io.Writer) error {
	if ts.scrapeFunc != nil {
		return ts.scrapeFunc(ctx, w)
	}
	return ts.scrapeErr
}

// untypedFromTriplet is a helper to adapt Prometheus unit tests to the new API based on MetricFamily.
func untypedFromTriplet(name string, t int64, v float64) *MetricFamily {
	return &MetricFamily{
		MetricFamily: &dto.MetricFamily{
			Name: proto.String(name),
			Type: dto.MetricType_UNTYPED.Enum(),
			Metric: []*dto.Metric{
				&dto.Metric{
					Untyped: &dto.Untyped{
						Value: proto.Float64(v),
					},
					TimestampMs: proto.Int64(t),
				},
			},
		},
		MetricResetTimestampMs: []int64{
			NoTimestamp,
		},
	}
}

func counterFromComponents(name string, t int64, reset int64, v float64) *MetricFamily {
	return &MetricFamily{
		MetricFamily: &dto.MetricFamily{
			Name: proto.String(name),
			Type: dto.MetricType_COUNTER.Enum(),
			Metric: []*dto.Metric{
				&dto.Metric{
					Counter: &dto.Counter{
						Value: proto.Float64(v),
					},
					TimestampMs: proto.Int64(t),
				},
			},
		},
		MetricResetTimestampMs: []int64{
			reset,
		},
	}
}

func histogramFromComponents(name string, t int64, reset int64, c1, c2 uint64, sum float64) *MetricFamily {
	return &MetricFamily{
		MetricFamily: &dto.MetricFamily{
			Name: proto.String(name),
			Type: dto.MetricType_HISTOGRAM.Enum(),
			Metric: []*dto.Metric{
				{
					Histogram: &dto.Histogram{
						SampleCount: proto.Uint64(c2),
						SampleSum:   proto.Float64(sum),
						Bucket: []*dto.Bucket{
							{
								CumulativeCount: proto.Uint64(c1),
								UpperBound:      proto.Float64(100),
							},
							{
								CumulativeCount: proto.Uint64(c2),
								UpperBound:      proto.Float64(math.Inf(1)),
							},
						},
					},
					TimestampMs: proto.Int64(t),
				},
			},
		},
		MetricResetTimestampMs: []int64{
			reset,
		},
	}
}

func addMetricLabels(metricFamily *MetricFamily, labelName, labelValue string) *MetricFamily {
	for _, metric := range metricFamily.Metric {
		metric.Label = append(metric.Label, &dto.LabelPair{
			Name:  proto.String(labelName),
			Value: proto.String(labelValue),
		})
	}
	return metricFamily
}
