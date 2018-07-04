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

	"github.com/Stackdriver/stackdriver-prometheus/relabel"
	"github.com/go-kit/kit/log"
	"github.com/gogo/protobuf/proto"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/stretchr/testify/require"
)

const (
	scrapeTimeout   = model.Duration(1500 * time.Millisecond)
	expectedTimeout = "1.500000"
)

var (
	instanceLabelPair = &dto.LabelPair{Name: proto.String(model.InstanceLabel), Value: proto.String("i123")}
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

func (m *simpleResetPointMap) Clear() {
	m.m = map[ResetPointKey]Point{}
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

type functorAppendable struct {
	f func(metricFamily *MetricFamily)
}

func (a functorAppendable) Appender() (Appender, error) {
	return functorAppender{f: a.f}, nil
}

type functorAppender struct {
	f func(metricFamily *MetricFamily)
}

func (a functorAppender) Add(metricFamily *MetricFamily) error {
	a.f(metricFamily)
	return nil
}

func TestScrapePoolSync(t *testing.T) {
	server, serverURL := newServer(t)
	defer server.Close()

	cfg := &config.ScrapeConfig{
		ScrapeInterval: model.Duration(3 * time.Second),
		ScrapeTimeout:  scrapeTimeout,
		MetricRelabelConfigs: []*config.RelabelConfig{
			{
				Action:       config.RelabelReplace,
				Regex:        mustNewRegexp("(.*)"),
				SourceLabels: model.LabelNames{"my_key"},
				Replacement:  "${1}_copy",
				TargetLabel:  "my_key_copy",
			},
			// TODO(jkohen): introduce a test file for relabel.go and move this more complex test case there.
			{
				Action:      config.RelabelLabelMap,
				Regex:       mustNewRegexp("(.*)"),
				Replacement: "${1}_clone",
			},
		},
	}
	app := &functorAppendable{}
	output := &bytes.Buffer{}
	sp := newScrapePool(cfg, app, log.NewLogfmtLogger(output))
	var once sync.Once
	var wg sync.WaitGroup
	var result *MetricFamily
	wg.Add(1)
	app.f = func(metricFamily *MetricFamily) {
		once.Do(func() {
			result = metricFamily
			wg.Done()
		})
	}
	targetGroup := []*targetgroup.Group{
		{
			Targets: []model.LabelSet{
				{
					model.SchemeLabel:  model.LabelValue(serverURL.Scheme),
					model.AddressLabel: model.LabelValue(serverURL.Host),
				},
			},
			Labels: model.LabelSet{"my_key": "my_value"},
			Source: "test",
		},
	}
	// Blocks until stop() is called on the scrapePool.
	sp.Sync(targetGroup)
	wg.Wait()
	sp.stop()
	if output.Len() > 0 {
		t.Errorf("succeeded with messages %v", output.String())
	}
	want := []*dto.LabelPair{
		{Name: proto.String(model.InstanceLabel), Value: proto.String(serverURL.Host)},
		{Name: proto.String("my_key"), Value: proto.String("my_value")},
		{Name: proto.String("my_key_copy"), Value: proto.String("my_value_copy")},
	}
	for _, pair := range want {
		want = append(want, &dto.LabelPair{
			Name:  proto.String(*pair.Name + "_clone"),
			Value: proto.String(*pair.Value),
		})
	}
	sort.Sort(LabelPairsByName(want))
	got := result.GetMetric()[0].GetLabel()
	sort.Sort(LabelPairsByName(got))
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("Appended samples not as expected.\nWanted: %+v\nGot:    %+v", want, got)
	}
}

func TestScrapeLoopStopBeforeRun(t *testing.T) {
	scraper := &testScraper{}

	sl := newScrapeLoop(context.Background(),
		scraper, &fakeResetPointMap{},
		nil, nil, labels.Labels{},
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

func nopMutator(l relabel.LabelPairs) relabel.LabelPairs {
	// Stackdriver requires the instance label.
	return append(l, instanceLabelPair)
}

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
		nil, nil, labels.Labels{},
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
	var upValues []float64
	for i, family := range appender.result {
		for _, metric := range family.Metric {
			if i%5 == 0 {
				ts = *metric.TimestampMs
			} else if *metric.TimestampMs != ts {
				t.Errorf("Unexpected multiple timestamps within single scrape")
			}
			if family.GetName() == "up" {
				upValue := metric.Gauge.GetValue()
				if upValue != 1 {
					t.Errorf("bad 'up' value; want 1, got %v", upValue)
				}
				upValues = append(upValues, upValue)
			}
		}
	}
	if len(upValues) != 2 {
		t.Errorf("missing 'up' metrics; expected %d, got %d; MetricFamily is %v",
			numScrapes, len(upValues), appender.result)
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
		nil, nil, labels.Labels{},
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
		nil, nil, labels.Labels{},
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

	targetLabels := labels.Labels{{Name: "foo", Value: "bar"}}
	sl := newScrapeLoop(context.Background(),
		nil, &fakeResetPointMap{},
		nil, nil, targetLabels,
		nopMutator,
		nopMutator,
		func() Appender { return app },
	)

	now := time.Now()
	_, _, err := sl.append([]byte(
		"metric_a 1\n"+
			"metric_b NaN\n"), now)
	if err != nil {
		t.Fatalf("Unexpected append error: %s", err)
	}
	result := app.Sorted()
	if len(result) != 2 || len(result[1].Metric) < 1 {
		t.Fatalf("expected 2 metrics; got %v", result)
	}
	// DeepEqual will report NaNs as being different, so replace with a different value.
	result[1].Metric[0].Untyped.Value = proto.Float64(42)
	want := []*MetricFamily{
		untypedFromTriplet("metric_a", timestamp.FromTime(now), 1),
		untypedFromTriplet("metric_b", timestamp.FromTime(now), 42),
	}
	for i := range want {
		want[i].TargetLabels = targetLabels
	}
	sort.Sort(ByName(want))
	if !reflect.DeepEqual(want, result) {
		t.Fatalf("Appended samples not as expected.\nWanted: %+v\nGot:    %+v", want, result)
	}
}

func TestScrapeLoopMutator(t *testing.T) {
	app := &collectResultAppender{}

	sl := newScrapeLoop(context.Background(),
		nil, &fakeResetPointMap{},
		nil, nil, labels.Labels{},
		func(input relabel.LabelPairs) relabel.LabelPairs {
			output := nopMutator(relabel.LabelPairs{})
			for _, label := range input {
				output = append(output, &dto.LabelPair{Name: label.Name, Value: label.Value})
			}
			return append(output, &dto.LabelPair{Name: proto.String("new_label"), Value: proto.String("foo")})
		},
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
	result := app.Sorted()
	want := []*MetricFamily{
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
		nil, nil, labels.Labels{},
		func(lset relabel.LabelPairs) relabel.LabelPairs {
			output := nopMutator(relabel.LabelPairs{})
			for _, label := range lset {
				if *label.Name == "delete" {
					if *label.Value == "label" {
						// delete this label
						continue
					} else if *label.Value == "metric" {
						// delete whole metric
						return nil
					}
				}
				output = append(output, &dto.LabelPair{Name: label.Name, Value: label.Value})
			}
			return output
		},
		nopMutator,
		func() Appender { return app },
	)

	now := time.Now()
	_, _, err := sl.append([]byte(
		"metric_a{delete=\"metric\"} 1\n"+
			"metric_b{keep=\"x\"} 2\n"+
			"metric_b{delete=\"label\",keep=\"y\"} 3\n"), now)
	if err != nil {
		t.Fatalf("Unexpected append error: %s", err)
	}
	// DeepEqual will report NaNs as being different, so replace with a different value.
	result := app.Sorted()
	want := []*MetricFamily{
		mustMetricFamily(NewMetricFamily(
			&dto.MetricFamily{
				Name: proto.String("metric_b"),
				Type: dto.MetricType_UNTYPED.Enum(),
				Metric: []*dto.Metric{
					&dto.Metric{
						Untyped: &dto.Untyped{
							Value: proto.Float64(2),
						},
						TimestampMs: proto.Int64(timestamp.FromTime(now)),
						Label: []*dto.LabelPair{
							instanceLabelPair,
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
							instanceLabelPair,
							{
								Name:  proto.String("keep"),
								Value: proto.String("y"),
							},
						},
					},
				},
			},
			[]int64{
				NoTimestamp,
				NoTimestamp,
			},
			labels.Labels{})),
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
		nil, nil, labels.Labels{},
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
		"metric_a 1\n"+
			"metric_b 1\n"+
			"metric_c 1\n"), now)
	if err != errSampleLimit {
		t.Errorf("Did not see expected sample limit error: %v", err)
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
		t.Errorf("Unexpected change of sample limit metric: %f", (value - beforeMetricValue))
	}

	// And verify that we got the samples that fit under the limit. We
	// cannot do a deep comparison, because the order of the metrics in the
	// input is undefined.
	if len(resApp.result) != 1 {
		t.Errorf("Appended samples not as expected. Wanted 1 sample, got: %+v", resApp.result)
	}
}

func TestScrapeLoopAppendNoStalenessIfTimestamp(t *testing.T) {
	app := &collectResultAppender{}
	sl := newScrapeLoop(context.Background(),
		nil, &fakeResetPointMap{},
		nil, nil, labels.Labels{},
		nopMutator,
		nopMutator,
		func() Appender { return app },
	)

	now := time.Now()
	_, _, err := sl.append([]byte("metric_a 1 1000\n"), now)
	if err != nil {
		t.Fatalf("Unexpected append error: %s", err)
	}
	_, _, err = sl.append([]byte(""), now.Add(time.Second))
	if err != nil {
		t.Fatalf("Unexpected append error: %s", err)
	}

	want := []*MetricFamily{
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
		nil, nil, labels.Labels{},
		nopMutator,
		nopMutator,
		app,
	)

	scraper.scrapeFunc = func(ctx context.Context, w io.Writer) error {
		cancel()
		return fmt.Errorf("scrape failed")
	}

	sl.run(10*time.Millisecond, time.Hour, nil)

	if appender.result != nil {
		t.Fatalf("unexpected 'up' value; got %v", appender.result)
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
		nil, nil, labels.Labels{},
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

	if appender.result != nil {
		t.Fatalf("unexpected 'up' value; got %v", appender.result)
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
		nil, nil, labels.Labels{},
		nopMutator,
		nopMutator,
		func() Appender { return app },
	)

	now := time.Unix(1, 0)
	_, _, err := sl.append([]byte(
		"out_of_order 1\n"+
			"amend 1\n"+
			"normal 1\n"+
			"out_of_bounds 1\n"), now)
	if err != nil {
		t.Fatalf("Unexpected append error: %s", err)
	}
	want := []*MetricFamily{
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
		nil, nil, labels.Labels{},
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
	total, added, err := sl.append([]byte("normal 1\n"), now)
	if total != 1 {
		t.Error("expected 1 metric")
		return
	}

	if added != 0 {
		t.Errorf("unexpected metrics")
	}

	if err != nil {
		t.Errorf("expect no error, got %s", err.Error())
	}
}

func TestEmptySum(t *testing.T) {
	app := &collectResultAppender{}

	resetPointMap := newSimpleResetPointMap()
	sl := newScrapeLoop(context.Background(),
		nil, &resetPointMap,
		nil, nil, labels.Labels{},
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
				"metric_h_sum 123.1\n"+
				"# TYPE metric_s summary\n"+
				"metric_s{quantile=\"1\"} 1\n"+
				"metric_s_count 1\n"), now); err != nil {
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
				"metric_h_sum 223.1\n"+
				"# TYPE metric_s summary\n"+
				"metric_s{quantile=\"1\"} 1\n"+
				"metric_s_count 7\n"), now); err != nil {
			t.Fatalf("Unexpected append error: %s", err)
		}
		resetTime := now.Add(-1 * time.Millisecond)
		want := []*MetricFamily{
			counterFromComponents("metric_a", timestamp.FromTime(now), timestamp.FromTime(existingReset), 10),
			counterFromComponents("metric_b", timestamp.FromTime(now), timestamp.FromTime(resetTime), 11),
			histogramFromComponents("metric_h", timestamp.FromTime(now), timestamp.FromTime(existingReset), 10, 20, 100),
			summaryFromComponentsNoSum("metric_s", timestamp.FromTime(now), timestamp.FromTime(existingReset), 6),
		}
		sort.Sort(ByName(want))
		if !reflect.DeepEqual(want, app.Sorted()) {
			t.Fatalf("Appended samples not as expected.\nWanted: %+v\nGot:    %+v", want, app.Sorted())
		}
		app.Reset()
	}
}

func TestPointExtractor(t *testing.T) {
	app := &collectResultAppender{}

	resetPointMap := newSimpleResetPointMap()
	sl := newScrapeLoop(context.Background(),
		nil, &resetPointMap,
		nil, nil, labels.Labels{},
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
				"metric_h_sum 123.1\n"+
				"# TYPE metric_s summary\n"+
				"metric_s{quantile=\"1\"} 1\n"+
				"metric_s_count 1\n"+
				"metric_s_sum 13.4\n"), now); err != nil {
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
				"metric_h_sum 223.1\n"+
				"# TYPE metric_s summary\n"+
				"metric_s{quantile=\"1\"} 1\n"+
				"metric_s_count 7\n"+
				"metric_s_sum 213.4\n"), now); err != nil {
			t.Fatalf("Unexpected append error: %s", err)
		}
		resetTime := now.Add(-1 * time.Millisecond)
		want := []*MetricFamily{
			counterFromComponents("metric_a", timestamp.FromTime(now), timestamp.FromTime(existingReset), 10),
			counterFromComponents("metric_b", timestamp.FromTime(now), timestamp.FromTime(resetTime), 11),
			histogramFromComponents("metric_h", timestamp.FromTime(now), timestamp.FromTime(existingReset), 10, 20, 100),
			summaryFromComponents("metric_s", timestamp.FromTime(now), timestamp.FromTime(existingReset), 6, 200.0),
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
				"metric_h_sum 23.1\n"+
				"# TYPE metric_s summary\n"+
				"metric_s{quantile=\"1\"} 1\n"+
				"metric_s_count 3\n"+
				"metric_s_sum 13.4\n"), now); err != nil {
			t.Fatalf("Unexpected append error: %s", err)
		}
		resetTime := now.Add(-1 * time.Millisecond)
		want := []*MetricFamily{
			counterFromComponents("metric_a", timestamp.FromTime(now), timestamp.FromTime(resetTime), 10),
			counterFromComponents("metric_b", timestamp.FromTime(now), timestamp.FromTime(resetTime), 1),
			histogramFromComponents("metric_h", timestamp.FromTime(now), timestamp.FromTime(resetTime), 1, 10, 23.1),
			summaryFromComponents("metric_s", timestamp.FromTime(now), timestamp.FromTime(resetTime), 3, 13.4),
		}
		sort.Sort(ByName(want))
		if !reflect.DeepEqual(want, app.Sorted()) {
			t.Fatalf("Appended samples not as expected.\nWanted: %+v\nGot:    %+v", want, app.Sorted())
		}
		app.Reset()
	}
}

func TestTargetScraperScrapeOK(t *testing.T) {
	const configTimeout = 1500 * time.Millisecond

	server, serverURL := newServer(t)
	defer server.Close()

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
	return mustMetricFamily(NewMetricFamily(
		&dto.MetricFamily{
			Name: proto.String(name),
			Type: dto.MetricType_UNTYPED.Enum(),
			Metric: []*dto.Metric{
				&dto.Metric{
					Untyped: &dto.Untyped{
						Value: proto.Float64(v),
					},
					TimestampMs: proto.Int64(t),
					Label:       []*dto.LabelPair{instanceLabelPair},
				},
			},
		},
		[]int64{
			NoTimestamp,
		},
		labels.Labels{}))
}

func counterFromComponents(name string, t int64, reset int64, v float64) *MetricFamily {
	return mustMetricFamily(NewMetricFamily(
		&dto.MetricFamily{
			Name: proto.String(name),
			Type: dto.MetricType_COUNTER.Enum(),
			Metric: []*dto.Metric{
				&dto.Metric{
					Counter: &dto.Counter{
						Value: proto.Float64(v),
					},
					TimestampMs: proto.Int64(t),
					Label:       []*dto.LabelPair{instanceLabelPair},
				},
			},
		},
		[]int64{
			reset,
		},
		labels.Labels{}))
}

func histogramFromComponents(name string, t int64, reset int64, c1, c2 uint64, sum float64) *MetricFamily {
	return mustMetricFamily(NewMetricFamily(
		&dto.MetricFamily{
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
					Label:       []*dto.LabelPair{instanceLabelPair},
				},
			},
		},
		[]int64{
			reset,
		},
		labels.Labels{}))
}

func summaryFromComponents(name string, t int64, reset int64, count uint64, sum float64) *MetricFamily {
	return mustMetricFamily(NewMetricFamily(
		&dto.MetricFamily{
			Name: proto.String(name),
			Type: dto.MetricType_SUMMARY.Enum(),
			Metric: []*dto.Metric{
				{
					Summary: &dto.Summary{
						SampleCount: proto.Uint64(count),
						SampleSum:   proto.Float64(sum),
						Quantile: []*dto.Quantile{
							{
								Quantile: proto.Float64(1),
								Value:    proto.Float64(1),
							},
						},
					},
					TimestampMs: proto.Int64(t),
					Label:       []*dto.LabelPair{instanceLabelPair},
				},
			},
		},
		[]int64{
			reset,
		},
		labels.Labels{}))
}

func summaryFromComponentsNoSum(name string, t int64, reset int64, count uint64) *MetricFamily {
	return mustMetricFamily(NewMetricFamily(
		&dto.MetricFamily{
			Name: proto.String(name),
			Type: dto.MetricType_SUMMARY.Enum(),
			Metric: []*dto.Metric{
				{
					Summary: &dto.Summary{
						SampleCount: proto.Uint64(count),
						Quantile: []*dto.Quantile{
							{
								Quantile: proto.Float64(1),
								Value:    proto.Float64(1),
							},
						},
					},
					TimestampMs: proto.Int64(t),
					Label:       []*dto.LabelPair{instanceLabelPair},
				},
			},
		},
		[]int64{
			reset,
		},
		labels.Labels{}))
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

func newServer(t *testing.T) (*httptest.Server, *url.URL) {
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

	serverURL, err := url.Parse(server.URL)
	if err != nil {
		panic(err)
	}
	return server, serverURL
}

func TestExtractTargetLabels(t *testing.T) {
	setUp := func() (metricLabels relabel.LabelPairs, targetLabels map[string]*labels.Label) {
		targetLabels = map[string]*labels.Label{}
		targetLabels["target_label"] = &labels.Label{Name: "target_label", Value: "target_value1"}
		targetLabels["common_label"] = &labels.Label{Name: "common_label", Value: "target_value2"}
		targetLabels["empty_target_label"] = &labels.Label{Name: "empty_target_label", Value: ""}
		metricLabels = relabel.LabelPairs{
			{
				Name:  proto.String("metric_label"),
				Value: proto.String("metric_value1"),
			},
			{
				Name:  proto.String("common_label"),
				Value: proto.String("metric_value2"),
			},
			{
				Name:  proto.String("empty_metric_label"),
				Value: proto.String(""),
			},
		}
		return
	}
	mrc := []*config.RelabelConfig{
		{
			Action:       config.RelabelReplace,
			Regex:        config.MustNewRegexp("(.*)"),
			SourceLabels: model.LabelNames{"target_label"},
			Replacement:  "machine",
			TargetLabel:  "target_label",
		},
	}
	t.Run("honorLabels=false", func(t *testing.T) {
		metricLabels, targetLabels := setUp()
		result := extractTargetLabels(metricLabels, targetLabels, false /*honorLabels*/)
		expectedLabels := relabel.LabelPairs{
			{
				Name:  proto.String("metric_label"),
				Value: proto.String("metric_value1"),
			},
			{
				Name:  proto.String("exported_common_label"),
				Value: proto.String("metric_value2"),
			},
			{
				Name:  proto.String("target_label"),
				Value: proto.String("target_value1"),
			},
			{
				Name:  proto.String("common_label"),
				Value: proto.String("target_value2"),
			},
		}
		sort.Sort(LabelPairsByName(expectedLabels))
		sort.Sort(LabelPairsByName(result))
		if !reflect.DeepEqual(expectedLabels, result) {
			t.Fatalf("labels not as expected.\nWanted: %+v\nGot:    %+v", expectedLabels, result)
		}
		result = relabel.Process(result, mrc...)
		_, expectedTargetLabels := setUp()
		if !reflect.DeepEqual(expectedTargetLabels, targetLabels) {
			t.Fatalf("labels not as expected.\nWanted: %+v\nGot:    %+v", expectedTargetLabels, targetLabels)
		}
	})
	t.Run("honorLabels=true", func(t *testing.T) {
		metricLabels, targetLabels := setUp()
		result := extractTargetLabels(metricLabels, targetLabels, true /*honorLabels*/)
		expectedLabels := relabel.LabelPairs{
			{
				Name:  proto.String("metric_label"),
				Value: proto.String("metric_value1"),
			},
			{
				Name:  proto.String("common_label"),
				Value: proto.String("metric_value2"),
			},
			{
				Name:  proto.String("target_label"),
				Value: proto.String("target_value1"),
			},
		}
		sort.Sort(LabelPairsByName(expectedLabels))
		sort.Sort(LabelPairsByName(result))
		if !reflect.DeepEqual(expectedLabels, result) {
			t.Fatalf("labels not as expected.\nWanted: %+v\nGot:    %+v", expectedLabels, result)
		}
		result = relabel.Process(result, mrc...)
		_, expectedTargetLabels := setUp()
		if !reflect.DeepEqual(expectedTargetLabels, targetLabels) {
			t.Fatalf("labels not as expected.\nWanted: %+v\nGot:    %+v", expectedTargetLabels, targetLabels)
		}
	})
}

func BenchmarkExtractTargetLabels(b *testing.B) {
	b.ReportAllocs()
	targetLabels := map[string]*labels.Label{}
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("target_label%d", i)
		value := fmt.Sprintf("%d:foo oaeu aoeu aoeu aoeu ou", i)
		targetLabels[key] = &labels.Label{Name: key, Value: value}
	}
	metricLabels := relabel.LabelPairs{
		{
			Name:  proto.String("metric_label1"),
			Value: proto.String("metric_value1"),
		},
		{
			Name:  proto.String("metric_label2"),
			Value: proto.String("metric_value2"),
		},
	}

	for i := 0; i < b.N; i++ {
		extractTargetLabels(metricLabels, targetLabels, false /*honorLabels*/)
	}
}
