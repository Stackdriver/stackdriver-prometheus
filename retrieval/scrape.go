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
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/version"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/pool"
	"github.com/prometheus/prometheus/pkg/relabel"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/util/httputil"
	"golang.org/x/net/context/ctxhttp"
)

var (
	targetIntervalLength = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "prometheus_target_interval_length_seconds",
			Help:       "Actual intervals between scrapes.",
			Objectives: map[float64]float64{0.01: 0.001, 0.05: 0.005, 0.5: 0.05, 0.90: 0.01, 0.99: 0.001},
		},
		[]string{"interval"},
	)
	targetReloadIntervalLength = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "prometheus_target_reload_length_seconds",
			Help:       "Actual interval to reload the scrape pool with a given configuration.",
			Objectives: map[float64]float64{0.01: 0.001, 0.05: 0.005, 0.5: 0.05, 0.90: 0.01, 0.99: 0.001},
		},
		[]string{"interval"},
	)
	targetSyncIntervalLength = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "prometheus_target_sync_length_seconds",
			Help:       "Actual interval to sync the scrape pool.",
			Objectives: map[float64]float64{0.01: 0.001, 0.05: 0.005, 0.5: 0.05, 0.90: 0.01, 0.99: 0.001},
		},
		[]string{"scrape_job"},
	)
	targetScrapePoolSyncsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrape_pool_sync_total",
			Help: "Total number of syncs that were executed on a scrape pool.",
		},
		[]string{"scrape_job"},
	)
	targetScrapeSampleLimit = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrapes_exceeded_sample_limit_total",
			Help: "Total number of scrapes that hit the sample limit and were rejected.",
		},
	)
	targetScrapeSampleDuplicate = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrapes_sample_duplicate_timestamp_total",
			Help: "Total number of samples rejected due to duplicate timestamps but different values",
		},
	)
	targetScrapeSampleOutOfOrder = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrapes_sample_out_of_order_total",
			Help: "Total number of samples rejected due to not being out of the expected order",
		},
	)
	targetScrapeSampleOutOfBounds = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrapes_sample_out_of_bounds_total",
			Help: "Total number of samples rejected due to timestamp falling outside of the time bounds",
		},
	)
)

func init() {
	prometheus.MustRegister(targetIntervalLength)
	prometheus.MustRegister(targetReloadIntervalLength)
	prometheus.MustRegister(targetSyncIntervalLength)
	prometheus.MustRegister(targetScrapePoolSyncsCounter)
	prometheus.MustRegister(targetScrapeSampleLimit)
	prometheus.MustRegister(targetScrapeSampleDuplicate)
	prometheus.MustRegister(targetScrapeSampleOutOfOrder)
	prometheus.MustRegister(targetScrapeSampleOutOfBounds)
}

// scrapePool manages scrapes for sets of targets.
type scrapePool struct {
	appendable Appendable
	logger     log.Logger

	mtx    sync.RWMutex
	config *config.ScrapeConfig
	client *http.Client
	// Targets and loops must always be synchronized to have the same
	// set of hashes.
	targets        map[uint64]*Target
	droppedTargets []*Target
	loops          map[uint64]loop
	cancel         context.CancelFunc

	// Constructor for new scrape loops. This is settable for testing convenience.
	newLoop func(*Target, scraper) loop
}

const (
	maxAheadTime = 10 * time.Minute
)

type labelsMutator func(labels.Labels) labels.Labels

func newScrapePool(cfg *config.ScrapeConfig, app Appendable, logger log.Logger) *scrapePool {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	client, err := httputil.NewClientFromConfig(cfg.HTTPClientConfig, cfg.JobName)
	if err != nil {
		// Any errors that could occur here should be caught during config validation.
		level.Error(logger).Log("msg", "Error creating HTTP client", "err", err)
	}

	buffers := pool.NewBytesPool(163, 100e6, 3)

	ctx, cancel := context.WithCancel(context.Background())
	sp := &scrapePool{
		cancel:     cancel,
		appendable: app,
		config:     cfg,
		client:     client,
		targets:    map[uint64]*Target{},
		loops:      map[uint64]loop{},
		logger:     logger,
	}
	sp.newLoop = func(t *Target, s scraper) loop {
		return newScrapeLoop(
			ctx,
			s,
			t,
			log.With(logger, "target", t),
			buffers,
			func(l labels.Labels) labels.Labels { return sp.mutateSampleLabels(l, t) },
			func(l labels.Labels) labels.Labels { return sp.mutateReportSampleLabels(l, t) },
			sp.appender,
		)
	}

	return sp
}

// stop terminates all scrape loops and returns after they all terminated.
func (sp *scrapePool) stop() {
	sp.cancel()
	var wg sync.WaitGroup

	sp.mtx.Lock()
	defer sp.mtx.Unlock()

	for fp, l := range sp.loops {
		wg.Add(1)

		go func(l loop) {
			l.stop()
			wg.Done()
		}(l)

		delete(sp.loops, fp)
		delete(sp.targets, fp)
	}
	wg.Wait()
}

// reload the scrape pool with the given scrape configuration. The target state is preserved
// but all scrape loops are restarted with the new scrape configuration.
// This method returns after all scrape loops that were stopped have stopped scraping.
func (sp *scrapePool) reload(cfg *config.ScrapeConfig) {
	start := time.Now()

	sp.mtx.Lock()
	defer sp.mtx.Unlock()

	client, err := httputil.NewClientFromConfig(cfg.HTTPClientConfig, cfg.JobName)
	if err != nil {
		// Any errors that could occur here should be caught during config validation.
		level.Error(sp.logger).Log("msg", "Error creating HTTP client", "err", err)
	}
	sp.config = cfg
	sp.client = client

	var (
		wg       sync.WaitGroup
		interval = time.Duration(sp.config.ScrapeInterval)
		timeout  = time.Duration(sp.config.ScrapeTimeout)
	)

	for fp, oldLoop := range sp.loops {
		var (
			t       = sp.targets[fp]
			s       = &targetScraper{Target: t, client: sp.client, timeout: timeout}
			newLoop = sp.newLoop(t, s)
		)
		wg.Add(1)

		go func(oldLoop, newLoop loop) {
			oldLoop.stop()
			wg.Done()

			go newLoop.run(interval, timeout, nil)
		}(oldLoop, newLoop)

		sp.loops[fp] = newLoop
	}

	wg.Wait()
	targetReloadIntervalLength.WithLabelValues(interval.String()).Observe(
		time.Since(start).Seconds(),
	)
}

// Sync converts target groups into actual scrape targets and synchronizes
// the currently running scraper with the resulting set.
func (sp *scrapePool) Sync(tgs []*targetgroup.Group) {
	start := time.Now()

	var all []*Target
	sp.mtx.Lock()
	sp.droppedTargets = []*Target{}
	for _, tg := range tgs {
		targets, err := targetsFromGroup(tg, sp.config)
		if err != nil {
			level.Error(sp.logger).Log("msg", "creating targets failed", "err", err)
			continue
		}
		for _, t := range targets {
			if t.Labels().Len() > 0 {
				all = append(all, t)
			} else if t.DiscoveredLabels().Len() > 0 {
				sp.droppedTargets = append(sp.droppedTargets, t)
			}
		}
	}
	sp.mtx.Unlock()
	sp.sync(all)

	targetSyncIntervalLength.WithLabelValues(sp.config.JobName).Observe(
		time.Since(start).Seconds(),
	)
	targetScrapePoolSyncsCounter.WithLabelValues(sp.config.JobName).Inc()
}

// sync takes a list of potentially duplicated targets, deduplicates them, starts
// scrape loops for new targets, and stops scrape loops for disappeared targets.
// It returns after all stopped scrape loops terminated.
func (sp *scrapePool) sync(targets []*Target) {
	sp.mtx.Lock()
	defer sp.mtx.Unlock()

	var (
		uniqueTargets = map[uint64]struct{}{}
		interval      = time.Duration(sp.config.ScrapeInterval)
		timeout       = time.Duration(sp.config.ScrapeTimeout)
	)

	for _, t := range targets {
		t := t
		hash := t.hash()
		uniqueTargets[hash] = struct{}{}

		if _, ok := sp.targets[hash]; !ok {
			s := &targetScraper{Target: t, client: sp.client, timeout: timeout}
			l := sp.newLoop(t, s)

			sp.targets[hash] = t
			sp.loops[hash] = l

			go l.run(interval, timeout, nil)
		}
	}

	var wg sync.WaitGroup

	// Stop and remove old targets and scraper loops.
	for hash := range sp.targets {
		if _, ok := uniqueTargets[hash]; !ok {
			wg.Add(1)
			go func(l loop) {
				l.stop()
				wg.Done()
			}(sp.loops[hash])

			delete(sp.loops, hash)
			delete(sp.targets, hash)
		}
	}

	// Wait for all potentially stopped scrapers to terminate.
	// This covers the case of flapping targets. If the server is under high load, a new scraper
	// may be active and tries to insert. The old scraper that didn't terminate yet could still
	// be inserting a previous sample set.
	wg.Wait()
}

func (sp *scrapePool) mutateSampleLabels(lset labels.Labels, target *Target) labels.Labels {
	lb := labels.NewBuilder(lset)

	if sp.config.HonorLabels {
		for _, l := range target.Labels() {
			if lv := lset.Get(l.Name); lv == "" {
				lb.Set(l.Name, l.Value)
			}
		}
	} else {
		for _, l := range target.Labels() {
			lv := lset.Get(l.Name)
			if lv != "" {
				lb.Set(model.ExportedLabelPrefix+l.Name, lv)
			}
			lb.Set(l.Name, l.Value)
		}
	}

	res := lb.Labels()

	if mrc := sp.config.MetricRelabelConfigs; len(mrc) > 0 {
		res = relabel.Process(res, mrc...)
	}

	return res
}

func (sp *scrapePool) mutateReportSampleLabels(lset labels.Labels, target *Target) labels.Labels {
	lb := labels.NewBuilder(lset)

	for _, l := range target.Labels() {
		lv := lset.Get(l.Name)
		if lv != "" {
			lb.Set(model.ExportedLabelPrefix+l.Name, lv)
		}
		lb.Set(l.Name, l.Value)
	}

	return lb.Labels()
}

// appender returns an appender for ingested samples from the target.
func (sp *scrapePool) appender() Appender {
	app, err := sp.appendable.Appender()
	if err != nil {
		panic(err)
	}

	app = &timeLimitAppender{
		Appender: app,
		maxTime:  timestamp.FromTime(time.Now().Add(maxAheadTime)),
	}

	// The limit is applied after metrics are potentially dropped via relabeling.
	if sp.config.SampleLimit > 0 {
		app = &limitAppender{
			Appender: app,
			limit:    int(sp.config.SampleLimit),
		}
	}
	return app
}

// A scraper retrieves samples and accepts a status report at the end.
type scraper interface {
	scrape(ctx context.Context, w io.Writer) error
	report(start time.Time, dur time.Duration, err error)
	offset(interval time.Duration) time.Duration
}

// targetScraper implements the scraper interface for a target.
type targetScraper struct {
	*Target

	client  *http.Client
	req     *http.Request
	timeout time.Duration

	gzipr *gzip.Reader
	buf   *bufio.Reader
}

const acceptHeader = `text/plain;version=0.0.4;q=1,*/*;q=0.1`

var userAgentHeader = fmt.Sprintf("Prometheus/%s", version.Version)

func (s *targetScraper) scrape(ctx context.Context, w io.Writer) error {
	if s.req == nil {
		req, err := http.NewRequest("GET", s.URL().String(), nil)
		if err != nil {
			return err
		}
		req.Header.Add("Accept", acceptHeader)
		req.Header.Add("Accept-Encoding", "gzip")
		req.Header.Set("User-Agent", userAgentHeader)
		req.Header.Set("X-Prometheus-Scrape-Timeout-Seconds", fmt.Sprintf("%f", s.timeout.Seconds()))

		s.req = req
	}

	resp, err := ctxhttp.Do(ctx, s.client, s.req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned HTTP status %s", resp.Status)
	}

	if resp.Header.Get("Content-Encoding") != "gzip" {
		_, err = io.Copy(w, resp.Body)
		return err
	}

	if s.gzipr == nil {
		s.buf = bufio.NewReader(resp.Body)
		s.gzipr, err = gzip.NewReader(s.buf)
		if err != nil {
			return err
		}
	} else {
		s.buf.Reset(resp.Body)
		s.gzipr.Reset(s.buf)
	}

	_, err = io.Copy(w, s.gzipr)
	s.gzipr.Close()
	return err
}

// A loop can run and be stopped again. It must not be reused after it was stopped.
type loop interface {
	run(interval, timeout time.Duration, errc chan<- error)
	stop()
}

type scrapeLoop struct {
	scraper        scraper
	resetPointMap  resetPointMapper
	l              log.Logger
	lastScrapeSize int
	buffers        *pool.BytesPool

	appender            func() Appender
	sampleMutator       labelsMutator
	reportSampleMutator labelsMutator

	ctx       context.Context
	scrapeCtx context.Context
	cancel    func()
	stopped   chan struct{}
}

func newScrapeLoop(ctx context.Context,
	sc scraper,
	resetPointMap resetPointMapper,
	l log.Logger,
	buffers *pool.BytesPool,
	sampleMutator labelsMutator,
	reportSampleMutator labelsMutator,
	appender func() Appender,
) *scrapeLoop {
	if l == nil {
		l = log.NewNopLogger()
	}
	if buffers == nil {
		buffers = pool.NewBytesPool(1e3, 1e6, 3)
	}
	sl := &scrapeLoop{
		scraper:             sc,
		resetPointMap:       resetPointMap,
		buffers:             buffers,
		appender:            appender,
		sampleMutator:       sampleMutator,
		reportSampleMutator: reportSampleMutator,
		stopped:             make(chan struct{}),
		l:                   l,
		ctx:                 ctx,
	}
	sl.scrapeCtx, sl.cancel = context.WithCancel(ctx)

	return sl
}

func (sl *scrapeLoop) run(interval, timeout time.Duration, errc chan<- error) {
	select {
	case <-time.After(sl.scraper.offset(interval)):
		// Continue after a scraping offset.
	case <-sl.scrapeCtx.Done():
		close(sl.stopped)
		return
	}

	var last time.Time

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	buf := bytes.NewBuffer(make([]byte, 0, 16000))

mainLoop:
	for {
		buf.Reset()
		select {
		case <-sl.ctx.Done():
			close(sl.stopped)
			return
		case <-sl.scrapeCtx.Done():
			break mainLoop
		default:
		}

		var (
			start             = time.Now()
			scrapeCtx, cancel = context.WithTimeout(sl.ctx, timeout)
		)

		// Only record after the first scrape.
		if !last.IsZero() {
			targetIntervalLength.WithLabelValues(interval.String()).Observe(
				time.Since(last).Seconds(),
			)
		}
		b := sl.buffers.Get(sl.lastScrapeSize)
		buf := bytes.NewBuffer(b)

		scrapeErr := sl.scraper.scrape(scrapeCtx, buf)
		cancel()

		var (
			total = 0
			added = 0
		)
		if scrapeErr == nil {
			b = buf.Bytes()
			// NOTE: There were issues with misbehaving clients in the past
			// that occasionally returned empty results. We don't want those
			// to falsely reset our buffer size.
			if len(b) > 0 {
				sl.lastScrapeSize = len(b)
			}

			var appErr error
			total, added, appErr = sl.append(b, start)
			if appErr != nil {
				level.Warn(sl.l).Log("msg", "append failed", "err", appErr)
				if scrapeErr == nil {
					scrapeErr = appErr
				}
			}

			sl.buffers.Put(b)
		} else {
			level.Debug(sl.l).Log("msg", "Scrape failed", "err", scrapeErr.Error())
			if errc != nil {
				errc <- scrapeErr
			}
		}

		sl.report(start, time.Since(start), total, added, scrapeErr)
		last = start

		select {
		case <-sl.ctx.Done():
			close(sl.stopped)
			return
		case <-sl.scrapeCtx.Done():
			break mainLoop
		case <-ticker.C:
		}
	}

	close(sl.stopped)
}

// Stop the scraping. Cancel the context to stop all writes.
func (sl *scrapeLoop) stop() {
	sl.cancel()
	<-sl.stopped
}

type sample struct {
	metric labels.Labels
	t      int64
	v      float64
}

type samples []sample

func (s samples) Len() int      { return len(s) }
func (s samples) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (s samples) Less(i, j int) bool {
	d := labels.Compare(s[i].metric, s[j].metric)
	if d < 0 {
		return true
	} else if d > 0 {
		return false
	}
	return s[i].t < s[j].t
}

func (sl *scrapeLoop) append(b []byte, ts time.Time) (total, added int, err error) {
	var (
		parser                      = expfmt.TextParser{}
		metricFamilies, parserError = parser.TextToMetricFamilies(strings.NewReader(string(b)))
		defTime                     = timestamp.FromTime(ts)
	)
	if parserError != nil {
		return total, added, parserError
	}

	var (
		app            = sl.appender()
		numOutOfOrder  = 0
		numDuplicates  = 0
		numOutOfBounds = 0
	)
	var sampleLimitErr error

	extractor := newPointExtractor(sl.resetPointMap)
loop:
	for name, metricFamily := range metricFamilies {
		total++
		metricFamily.Metric = relabelMetrics(metricFamily.Metric, sl.sampleMutator)
		// Drop family if we dropped all metrics.
		if metricFamily.Metric == nil {
			continue
		}
		metrics := []*dto.Metric{}
		resetTimes := []int64{}
		for _, metric := range metricFamily.Metric {
			if metric.TimestampMs == nil {
				metric.TimestampMs = proto.Int64(defTime)
			}
			if emit, resetTime := extractor.UpdateValue(metricFamily, metric); emit {
				metrics = append(metrics, metric)
				resetTimeMs := timestamp.FromTime(resetTime)
				resetTimes = append(resetTimes, resetTimeMs)
			}
		}
		metricFamily.Metric = metrics
		if len(metricFamily.Metric) == 0 {
			continue
		}
		err = app.Add(&MetricFamily{metricFamily, resetTimes})
		switch err {
		case nil:
		case ErrOutOfOrderSample:
			err = nil
			numOutOfOrder++
			level.Debug(sl.l).Log("msg", "Out of order sample", "series", string(name))
			targetScrapeSampleOutOfOrder.Inc()
			continue
		case ErrDuplicateSampleForTimestamp:
			err = nil
			numDuplicates++
			level.Debug(sl.l).Log("msg", "Duplicate sample for timestamp", "series", string(name))
			targetScrapeSampleDuplicate.Inc()
			continue
		case ErrOutOfBounds:
			err = nil
			numOutOfBounds++
			level.Debug(sl.l).Log("msg", "Out of bounds metric", "series", string(name))
			targetScrapeSampleOutOfBounds.Inc()
			continue
		case errSampleLimit:
			sampleLimitErr = err
			added++
			continue
		default:
			level.Debug(sl.l).Log("msg", "unexpected error", "series", string(name), "err", err)
			break loop
		}
		added++
	}
	if sampleLimitErr != nil {
		// We only want to increment this once per scrape, so this is Inc'd outside the loop.
		targetScrapeSampleLimit.Inc()
	}
	if numOutOfOrder > 0 {
		level.Warn(sl.l).Log("msg", "Error on ingesting out-of-order samples", "num_dropped", numOutOfOrder)
	}
	if numDuplicates > 0 {
		level.Warn(sl.l).Log("msg", "Error on ingesting samples with different value but same timestamp", "num_dropped", numDuplicates)
	}
	if numOutOfBounds > 0 {
		level.Warn(sl.l).Log("msg", "Error on ingesting samples that are too old or are too far into the future", "num_dropped", numOutOfBounds)
	}
	if err != nil {
		return total, added, err
	}

	return total, added, nil
}

const (
	scrapeHealthMetricName       = "up"
	scrapeDurationMetricName     = "scrape_duration_seconds"
	scrapeSamplesMetricName      = "scrape_samples_scraped"
	samplesPostRelabelMetricName = "scrape_samples_post_metric_relabeling"
)

func (sl *scrapeLoop) report(start time.Time, duration time.Duration, scraped, appended int, err error) error {
	sl.scraper.report(start, duration, err)

	ts := timestamp.FromTime(start)

	var health float64
	if err == nil {
		health = 1
	}
	app := sl.appender()

	if err := sl.addReportSample(app, scrapeHealthMetricName, ts, health); err != nil {
		return err
	}
	if err := sl.addReportSample(app, scrapeDurationMetricName, ts, duration.Seconds()); err != nil {
		return err
	}
	if err := sl.addReportSample(app, scrapeSamplesMetricName, ts, float64(scraped)); err != nil {
		return err
	}
	if err := sl.addReportSample(app, samplesPostRelabelMetricName, ts, float64(appended)); err != nil {
		return err
	}
	return nil
}

func (sl *scrapeLoop) addReportSample(app Appender, name string, t int64, v float64) error {
	lset := labels.Labels{
		labels.Label{Name: labels.MetricName, Value: name},
	}

	lset = sl.reportSampleMutator(lset)

	// TODO(jkohen): reportSampleMutator calls mutateReportSampleLabels, which returns the metric name and the target labels, so we could simplify the code to do just that.

	metricFamily := &dto.MetricFamily{
		Name: proto.String(name),
		Type: dto.MetricType_GAUGE.Enum(),
		Metric: []*dto.Metric{
			&dto.Metric{
				Gauge: &dto.Gauge{
					Value: proto.Float64(v),
				},
				TimestampMs: proto.Int64(t),
				Label:       LabelsToLabelPairs(lset),
			},
		},
	}
	err := app.Add(&MetricFamily{metricFamily, []int64{NoTimestamp}})
	switch err {
	case nil:
		return nil
	case ErrOutOfOrderSample, ErrDuplicateSampleForTimestamp:
		return nil
	default:
		return err
	}
}

// relabelMetrics returns null if the relabeling requested the metric to be dropped.
func relabelMetrics(inputMetrics []*dto.Metric, mutator labelsMutator) []*dto.Metric {
	metrics := []*dto.Metric{}
	for _, metric := range inputMetrics {
		lset := LabelPairsToLabels(metric.Label)

		// Add target labels and relabeling and store the final label set.
		lset = mutator(lset)
		// The label set may be set to nil to indicate dropping.
		if lset == nil {
			continue
		}

		metric.Label = LabelsToLabelPairs(lset)
		metrics = append(metrics, metric)
	}
	if len(metrics) == 0 {
		return nil
	}
	return metrics
}

type pointExtractor struct {
	resetPointMap resetPointMapper
	firstScrape   bool
}

// newPointExtractor returns a new PointExtractor. A new PointExtractor should
// be used for each independent scrape. On the other hand, the resetPointMap
// should match the life cycle of the target as accurately as possible (as
// opposed to the scrapeLoop).
func newPointExtractor(resetPointMap resetPointMapper) (mutator *pointExtractor) {
	return &pointExtractor{
		resetPointMap: resetPointMap,
		firstScrape:   !resetPointMap.HasResetPoints(),
	}
}

// getPoint returns the point within the given Metric. In some cases it may
// return nil, indicating that no Point should be emitted. E.g., when the reset
// timestamp is unknown.
func (m *pointExtractor) UpdateValue(family *dto.MetricFamily, metric *dto.Metric) (
	emit bool, resetTimestamp time.Time) {
	if !ResetPointCompatible(family.GetType()) {
		emit = true
		resetTimestamp = timestamp.Time(NoTimestamp)
		return
	}
	key := NewResetPointKey(
		family.GetName(), LabelPairsToLabels(metric.Label), family.GetType())
	value := valueFromMetric(metric)
	if m.firstScrape {
		m.resetPointMap.AddResetPoint(key, Point{
			Timestamp:  timestamp.Time(metric.GetTimestampMs()),
			ResetValue: value,
			LastValue:  value,
		})
		// Reset time unknown, use the first point as the reset
		// point, and drop its value from the output.
		emit = false
		return
	}
	resetPoint := m.resetPointMap.GetResetPoint(key)
	if resetPoint == nil ||
		(resetPoint.LastValue != nil && valueReset(metric, *resetPoint.LastValue)) {

		// New time series or value decreased, use the point itself as
		// the reset point. Subtract an epsilon from the reset
		// timestamp, because Stackdriver requires a non-zero interval
		// for cumulative metrics.
		resetPoint = &Point{Timestamp: timestamp.Time(metric.GetTimestampMs() - 1)}
	}
	resetPoint.LastValue = value
	m.resetPointMap.AddResetPoint(key, *resetPoint)
	if resetPoint.ResetValue != nil {
		subtractResetValue(*resetPoint.ResetValue, metric)
	}
	emit = true
	resetTimestamp = resetPoint.Timestamp
	return
}

func valueFromMetric(metric *dto.Metric) *PointValue {
	if metric.Counter != nil {
		return &PointValue{Counter: metric.Counter.GetValue()}
	} else if metric.Histogram != nil {
		h := metric.Histogram
		value := &PointValue{
			Histogram: PointHistogram{
				Count:  h.GetSampleCount(),
				Sum:    h.GetSampleSum(),
				Bucket: make([]PointHistogramBucket, len(h.Bucket)),
			},
		}
		for i, bucket := range h.Bucket {
			value.Histogram.Bucket[i].UpperBound = bucket.GetUpperBound()
			value.Histogram.Bucket[i].CumulativeCount = bucket.GetCumulativeCount()
		}
		return value
	}
	return nil
}

func valueReset(metric *dto.Metric, ref PointValue) bool {
	if metric.Counter != nil {
		return metric.Counter.GetValue() < ref.Counter
	} else if metric.Histogram != nil {
		return metric.Histogram.GetSampleSum() < ref.Histogram.Sum ||
			metric.Histogram.GetSampleCount() < ref.Histogram.Count
	}
	return false
}

// subtractResetValue expects the histograms to have the same bucket bounds. If this isn't the case, it returns the original struct, which allows for the buckets to change over time, with only some blips in the cumulative data.
func subtractResetValue(resetValue PointValue, metric *dto.Metric) {
	if resetValue.Counter > 0 {
		*metric.Counter.Value -= resetValue.Counter
	}
	if resetValue.Histogram.Count > 0 {
		h := metric.Histogram
		r := resetValue.Histogram
		if len(h.Bucket) != len(r.Bucket) {
			return
		}
		for i, bucket := range h.Bucket {
			if bucket.GetUpperBound() != r.Bucket[i].UpperBound {
				return
			}
		}
		*h.SampleCount -= r.Count
		*h.SampleSum -= r.Sum
		for i, _ := range h.Bucket {
			*h.Bucket[i].CumulativeCount -= r.Bucket[i].CumulativeCount
		}
	}
}
