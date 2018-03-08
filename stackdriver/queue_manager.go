// Copyright 2013 The Prometheus Authors
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

package stackdriver

import (
	"math"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/jkohen/prometheus/relabel"
	"github.com/jkohen/prometheus/retrieval"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"golang.org/x/time/rate"
	monitoring "google.golang.org/genproto/googleapis/monitoring/v3"
)

// String constants for instrumentation.
const (
	namespace = "prometheus"
	subsystem = "remote_storage"
	queue     = "queue"

	// We track samples in/out and how long pushes take using an Exponentially
	// Weighted Moving Average.
	ewmaWeight          = 0.2
	shardUpdateDuration = 10 * time.Second

	// Allow 10% too many shards before scaling down.
	shardToleranceFraction = 0.1

	// Limit to 1 log event every 10s
	logRateLimit = 0.1
	logBurst     = 10
)

var (
	succeededSamplesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "succeeded_samples_total",
			Help:      "Total number of samples successfully sent to remote storage.",
		},
		[]string{queue},
	)
	failedSamplesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "failed_samples_total",
			Help:      "Total number of samples which failed on send to remote storage.",
		},
		[]string{queue},
	)
	droppedSamplesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "dropped_samples_total",
			Help:      "Total number of samples which were dropped due to the queue being full.",
		},
		[]string{queue},
	)
	sentBatchDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "sent_batch_duration_seconds",
			Help:      "Duration of sample batch send calls to the remote storage.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{queue},
	)
	queueLength = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "queue_length",
			Help:      "The number of processed samples queued to be sent to the remote storage.",
		},
		[]string{queue},
	)
	queueCapacity = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "queue_capacity",
			Help:      "The capacity of the queue of samples to be sent to the remote storage.",
		},
		[]string{queue},
	)
	numShards = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "shards",
			Help:      "The number of shards used for parallel sending to the remote storage.",
		},
		[]string{queue},
	)
)

func init() {
	prometheus.MustRegister(succeededSamplesTotal)
	prometheus.MustRegister(failedSamplesTotal)
	prometheus.MustRegister(droppedSamplesTotal)
	prometheus.MustRegister(sentBatchDuration)
	prometheus.MustRegister(queueLength)
	prometheus.MustRegister(queueCapacity)
	prometheus.MustRegister(numShards)
}

// StorageClient defines an interface for sending a batch of samples to an
// external timeseries database.
type StorageClient interface {
	// Store stores the given metric families in the remote storage.
	Store(*monitoring.CreateTimeSeriesRequest) error
	// Name identifies the remote storage implementation.
	Name() string
	// Release the resources allocated by the client.
	Close() error
}

type StorageClientFactory interface {
	New() StorageClient
	Name() string
}

// QueueManager manages a queue of samples to be sent to the Storage
// indicated by the provided StorageClient.
type QueueManager struct {
	logger log.Logger

	cfg              config.QueueConfig
	externalLabels   map[string]*string
	relabelConfigs   []*config.RelabelConfig
	clientFactory    StorageClientFactory
	queueName        string
	logLimiter       *rate.Limiter
	resourceMappings []ResourceMap

	shardsMtx   sync.RWMutex
	shards      *shards
	numShards   int
	reshardChan chan int
	quit        chan struct{}
	wg          sync.WaitGroup

	samplesIn *ewmaRate
}

// NewQueueManager builds a new QueueManager.
func NewQueueManager(logger log.Logger, cfg config.QueueConfig, externalLabelSet model.LabelSet, relabelConfigs []*config.RelabelConfig, clientFactory StorageClientFactory, sdCfg *StackdriverConfig) *QueueManager {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	resourceMappings := DefaultResourceMappings
	if sdCfg.K8sResourceTypes {
		resourceMappings = K8sResourceMappings
	}

	externalLabels := make(map[string]*string, len(externalLabelSet))
	for ln, lv := range externalLabelSet {
		externalLabels[string(ln)] = proto.String(string(lv))
	}
	t := &QueueManager{
		logger:           logger,
		cfg:              cfg,
		externalLabels:   externalLabels,
		relabelConfigs:   relabelConfigs,
		clientFactory:    clientFactory,
		queueName:        clientFactory.Name(),
		resourceMappings: resourceMappings,

		logLimiter:  rate.NewLimiter(logRateLimit, logBurst),
		numShards:   1,
		reshardChan: make(chan int),
		quit:        make(chan struct{}),

		samplesIn: newEWMARate(ewmaWeight, shardUpdateDuration),
	}
	t.shards = t.newShards(t.numShards)
	numShards.WithLabelValues(t.queueName).Set(float64(t.numShards))
	queueCapacity.WithLabelValues(t.queueName).Set(float64(t.cfg.Capacity))

	// Initialise counter labels to zero.
	sentBatchDuration.WithLabelValues(t.queueName)
	succeededSamplesTotal.WithLabelValues(t.queueName)
	failedSamplesTotal.WithLabelValues(t.queueName)
	droppedSamplesTotal.WithLabelValues(t.queueName)

	return t
}

// Append queues a sample to be sent to the remote storage. It drops the
// sample on the floor if the queue is full.
// Always returns nil.
func (t *QueueManager) Append(metricFamily *retrieval.MetricFamily) error {
	metricFamily = t.relabelMetrics(metricFamily)
	// Drop family if we dropped all metrics.
	if metricFamily.Metric == nil {
		return nil
	}
	if len(metricFamily.Metric) != len(metricFamily.MetricResetTimestampMs) {
		level.Error(t.logger).Log(
			"msg", "bug: number of metrics and reset timestamps must match",
			"metrics", len(metricFamily.Metric),
			"reset_ts", len(metricFamily.MetricResetTimestampMs))
	}

	t.shardsMtx.RLock()
	for i := range metricFamily.Metric {
		metricFamilySlice := &retrieval.MetricFamily{
			MetricFamily: &dto.MetricFamily{
				Name:   metricFamily.Name,
				Help:   metricFamily.Help,
				Type:   metricFamily.Type,
				Metric: metricFamily.Metric[i : i+1],
			},
			MetricResetTimestampMs: metricFamily.MetricResetTimestampMs[i : i+1],
		}
		t.shards.enqueue(metricFamilySlice)
	}
	t.shardsMtx.RUnlock()

	queueLength.WithLabelValues(t.queueName).Inc()
	return nil
}

// Start the queue manager sending samples to the remote storage.
// Does not block.
func (t *QueueManager) Start() {
	t.wg.Add(2)
	go t.updateShardsLoop()
	go t.reshardLoop()

	t.shardsMtx.Lock()
	defer t.shardsMtx.Unlock()
	t.shards.start()
}

// Stop stops sending samples to the remote storage and waits for pending
// sends to complete.
func (t *QueueManager) Stop() {
	level.Info(t.logger).Log("msg", "Stopping remote storage...")
	close(t.quit)
	t.wg.Wait()

	t.shardsMtx.Lock()
	defer t.shardsMtx.Unlock()
	t.shards.stop()

	level.Info(t.logger).Log("msg", "Remote storage stopped.")
}

func (t *QueueManager) updateShardsLoop() {
	defer t.wg.Done()

	ticker := time.NewTicker(shardUpdateDuration)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			t.calculateDesiredShards()
		case <-t.quit:
			return
		}
	}
}

func (t *QueueManager) calculateDesiredShards() {
	t.samplesIn.tick()

	// We use the number of incoming samples as a prediction of how much work we
	// will need to do next iteration.  We add to this any pending samples
	// (received - send) so we can catch up with any backlog. We use the average
	// outgoing batch latency to work out how many shards we need.
	// These rates are samples per second.
	samplesIn := t.samplesIn.rate()

	// Size the shards so that they can fit enough samples to keep
	// good flow, but not more than the batch send deadline,
	// otherwise we'll underutilize the batches. Below "send" is for one batch.
	desiredShards := t.cfg.BatchSendDeadline.Seconds() * samplesIn / float64(t.cfg.Capacity)

	level.Debug(t.logger).Log("msg", "QueueManager.caclulateDesiredShards",
		"samplesIn", samplesIn, "desiredShards", desiredShards)

	// Changes in the number of shards must be greater than shardToleranceFraction.
	var (
		lowerBound = float64(t.numShards) * (1. - shardToleranceFraction)
		upperBound = float64(t.numShards) * (1. + shardToleranceFraction)
	)
	level.Debug(t.logger).Log("msg", "QueueManager.updateShardsLoop",
		"lowerBound", lowerBound, "desiredShards", desiredShards, "upperBound", upperBound)
	if lowerBound <= desiredShards && desiredShards <= upperBound {
		return
	}

	numShards := int(math.Ceil(desiredShards))
	if numShards > t.cfg.MaxShards {
		numShards = t.cfg.MaxShards
	} else if numShards < 1 {
		numShards = 1
	}
	if numShards == t.numShards {
		return
	}

	// Resharding can take some time, and we want this loop
	// to stay close to shardUpdateDuration.
	select {
	case t.reshardChan <- numShards:
		level.Debug(t.logger).Log("msg", "Remote storage resharding", "from", t.numShards, "to", numShards)
		t.numShards = numShards
	default:
		level.Debug(t.logger).Log("msg", "Currently resharding, skipping", "to", numShards)
	}
}

func (t *QueueManager) reshardLoop() {
	defer t.wg.Done()

	for {
		select {
		case numShards := <-t.reshardChan:
			t.reshard(numShards)
		case <-t.quit:
			return
		}
	}
}

func (t *QueueManager) reshard(n int) {
	numShards.WithLabelValues(t.queueName).Set(float64(n))

	t.shardsMtx.Lock()
	newShards := t.newShards(n)
	oldShards := t.shards
	t.shards = newShards
	t.shardsMtx.Unlock()

	oldShards.stop()

	// We start the newShards after we have stopped (the therefore completely
	// flushed) the oldShards, to guarantee we only every deliver samples in
	// order.
	newShards.start()
}

type shards struct {
	qm         *QueueManager
	client     StorageClient
	translator *Translator
	queues     []chan *retrieval.MetricFamily
	done       chan struct{}
	wg         sync.WaitGroup
}

func (t *QueueManager) newShards(numShards int) *shards {
	queues := make([]chan *retrieval.MetricFamily, numShards)
	for i := 0; i < numShards; i++ {
		queues[i] = make(chan *retrieval.MetricFamily, t.cfg.Capacity)
	}
	s := &shards{
		qm:         t,
		translator: NewTranslator(t.logger, metricsPrefix, t.resourceMappings),
		queues:     queues,
		done:       make(chan struct{}),
	}
	s.wg.Add(numShards)
	return s
}

func (s *shards) len() int {
	return len(s.queues)
}

func (s *shards) start() {
	for i := 0; i < len(s.queues); i++ {
		go s.runShard(i)
	}
}

func (s *shards) stop() {
	for _, shard := range s.queues {
		close(shard)
	}
	s.wg.Wait()
	level.Debug(s.qm.logger).Log("msg", "Stopped resharding")
}

func (s *shards) enqueue(sample *retrieval.MetricFamily) {
	s.qm.samplesIn.incr(1)

	fp := sample.Fingerprint()
	shard := fp % uint64(len(s.queues))
	s.queues[shard] <- sample
}

func (s *shards) runShard(i int) {
	defer s.wg.Done()
	client := s.qm.clientFactory.New()
	defer client.Close()
	queue := s.queues[i]

	// Send batches of at most MaxSamplesPerSend samples to the remote storage.
	// If we have fewer samples than that, flush them out after a deadline
	// anyways.
	pendingSamples := make([]*retrieval.MetricFamily, 0, s.qm.cfg.MaxSamplesPerSend)

	timer := time.NewTimer(s.qm.cfg.BatchSendDeadline)
	defer func() {
		if !timer.Stop() {
			<-timer.C
		}
	}()

	for {
		select {
		case sample, ok := <-queue:
			if !ok {
				if len(pendingSamples) > 0 {
					level.Debug(s.qm.logger).Log("msg", "Flushing samples to remote storage...", "count", len(pendingSamples))
					s.sendSamples(client, pendingSamples)
					level.Debug(s.qm.logger).Log("msg", "Done flushing.")
				}
				return
			}

			queueLength.WithLabelValues(s.qm.queueName).Dec()
			pendingSamples = append(pendingSamples, sample)

			var sentSamples bool
			for len(pendingSamples) >= s.qm.cfg.MaxSamplesPerSend {
				s.sendSamples(client, pendingSamples[:s.qm.cfg.MaxSamplesPerSend])
				pendingSamples = pendingSamples[s.qm.cfg.MaxSamplesPerSend:]
				sentSamples = true
			}
			if sentSamples {
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(s.qm.cfg.BatchSendDeadline)
			}
		case <-timer.C:
			if len(pendingSamples) > 0 {
				s.sendSamples(client, pendingSamples)
				pendingSamples = pendingSamples[:0]
			}
			timer.Reset(s.qm.cfg.BatchSendDeadline)
		}
	}
}

// sendSamples to the remote storage with backoff for recoverable errors.
func (s *shards) sendSamples(client StorageClient, samples []*retrieval.MetricFamily) {
	req := s.translator.ToCreateTimeSeriesRequest(samples)
	backoff := s.qm.cfg.MinBackoff
	for retries := s.qm.cfg.MaxRetries; retries > 0; retries-- {
		begin := time.Now()
		err := client.Store(req)

		sentBatchDuration.WithLabelValues(s.qm.queueName).Observe(time.Since(begin).Seconds())
		if err == nil {
			succeededSamplesTotal.WithLabelValues(s.qm.queueName).Add(float64(len(samples)))
			return
		}

		if _, ok := err.(recoverableError); !ok {
			level.Warn(s.qm.logger).Log("msg", "Unrecoverable error sending samples to remote storage", "err", err)
			break
		}
		time.Sleep(backoff)
		backoff = backoff * 2
		if backoff > s.qm.cfg.MaxBackoff {
			backoff = s.qm.cfg.MaxBackoff
		}
	}

	failedSamplesTotal.WithLabelValues(s.qm.queueName).Add(float64(len(samples)))
}

// relabelMetrics returns nil if the relabeling requested the metric to be dropped.
func (t *QueueManager) relabelMetrics(metricFamily *retrieval.MetricFamily) *retrieval.MetricFamily {
	metrics := []*dto.Metric{}
	resetTimestamps := []int64{}
	for i, metric := range metricFamily.Metric {
		if metric.Label == nil {
			// Need to distinguish dropped labels from uninitialized.
			metric.Label = []*dto.LabelPair{}
		}
		// Add any external labels. If an external label name is already
		// found in the set of metric labels, don't add that label.
		lset := make(map[string]struct{})
		for i := range metric.Label {
			lset[*metric.Label[i].Name] = struct{}{}
		}
		for ln, lv := range t.externalLabels {
			if _, ok := lset[ln]; !ok {
				metric.Label = append(metric.Label, &dto.LabelPair{
					Name:  proto.String(ln),
					Value: lv,
				})
			}
		}

		metric.Label = relabel.Process(metric.Label, t.relabelConfigs...)
		// The label set may be set to nil to indicate dropping.
		if metric.Label != nil {
			if len(metric.Label) == 0 {
				metric.Label = nil
			}
			metrics = append(metrics, metric)
			resetTimestamps = append(resetTimestamps, metricFamily.MetricResetTimestampMs[i])
		}
	}
	metricFamily.Metric = metrics
	metricFamily.MetricResetTimestampMs = resetTimestamps
	return metricFamily
}
