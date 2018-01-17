// Copyright 2018 Google LLC

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     https://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package stackdriver

import (
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/jkohen/prometheus/retrieval"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/storage/remote"
)

type Storage struct {
	logger log.Logger
	mtx    sync.RWMutex

	// For writes
	queues []*remote.QueueManager
}

func NewStorage(logger log.Logger) *Storage {
	return &Storage{
		logger: logger,
	}
}

// Appender implements the retrieval.Appendable interface.
func (s *Storage) Appender() (retrieval.Appender, error) {
	return s, nil
}

// Add implements the retrieval.Appender interface.
func (s *Storage) Add(metricFamily *dto.MetricFamily) error {
	// TODO(jkohen): Implement.
	return nil
}

// Close closes the storage and all its underlying resources.
func (s *Storage) Close() error {
	return nil
}

// ApplyConfig updates the state as the new config requires.
func (s *Storage) ApplyConfig(conf *config.Config) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	// Update write queues

	newQueues := []*remote.QueueManager{}
	// TODO: we should only stop & recreate queues which have changes,
	// as this can be quite disruptive.
	for i, rwConf := range conf.RemoteWriteConfigs {
		c, err := NewClient(i, &ClientConfig{
			URL:              rwConf.URL,
			Timeout:          rwConf.RemoteTimeout,
			HTTPClientConfig: rwConf.HTTPClientConfig,
		})
		if err != nil {
			return err
		}
		newQueues = append(newQueues, remote.NewQueueManager(
			s.logger,
			config.DefaultQueueConfig,
			conf.GlobalConfig.ExternalLabels,
			rwConf.WriteRelabelConfigs,
			c,
		))
	}

	for _, q := range s.queues {
		q.Stop()
	}

	s.queues = newQueues
	for _, q := range s.queues {
		q.Start()
	}

	return nil
}
