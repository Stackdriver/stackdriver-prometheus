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
package stackdriver

import (
	"fmt"
	"sync"

	md "cloud.google.com/go/compute/metadata"
	"github.com/Stackdriver/stackdriver-prometheus/retrieval"
	"github.com/go-kit/kit/log"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
)

type Storage struct {
	logger log.Logger
	cfg    *StackdriverConfig
	mtx    sync.RWMutex

	// For writes
	queues []*QueueManager
}

func NewStorage(logger log.Logger, cfg *StackdriverConfig) *Storage {
	return &Storage{
		logger: logger,
		cfg:    cfg,
	}
}

// Appender implements the retrieval.Appendable interface.
func (s *Storage) Appender() (retrieval.Appender, error) {
	return s, nil
}

// Add implements the retrieval.Appender interface.
func (s *Storage) Add(metricFamily *retrieval.MetricFamily) error {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	for _, q := range s.queues {
		q.Append(metricFamily)
	}
	return nil
}

// Close closes the storage and all its underlying resources.
func (s *Storage) Close() error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	for _, q := range s.queues {
		q.Stop()
	}

	return nil
}

// ApplyConfig updates the state as the new config requires.
func (s *Storage) ApplyConfig(conf *config.Config) error {
	var projectId string
	var location string
	var clusterName string
	el := conf.GlobalConfig.ExternalLabels
	if md.OnGCE() {
		if id, err := md.ProjectID(); err == nil {
			projectId = "projects/" + id
			if v, ok := el[ProjectIdLabel]; !ok || v == "" {
				el[ProjectIdLabel] = model.LabelValue(id)
			}
		}
		if l, err := md.InstanceAttributeValue("cluster-location"); err == nil {
			location = l
			if v, ok := el[LocationLabel]; !ok || v == "" {
				el[LocationLabel] = model.LabelValue(l)
			}
		}
		if cn, err := md.InstanceAttributeValue("cluster-name"); err == nil {
			clusterName = cn
			if v, ok := el[ClusterNameLabel]; !ok || v == "" {
				el[ClusterNameLabel] = model.LabelValue(cn)
			}
		}
	}
	if value, ok := el[ProjectIdLabel]; !ok {
		return fmt.Errorf(
			"the Stackdriver remote writer requires an external label '%s' in its configuration, and it must contain a project id or number",
			ProjectIdLabel)
	} else {
		projectId = fmt.Sprintf("projects/%v", value)
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()

	// Update write queues

	newQueues := []*QueueManager{}
	// TODO: we should only stop & recreate queues which have changes,
	// as this can be quite disruptive.
	for i, rwConf := range conf.RemoteWriteConfigs {
		newQueues = append(newQueues, NewQueueManager(
			s.logger,
			rwConf.QueueConfig,
			el,
			rwConf.WriteRelabelConfigs,
			&clientFactory{
				logger:      s.logger,
				projectId:   projectId,
				location:    location,
				clusterName: clusterName,
				url:         rwConf.URL,
				timeout:     rwConf.RemoteTimeout,
				index:       i,
			},
			s.cfg,
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

type clientFactory struct {
	logger      log.Logger
	projectId   string
	location    string
	clusterName string
	url         *config_util.URL
	timeout     model.Duration
	index       int
}

func (f *clientFactory) New() StorageClient {
	return NewClient(f.index, &ClientConfig{
		Logger:      f.logger,
		ProjectId:   f.projectId,
		ClusterName: f.clusterName,
		Location:    f.location,
		URL:         f.url,
		Timeout:     f.timeout,
	})
}

func (f *clientFactory) Name() string {
	return fmt.Sprintf("%d:%s", f.index, f.url)
}
