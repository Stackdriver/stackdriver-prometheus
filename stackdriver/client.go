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

package stackdriver

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/oauth2/google"

	"google.golang.org/api/googleapi"
	monitoring "google.golang.org/api/monitoring/v3"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/model"

	config_util "github.com/prometheus/common/config"
)

const (
	// TODO(jkohen): Use a custom prefix specific to Prometheus.
	metricsPrefix             = "custom.googleapis.com"
	maxTimeseriesesPerRequest = 200
)

// Client allows reading and writing from/to a remote HTTP endpoint.
type Client struct {
	index     int // Used to differentiate clients in metrics.
	logger    log.Logger
	projectId string
	url       *config_util.URL
	timeout   time.Duration
}

// ClientConfig configures a Client.
type ClientConfig struct {
	Logger    log.Logger
	ProjectId string // The Stackdriver project id in "projects/name-or-number" format.
	URL       *config_util.URL
	Timeout   model.Duration
}

// NewClient creates a new Client.
func NewClient(index int, conf *ClientConfig) (*Client, error) {
	return &Client{
		index:     index,
		logger:    conf.Logger,
		projectId: conf.ProjectId,
		url:       conf.URL,
		timeout:   time.Duration(conf.Timeout),
	}, nil
}

type recoverableError struct {
	error
}

// Store sends a batch of samples to the HTTP endpoint.
func (c *Client) Store(req *monitoring.CreateTimeSeriesRequest) error {
	tss := req.TimeSeries
	if len(tss) == 0 {
		// Nothing to do, return silently.
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	client, err := google.DefaultClient(ctx, monitoring.MonitoringWriteScope)
	if err != nil {
		return err
	}
	service, err := monitoring.New(client)
	service.BasePath = c.url.String()
	if err != nil {
		return err
	}

	errors := make(chan error, len(tss)/maxTimeseriesesPerRequest+1)
	var wg sync.WaitGroup
	for i := 0; i < len(tss); i += maxTimeseriesesPerRequest {
		end := i + maxTimeseriesesPerRequest
		if end > len(tss) {
			end = len(tss)
		}
		wg.Add(1)
		go func(begin int, end int) {
			defer wg.Done()
			req_copy := &monitoring.CreateTimeSeriesRequest{
				TimeSeries: req.TimeSeries[begin:end],
			}
			_, err := service.Projects.TimeSeries.Create(c.projectId, req_copy).Context(ctx).Do()
			if err != nil {
				gerr, ok := err.(*googleapi.Error)
				if !ok {
					level.Warn(c.logger).Log("msg", "Unexpected error message type from Monitoring API", "err", err)
					errors <- err
				}
				if gerr.Code/100 == 5 {
					errors <- recoverableError{err}
				} else {
					errors <- err
				}
			}
		}(i, end)
	}
	wg.Wait()
	close(errors)
	if err, ok := <-errors; ok {
		return err
	}
	return nil
}

// Name identifies the client.
func (c Client) Name() string {
	return fmt.Sprintf("%d:%s", c.index, c.url)
}
