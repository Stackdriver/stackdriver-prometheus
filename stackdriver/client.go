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
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	monitoring "google.golang.org/genproto/googleapis/monitoring/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"
	"google.golang.org/grpc/status"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/model"

	config_util "github.com/prometheus/common/config"
)

const (
	// TODO(jkohen): Use a custom prefix specific to Prometheus.
	metricsPrefix             = "custom.googleapis.com"
	maxTimeseriesesPerRequest = 200
	MonitoringWriteScope      = "https://www.googleapis.com/auth/monitoring.write"
)

// Client allows reading and writing from/to a remote HTTP endpoint.
type Client struct {
	index     int // Used to differentiate clients in metrics.
	logger    log.Logger
	projectId string
	url       *config_util.URL
	timeout   time.Duration
	dopts     []grpc.DialOption
}

// ClientConfig configures a Client.
type ClientConfig struct {
	Logger    log.Logger
	ProjectId string // The Stackdriver project id in "projects/name-or-number" format.
	URL       *config_util.URL
	Timeout   model.Duration
	Auth      bool
}

// NewClient creates a new Client.
func NewClient(index int, conf *ClientConfig) (*Client, error) {
	logger := conf.Logger
	if logger == nil {
		logger = log.NewNopLogger()
	}
	dopts := []grpc.DialOption{}
	if conf.Auth {
		rpcCreds, err := oauth.NewApplicationDefault(context.Background(), MonitoringWriteScope)
		if err != nil {
			return nil, err
		}
		tlsCreds := credentials.NewTLS(&tls.Config{})
		dopts = append(dopts,
			grpc.WithTransportCredentials(tlsCreds),
			grpc.WithPerRPCCredentials(rpcCreds))
	} else {
		dopts = append(dopts, grpc.WithInsecure())
	}
	return &Client{
		index:     index,
		logger:    logger,
		projectId: conf.ProjectId,
		url:       conf.URL,
		timeout:   time.Duration(conf.Timeout),
		dopts:     dopts,
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

	address := c.url.Hostname()
	if len(c.url.Port()) > 0 {
		address = fmt.Sprintf("%s:%s", address, c.url.Port())
	}
	level.Debug(c.logger).Log(
		"msg", "sending request to Stackdriver",
		"address", address)
	conn, err := grpc.Dial(address, c.dopts...)
	if err != nil {
		return err
	}
	defer conn.Close()
	service := monitoring.NewMetricServiceClient(conn)

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
				Name:       c.projectId,
				TimeSeries: req.TimeSeries[begin:end],
			}
			_, err := service.CreateTimeSeries(ctx, req_copy)
			if err != nil {
				level.Debug(c.logger).Log(
					"msg", "Partial failure calling CreateTimeSeries",
					"err", err,
					"req", req_copy.String())
				status, ok := status.FromError(err)
				if !ok {
					level.Warn(c.logger).Log("msg", "Unexpected error message type from Monitoring API", "err", err)
					errors <- err
					return
				}
				if status.Code() == codes.Unavailable {
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
