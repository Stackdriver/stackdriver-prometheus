// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v1

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/Stackdriver/stackdriver-prometheus/retrieval"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/util/httputil"
)

type status string

const (
	statusSuccess status = "success"
	statusError   status = "error"
)

type errorType string

const (
	errorNone        errorType = ""
	errorTimeout     errorType = "timeout"
	errorCanceled    errorType = "canceled"
	errorExec        errorType = "execution"
	errorBadData     errorType = "bad_data"
	errorInternal    errorType = "internal"
	errorUnavailable errorType = "unavailable"
)

var corsHeaders = map[string]string{
	"Access-Control-Allow-Headers":  "Accept, Authorization, Content-Type, Origin",
	"Access-Control-Allow-Methods":  "GET, OPTIONS",
	"Access-Control-Allow-Origin":   "*",
	"Access-Control-Expose-Headers": "Date",
}

type apiError struct {
	typ errorType
	err error
}

func (e *apiError) Error() string {
	return fmt.Sprintf("%s: %s", e.typ, e.err)
}

type targetRetriever interface {
	Targets() []*retrieval.Target
}

type alertmanagerRetriever interface {
	Alertmanagers() []*url.URL
}

type response struct {
	Status    status      `json:"status"`
	Data      interface{} `json:"data,omitempty"`
	ErrorType errorType   `json:"errorType,omitempty"`
	Error     string      `json:"error,omitempty"`
}

// Enables cross-site script calls.
func setCORS(w http.ResponseWriter) {
	for h, v := range corsHeaders {
		w.Header().Set(h, v)
	}
}

type apiFunc func(r *http.Request) (interface{}, *apiError)

// API can register a set of endpoints in a router and handle
// them using the provided storage and query engine.
type API struct {
	targetRetriever targetRetriever

	now    func() time.Time
	config func() config.Config
	ready  func(http.HandlerFunc) http.HandlerFunc

	enableAdmin bool
}

// NewAPI returns an initialized API type.
func NewAPI(
	tr targetRetriever,
	configFunc func() config.Config,
	readyFunc func(http.HandlerFunc) http.HandlerFunc,
) *API {
	return &API{
		targetRetriever: tr,
		now:             time.Now,
		config:          configFunc,
		ready:           readyFunc,
	}
}

// Register the API's endpoints in the given router.
func (api *API) Register(r *route.Router) {
	instr := func(name string, f apiFunc) http.HandlerFunc {
		hf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			setCORS(w)
			if data, err := f(r); err != nil {
				respondError(w, err, data)
			} else if data != nil {
				respond(w, data)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
		})
		return api.ready(prometheus.InstrumentHandler(name, httputil.CompressionHandler{
			Handler: hf,
		}))
	}

	r.Options("/*path", instr("options", api.options))

	r.Get("/targets", instr("targets", api.targets))

	r.Get("/status/config", instr("config", api.serveConfig))
}

func (api *API) options(r *http.Request) (interface{}, *apiError) {
	return nil, nil
}

// Target has the information for one target.
type Target struct {
	// Labels before any processing.
	DiscoveredLabels map[string]string `json:"discoveredLabels"`
	// Any labels that are added to this target and its metrics.
	Labels map[string]string `json:"labels"`

	ScrapeURL string `json:"scrapeUrl"`

	LastError  string                 `json:"lastError"`
	LastScrape time.Time              `json:"lastScrape"`
	Health     retrieval.TargetHealth `json:"health"`
}

// TargetDiscovery has all the active targets.
type TargetDiscovery struct {
	ActiveTargets []*Target `json:"activeTargets"`
}

func (api *API) targets(r *http.Request) (interface{}, *apiError) {
	targets := api.targetRetriever.Targets()
	res := &TargetDiscovery{ActiveTargets: make([]*Target, len(targets))}

	for i, t := range targets {
		lastErrStr := ""
		lastErr := t.LastError()
		if lastErr != nil {
			lastErrStr = lastErr.Error()
		}

		res.ActiveTargets[i] = &Target{
			DiscoveredLabels: t.DiscoveredLabels().Map(),
			Labels:           t.Labels().Map(),
			ScrapeURL:        t.URL().String(),
			LastError:        lastErrStr,
			LastScrape:       t.LastScrape(),
			Health:           t.Health(),
		}
	}

	return res, nil
}

type prometheusConfig struct {
	YAML string `json:"yaml"`
}

func (api *API) serveConfig(r *http.Request) (interface{}, *apiError) {
	cfg := &prometheusConfig{
		YAML: api.config().String(),
	}
	return cfg, nil
}

func respond(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	b, err := json.Marshal(&response{
		Status: statusSuccess,
		Data:   data,
	})
	if err != nil {
		return
	}
	w.Write(b)
}

func respondError(w http.ResponseWriter, apiErr *apiError, data interface{}) {
	w.Header().Set("Content-Type", "application/json")

	var code int
	switch apiErr.typ {
	case errorBadData:
		code = http.StatusBadRequest
	case errorExec:
		code = 422
	case errorCanceled, errorTimeout:
		code = http.StatusServiceUnavailable
	case errorInternal:
		code = http.StatusInternalServerError
	default:
		code = http.StatusInternalServerError
	}
	w.WriteHeader(code)

	b, err := json.Marshal(&response{
		Status:    statusError,
		ErrorType: apiErr.typ,
		Error:     apiErr.err.Error(),
		Data:      data,
	})
	if err != nil {
		return
	}
	w.Write(b)
}

func parseTime(s string) (time.Time, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		return time.Unix(int64(s), int64(ns*float64(time.Second))), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("cannot parse %q to a valid timestamp", s)
}

func parseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, fmt.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}
	return 0, fmt.Errorf("cannot parse %q to a valid duration", s)
}
