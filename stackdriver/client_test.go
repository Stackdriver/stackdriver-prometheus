// Copyright 2017 The Prometheus Authors
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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"google.golang.org/api/googleapi"
	monitoring "google.golang.org/api/monitoring/v3"

	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
)

var longErrMessage = strings.Repeat("[error message]", 10)

func TestStoreErrorHandling(t *testing.T) {
	tests := []struct {
		code        int
		err         error //*googleapi.Error
		recoverable bool
	}{
		{
			code: 200,
			err:  nil,
		},
		{
			code: 404,
			err: &googleapi.Error{
				Code: 404,
				Body: longErrMessage,
			},
			recoverable: false,
		},
		{
			code: 500,
			err: &googleapi.Error{
				Code: 500,
				Body: longErrMessage,
			},
			recoverable: true,
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			server := httptest.NewServer(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if test.err == nil {
						w.WriteHeader(http.StatusOK)
						io.WriteString(w, "{}")
					} else {
						// This isn't exactly how the Monitoring API encodes errors, but it works well enough for testing.
						http.Error(w, longErrMessage, test.code)
					}
				}),
			)

			serverURL, err := url.Parse(server.URL)
			if err != nil {
				t.Fatal(err)
			}

			c, err := NewClient(0, &ClientConfig{
				URL:     &config_util.URL{URL: serverURL},
				Timeout: model.Duration(time.Second),
			})
			if err != nil {
				t.Fatal(err)
			}

			err = c.Store(&monitoring.CreateTimeSeriesRequest{
				TimeSeries: []*monitoring.TimeSeries{
					&monitoring.TimeSeries{},
				},
			})
			if test.err != nil {
				rerr, recoverable := err.(recoverableError)
				if recoverable != test.recoverable {
					if test.recoverable {
						t.Errorf("expected recoverableError in error %v", err)
					} else {
						t.Errorf("unexpected recoverableError in error %v", err)
					}
				}
				if recoverable {
					err = rerr.error
				}
				gerr := err.(*googleapi.Error)
				if gerr.Code != test.code {
					t.Errorf("expected code %d in error %v", test.code, gerr)
				}
				if gerr.Body != longErrMessage+"\n" {
					t.Errorf("expected message %v in error %v", longErrMessage, gerr)
				}
			}

			server.Close()
		})
	}
}

func TestEmptyRequest(t *testing.T) {
	c, err := NewClient(0, &ClientConfig{
		Timeout: model.Duration(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}

	expected_error := "received empty request"
	err = c.Store(&monitoring.CreateTimeSeriesRequest{})
	if err.Error() != expected_error {
		t.Errorf("Unexpected error; want %v, got %v", expected_error, err)
	}
}
