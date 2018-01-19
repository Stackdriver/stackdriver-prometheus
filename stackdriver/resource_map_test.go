/*
Copyright 2017 Google Inc.

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
	"reflect"
	"testing"

	"github.com/gogo/protobuf/proto"
	dto "github.com/prometheus/client_model/go"
)

func TestTranslate(t *testing.T) {
	r := ResourceMap{
		Type: "my_type",
		LabelMap: map[string]string{
			"kube1": "sd1",
			"kube2": "sd2",
		},
	}
	// This metric is missing label "kube1".
	noMatchMetric := dto.Metric{
		Label: []*dto.LabelPair{
			makeLabelPair("ignored", "a"),
			makeLabelPair("kube2", "b"),
		},
	}
	if labels := r.Translate(&noMatchMetric); labels != nil {
		t.Errorf("Expected no match, matched %v", labels)
	}
	matchMetric := dto.Metric{
		Label: []*dto.LabelPair{
			makeLabelPair("ignored", "a"),
			makeLabelPair("kube2", "b"),
			makeLabelPair("kube1", "c"),
		},
	}

	expectedLabels := map[string]string{
		"sd1": "c",
		"sd2": "b",
	}
	if labels := r.Translate(&matchMetric); labels == nil {
		t.Errorf("Expected %v, actual nil", expectedLabels)
	} else if !reflect.DeepEqual(labels, expectedLabels) {
		t.Errorf("Expected %v, actual %v", expectedLabels, labels)
	}
}

func makeLabelPair(name, value string) *dto.LabelPair {
	return &dto.LabelPair{Name: proto.String(name), Value: proto.String(value)}
}
