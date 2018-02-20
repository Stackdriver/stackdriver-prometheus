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

func BenchmarkTranslate(b *testing.B) {
	b.ReportAllocs()
	r := ResourceMap{
		Type: "gke_container",
		LabelMap: map[string]string{
			ProjectIdLabel:                   "project_id",
			"_kubernetes_location":           "zone",
			"_kubernetes_cluster_name":       "cluster_name",
			"_kubernetes_namespace":          "namespace_id",
			"_kubernetes_pod_name":           "pod_id",
			"_kubernetes_pod_node_name":      "instance_id",
			"_kubernetes_pod_container_name": "container_name",
		},
	}
	metric := dto.Metric{
		Label: []*dto.LabelPair{
			makeLabelPair(ProjectIdLabel, "1:anoeuh oeusoeh uasoeuh"),
			makeLabelPair("_kubernetes_location", "2:anoeuh oeusoeh uasoeuh"),
			makeLabelPair("_kubernetes_cluster_name", "3:anoeuh oeusoeh uasoeuh"),
			makeLabelPair("_kubernetes_namespace", "4:anoeuh oeusoeh uasoeuh"),
			makeLabelPair("_kubernetes_pod_name", "5:anoeuh oeusoeh uasoeuh"),
			makeLabelPair("_kubernetes_pod_node_name", "6:anoeuh oeusoeh uasoeuh"),
			makeLabelPair("_kubernetes_pod_container_name", "7:anoeuh oeusoeh uasoeuh"),
			makeLabelPair("ignored", "8:anoeuh oeusoeh uasoeuh"),
		},
	}

	for i := 0; i < b.N; i++ {
		if labels := r.Translate(&metric); labels == nil {
			b.Fail()
		}
	}
}
