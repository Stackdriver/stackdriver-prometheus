# Stackdriver Prometheus server

This repository contains a variant of the Prometheus server that can send
metrics to Stackdriver. This software can support most Prometheus deployments
with minor changes to the Prometheus server configuration.

To install the Stackdriver Prometheus server see [Using Prometheus with Stackdriver Kubernetes Monitoring](https://cloud.google.com/monitoring/kubernetes-engine/prometheus). Stackdriver Kubernetes Monitoring is an add-on for Kubernetes that provides integrated monitoring and logging support with Stackdriver.

The long-term plan is to integrate with Prometheus server as a sidecar. We are
working on [a
design](https://docs.google.com/document/d/1TEqqE_Stq04drhjSU1I7Ctmuy0dpsvlPL1AKxqEQoSg/edit)
with the necessary changes to the Prometheus server within the Prometheus
community. Discussion happens in the document and in [this thread](https://groups.google.com/d/topic/prometheus-developers/BdhHaSP-qG0/discussion). Access to the design document may require membership on
[prometheus-developers@googlegroups.com](https://groups.google.com/forum/#!forum/prometheus-developers).

## Alternatives

Google develops **stackdriver-prometheus** primarily for Stackdriver users and gives support to Stackdriver users. We designed the user experience to meet the expectations of Prometheus users and to make it easy to run with Prometheus server. stackdriver-prometheus is intended to monitor all your applications, Kubernetes and beyond.

Google develops **prometheus-to-sd** primarily for Google Kubernetes Engine to collect metrics from system services in order to support Kubernetes users. We designed the tool to be lean when deployed as a sidecar in your pod. It's intended to support only the metrics the Kubernetes team at Google needs; you can use it to monitor your applications, but the user experience could be rough.

## Source Code Headers

Every file containing source code must include copyright and license
information. This includes any JS/CSS files that you might be serving out to
browsers. (This is to help well-intentioned people avoid accidental copying that
doesn't comply with the license.)

Apache header:

    Copyright 2018 Google Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
