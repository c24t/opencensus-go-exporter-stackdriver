// Copyright 2018, OpenCensus Authors
//
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

package stackdriver

import (
	"context"
	"errors"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"contrib.go.opencensus.io/exporter/ocagent"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"google.golang.org/grpc"

	agentmetricspb "github.com/census-instrumentation/opencensus-proto/gen-go/agent/metrics/v1"
)

func TestStatsAndMetricsEquivalence(t *testing.T) {
	ma, addr, stop := createMockAgent(t)
	defer stop()

	oce, err := ocagent.NewExporter(ocagent.WithInsecure(),
		ocagent.WithAddress(addr),
		ocagent.WithReconnectionPeriod(1*time.Millisecond))
	if err != nil {
		t.Fatalf("Failed to create the ocagent exporter: %v", err)
	}
	time.Sleep(5 * time.Millisecond)

	startTime := time.Date(2018, 11, 25, 15, 38, 18, 997, time.UTC)
	endTime := startTime.Add(100 * time.Millisecond)
	mLatencyMs := stats.Float64("latency", "The latency for various methods", "ms")

	var vdl []*view.Data
	for i := 0; i < 100; i++ {
		vdl = append(vdl, &view.Data{
			Start: startTime,
			End:   endTime,
			View: &view.View{
				Name:        "ocagent.io/latency",
				Description: "The latency of the various methods",
				Aggregation: view.Distribution(0, 100, 200, 500, 800, 1000),
				Measure:     mLatencyMs,
			},
			Rows: []*view.Row{
				{
					Data: &view.CountData{Value: 4},
				},
			},
		})
	}

	for i, vd := range vdl {
		oce.ExportView(vd)
		oce.Flush()

		time.Sleep(30 * time.Millisecond)
		oce.Flush()

		var last *agentmetricspb.ExportMetricsServiceRequest
		ma.forEachRequest(func(emr *agentmetricspb.ExportMetricsServiceRequest) {
			last = emr
		})

		if last == nil || len(last.Metrics) == 0 {
			t.Errorf("#%d: Failed to retrieve any metrics", i)
			continue
		}

		se := &statsExporter{
			o: Options{ProjectID: "equivalence"},
		}

		ctx := context.Background()
		sMD, err := se.viewToMetricDescriptor(ctx, vd.View)
		if err != nil {
			t.Errorf("#%d: Stats.viewToMetricDescriptor: %v", i, err)
		}
		pMD, err := se.protoMetricDescriptorToCreateMetricDescriptorRequest(ctx, last.Metrics[0])
		if err != nil {
			t.Errorf("#%d: Stats.protoMetricDescriptorToMetricDescriptor: %v", i, err)
		}
		if !reflect.DeepEqual(sMD, pMD) {
			t.Errorf("MetricDescriptor Mismatch\nStats MetricDescriptor:\n\t%v\nProto MetricDescriptor:\n\t%v\n", sMD, pMD)
		}

		vdl := []*view.Data{vd}
		sctreql := se.makeReq(vdl, maxTimeSeriesPerUpload)
		pctreql, _ := se.protoMetricToCreateTimeSeriesRequest(ctx, last.Node, last.Resource, last.Metrics[0], maxTimeSeriesPerUpload)
		if !reflect.DeepEqual(sctreql, pctreql) {
			t.Errorf("#%d: TimeSeries Mismatch\nStats MetricDescriptor:\n\t%v\nProto MetricDescriptor:\n\t%v\n", i, sctreql, pctreql)
		}
	}
}

type metricsAgent struct {
	mu      sync.RWMutex
	metrics []*agentmetricspb.ExportMetricsServiceRequest
}

func createMockAgent(t *testing.T) (*metricsAgent, string, func()) {
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Failed to bind to an available address: %v", err)
	}
	ma := new(metricsAgent)
	srv := grpc.NewServer()
	agentmetricspb.RegisterMetricsServiceServer(srv, ma)
	go func() {
		_ = srv.Serve(ln)
	}()
	stop := func() {
		srv.Stop()
		_ = ln.Close()
	}
	_, agentPortStr, _ := net.SplitHostPort(ln.Addr().String())
	return ma, ":" + agentPortStr, stop
}

func (ma *metricsAgent) Export(mes agentmetricspb.MetricsService_ExportServer) error {
	// Expecting the first message to contain the Node information
	firstMetric, err := mes.Recv()
	if err != nil {
		return err
	}

	if firstMetric == nil || firstMetric.Node == nil {
		return errors.New("Expecting a non-nil Node in the first message")
	}

	ma.addMetric(firstMetric)

	for {
		msg, err := mes.Recv()
		if err != nil {
			return err
		}
		ma.addMetric(msg)
	}
}

func (ma *metricsAgent) addMetric(metric *agentmetricspb.ExportMetricsServiceRequest) {
	ma.mu.Lock()
	ma.metrics = append(ma.metrics, metric)
	ma.mu.Unlock()
}

func (ma *metricsAgent) forEachRequest(fn func(*agentmetricspb.ExportMetricsServiceRequest)) {
	ma.mu.RLock()
	defer ma.mu.RUnlock()

	for _, req := range ma.metrics {
		fn(req)
	}
}
