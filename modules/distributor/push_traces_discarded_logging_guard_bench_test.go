package distributor

import (
	"context"
	"testing"

	"github.com/grafana/dskit/user"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/grafana/tempo/pkg/tempopb"
)

// BenchmarkDistributorPushTracesDiscardedLogging keeps the successful-ingest
// cost visible when discarded-span logging is enabled. The setting should not
// force a request-wide decode unless validation rejects the request.
func BenchmarkDistributorPushTracesDiscardedLogging(b *testing.B) {
	const traceCount = 37
	traces := benchmarkPushTracesInput(traceCount, 6)
	inputBytes := (&ptrace.ProtoMarshaler{}).TracesSize(traces)

	var deliveredFragments, deliveredBytes int
	d := newPushTracesBenchmarkDistributor(b, benchmarkMaxAttributeBytes, LocalPushTargets{
		LiveStore: func(_ context.Context, req *tempopb.PushBytesRequest) (*tempopb.PushResponse, error) {
			deliveredFragments += len(req.Traces)
			for _, trace := range req.Traces {
				deliveredBytes += len(trace.Slice)
			}
			return &tempopb.PushResponse{}, nil
		},
	})
	d.cfg.LogDiscardedSpans.Enabled = true

	ctx := user.InjectOrgID(context.Background(), "benchmark-tenant")
	b.SetBytes(int64(inputBytes))
	b.ReportAllocs()

	iterations := 0
	for b.Loop() {
		response, err := d.PushTraces(ctx, traces)
		if err != nil {
			b.Fatal(err)
		}
		if response != nil {
			b.Fatalf("unexpected PushTraces response: %#v", response)
		}
		iterations++
	}

	if deliveredFragments != iterations*traceCount {
		b.Fatalf("delivered %d fragments after %d iterations, want %d", deliveredFragments, iterations, iterations*traceCount)
	}
	if deliveredBytes == 0 {
		b.Fatal("local live-store did not consume encoded trace bytes")
	}
}
