package distributor

import (
	"context"
	"testing"

	"github.com/grafana/dskit/user"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/grafana/tempo/pkg/tempopb"
	v1_common "github.com/grafana/tempo/pkg/tempopb/common/v1"
)

// BenchmarkDistributorPushTracesRichAttributes prices the direct-rebatch path
// when every OTLP attribute container has nested map, array, and bytes values.
func BenchmarkDistributorPushTracesRichAttributes(b *testing.B) {
	const traceCount = 37
	benchmarkPushTracesGuard(b, benchmarkPushTracesRichAttributesInput(b, traceCount, 6), traceCount)
}

// BenchmarkDistributorPushTracesEntityRefs keeps the legacy fallback cost
// visible for Resource entity references that pdata does not expose publicly.
func BenchmarkDistributorPushTracesEntityRefs(b *testing.B) {
	const traceCount = 37
	benchmarkPushTracesGuard(b, benchmarkPushTracesEntityRefsInput(b, traceCount, 6), traceCount)
}

func benchmarkPushTracesGuard(b *testing.B, traces ptrace.Traces, traceCount int) {
	b.Helper()

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

func benchmarkPushTracesRichAttributesInput(tb testing.TB, traceCount, spansPerTrace int) ptrace.Traces {
	tb.Helper()

	traces := benchmarkPushTracesInput(traceCount, spansPerTrace)
	resourceSpans := traces.ResourceSpans()
	for resourceIndex := 0; resourceIndex < resourceSpans.Len(); resourceIndex++ {
		resourceSpan := resourceSpans.At(resourceIndex)
		addRichBenchmarkAttributes(resourceSpan.Resource().Attributes(), resourceIndex)

		scopeSpans := resourceSpan.ScopeSpans()
		for scopeIndex := 0; scopeIndex < scopeSpans.Len(); scopeIndex++ {
			scopeSpan := scopeSpans.At(scopeIndex)
			addRichBenchmarkAttributes(scopeSpan.Scope().Attributes(), scopeIndex)

			spans := scopeSpan.Spans()
			for spanIndex := 0; spanIndex < spans.Len(); spanIndex++ {
				span := spans.At(spanIndex)
				addRichBenchmarkAttributes(span.Attributes(), spanIndex)

				events := span.Events()
				for eventIndex := 0; eventIndex < events.Len(); eventIndex++ {
					addRichBenchmarkAttributes(events.At(eventIndex).Attributes(), eventIndex)
				}
				links := span.Links()
				for linkIndex := 0; linkIndex < links.Len(); linkIndex++ {
					addRichBenchmarkAttributes(links.At(linkIndex).Attributes(), linkIndex)
				}
			}
		}
	}
	return traces
}

func addRichBenchmarkAttributes(attributes pcommon.Map, seed int) {
	attributes.PutBool("benchmark.bool", seed%2 == 0)
	attributes.PutDouble("benchmark.double", float64(seed)+0.5)
	attributes.PutEmptyBytes("benchmark.bytes").FromRaw([]byte{byte(seed), byte(seed >> 8), 0x7f, 0x42})

	nested := attributes.PutEmptyMap("benchmark.map")
	nested.PutStr("nested.string", "nested benchmark value")
	nested.PutInt("nested.int", int64(seed))

	values := attributes.PutEmptySlice("benchmark.array")
	values.AppendEmpty().SetStr("array benchmark value")
	values.AppendEmpty().SetInt(int64(seed))
	values.AppendEmpty().SetEmptyMap().PutStr("nested.array.value", "nested")
}

func benchmarkPushTracesEntityRefsInput(tb testing.TB, traceCount, spansPerTrace int) ptrace.Traces {
	tb.Helper()

	traces := benchmarkPushTracesInput(traceCount, spansPerTrace)
	payload, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(traces)
	if err != nil {
		tb.Fatal(err)
	}

	trace := &tempopb.Trace{}
	if err := trace.Unmarshal(payload); err != nil {
		tb.Fatal(err)
	}
	trace.ResourceSpans[0].Resource.EntityRefs = []*v1_common.EntityRef{{
		Type:   "service",
		IdKeys: []string{"service.name"},
	}}
	payload, err = trace.Marshal()
	if err != nil {
		tb.Fatal(err)
	}

	traces, err = (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(payload)
	if err != nil {
		tb.Fatal(err)
	}
	return traces
}
