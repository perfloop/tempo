package distributor

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"strings"
	"testing"

	kitlog "github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	dslog "github.com/grafana/dskit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/grafana/tempo/modules/distributor/receiver"
	"github.com/grafana/tempo/modules/overrides"
	"github.com/grafana/tempo/pkg/tempopb"
	v1_common "github.com/grafana/tempo/pkg/tempopb/common/v1"
	v1_resource "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	v1 "github.com/grafana/tempo/pkg/tempopb/trace/v1"
)

const (
	pushTracesBenchmarkTraceCount    = 37
	pushTracesBenchmarkSpansPerTrace = 6
	pushTracesBenchmarkRateLimit     = 1 << 50
)

// BenchmarkPushTracesAcceptedNormalBatch measures one accepted OTLP push with
// the normal production-like trace and span cardinality used by the rebatching
// benchmark. The two inputs have the same shape but different runtime data so
// the conversion work cannot be specialized to a compile-time constant.
func BenchmarkPushTracesAcceptedNormalBatch(b *testing.B) {
	d := newPushTracesConversionHarnessDistributor(b, 0, LocalPushTargets{})
	requestCtx := user.InjectOrgID(context.Background(), "benchmark")
	inputs := []ptrace.Traces{
		makePushTracesConversionHarnessTraces(b, pushTracesBenchmarkTraceCount, pushTracesBenchmarkSpansPerTrace, 0, false),
		makePushTracesConversionHarnessTraces(b, pushTracesBenchmarkTraceCount, pushTracesBenchmarkSpansPerTrace, 1, false),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		response, err := d.PushTraces(requestCtx, inputs[i&1])
		if err != nil {
			b.Fatal(err)
		}
		if response != nil {
			b.Fatal("PushTraces returned an unexpected response")
		}
	}
}

func TestPushTracesPreservesForwardableInputAndRebatchedOutput(t *testing.T) {
	const maxAttributeBytes = 64

	traces := makePushTracesConversionHarnessTraces(t, 1, 2, 0, true)
	expected := referenceRebatchedTrace(t, traces, maxAttributeBytes)

	var captured *tempopb.PushBytesRequest
	d := newPushTracesConversionHarnessDistributor(t, maxAttributeBytes, LocalPushTargets{
		LiveStore: func(_ context.Context, req *tempopb.PushBytesRequest) (*tempopb.PushResponse, error) {
			captured = req
			return &tempopb.PushResponse{}, nil
		},
	})

	response, err := d.PushTraces(user.InjectOrgID(context.Background(), "test"), traces)
	if err != nil {
		t.Fatal(err)
	}
	if response != nil {
		t.Fatal("PushTraces returned an unexpected response")
	}

	if captured == nil || len(captured.Traces) != 1 {
		t.Fatalf("expected one rebatched live-store trace, got %#v", captured)
	}

	var actual tempopb.Trace
	if err := actual.Unmarshal(captured.Traces[0].Slice); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(expected, &actual) {
		t.Fatalf("rebatched trace differs from the wire-compatible reference\nexpected: %s\nactual:   %s", expected, &actual)
	}

	span := traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	value, ok := span.Attributes().Get("forwardable.large")
	if !ok {
		t.Fatal("forwardable input is missing the oversized attribute")
	}
	if got, want := value.Str(), strings.Repeat("x", maxAttributeBytes+1); got != want {
		t.Fatalf("forwardable input was mutated by rebatching: got %d bytes, want %d", len(got), len(want))
	}
}

func newPushTracesConversionHarnessDistributor(tb testing.TB, maxAttributeBytes int, localPushTargets LocalPushTargets) *Distributor {
	tb.Helper()

	limits := overrides.Config{}
	limits.RegisterFlagsAndApplyDefaults(flag.NewFlagSet("pushtraces-conversion", flag.ContinueOnError))
	limits.Defaults.Ingestion.RateLimitBytes = pushTracesBenchmarkRateLimit
	limits.Defaults.Ingestion.BurstSizeBytes = pushTracesBenchmarkRateLimit

	overridesSvc, err := overrides.NewOverrides(limits, nil, prometheus.NewRegistry())
	if err != nil {
		tb.Fatal(err)
	}

	loggingLevel := dslog.Level{}
	if err := loggingLevel.Set("error"); err != nil {
		tb.Fatal(err)
	}

	d, err := New(
		Config{MaxAttributeBytes: maxAttributeBytes},
		localPushTargets,
		nil,
		overridesSvc,
		receiver.MultiTenancyMiddleware(),
		kitlog.NewNopLogger(),
		loggingLevel,
		prometheus.NewRegistry(),
	)
	if err != nil {
		tb.Fatal(err)
	}
	return d
}

func makePushTracesConversionHarnessTraces(tb testing.TB, traceCount, spansPerTrace, variant int, includeOversizedAttribute bool) ptrace.Traces {
	tb.Helper()

	resourceSpans := &v1.ResourceSpans{
		Resource: &v1_resource.Resource{
			Attributes: []*v1_common.KeyValue{
				stringKeyValue("service.name", "pushtraces-benchmark"),
				stringKeyValue("deployment.environment", fmt.Sprintf("benchmark-%d", variant)),
				{
					Key: "resource.nested",
					Value: &v1_common.AnyValue{Value: &v1_common.AnyValue_KvlistValue{
						KvlistValue: &v1_common.KeyValueList{Values: []*v1_common.KeyValue{
							stringKeyValue("region", "test"),
							{Key: "replica", Value: &v1_common.AnyValue{Value: &v1_common.AnyValue_IntValue{IntValue: int64(variant + 1)}}},
						}},
					}},
				},
			},
			DroppedAttributesCount: 2,
		},
		SchemaUrl: "https://opentelemetry.io/schemas/1.26.0",
		ScopeSpans: []*v1.ScopeSpans{
			{
				Scope: &v1_common.InstrumentationScope{
					Name:    "pushtraces-benchmark",
					Version: "1.0.0",
					Attributes: []*v1_common.KeyValue{
						stringKeyValue("scope.attribute", "present"),
					},
					DroppedAttributesCount: 1,
				},
				SchemaUrl: "https://opentelemetry.io/schemas/1.26.0",
			},
		},
	}

	spans := make([]*v1.Span, 0, traceCount*spansPerTrace)
	var spanSequence uint64
	for traceIndex := 0; traceIndex < traceCount; traceIndex++ {
		traceID := make([]byte, 16)
		binary.BigEndian.PutUint64(traceID[8:], uint64(traceIndex+1+variant*traceCount))

		for spanIndex := 0; spanIndex < spansPerTrace; spanIndex++ {
			spanSequence++
			spanID := make([]byte, 8)
			binary.BigEndian.PutUint64(spanID, spanSequence)

			attributes := []*v1_common.KeyValue{
				stringKeyValue("http.route", fmt.Sprintf("/resource/%d", traceIndex%7)),
				{Key: "retry.count", Value: &v1_common.AnyValue{Value: &v1_common.AnyValue_IntValue{IntValue: int64(spanIndex)}}},
				{Key: "cache.hit", Value: &v1_common.AnyValue{Value: &v1_common.AnyValue_BoolValue{BoolValue: spanIndex%2 == 0}}},
				{Key: "load.factor", Value: &v1_common.AnyValue{Value: &v1_common.AnyValue_DoubleValue{DoubleValue: float64(traceIndex) + 0.5}}},
				{Key: "payload.bytes", Value: &v1_common.AnyValue{Value: &v1_common.AnyValue_BytesValue{BytesValue: []byte(strings.Repeat("payload", 16))}}},
				{
					Key: "payload.array",
					Value: &v1_common.AnyValue{Value: &v1_common.AnyValue_ArrayValue{
						ArrayValue: &v1_common.ArrayValue{Values: []*v1_common.AnyValue{
							{Value: &v1_common.AnyValue_StringValue{StringValue: "first"}},
							{Value: &v1_common.AnyValue_IntValue{IntValue: int64(variant)}},
						}},
					}},
				},
			}
			if includeOversizedAttribute && traceIndex == 0 && spanIndex == 0 {
				attributes = append(attributes, stringKeyValue("forwardable.large", strings.Repeat("x", 65)))
			}

			span := &v1.Span{
				TraceId:                traceID,
				SpanId:                 spanID,
				TraceState:             "vendor=value",
				Flags:                  0x101,
				Name:                   fmt.Sprintf("operation-%d", spanIndex%4),
				Kind:                   v1.Span_SPAN_KIND_SERVER,
				StartTimeUnixNano:      uint64(1_700_000_000_000_000_000 + spanSequence*1_000),
				EndTimeUnixNano:        uint64(1_700_000_000_000_000_000 + spanSequence*1_000 + 750),
				Attributes:             attributes,
				DroppedAttributesCount: 3,
				DroppedEventsCount:     4,
				DroppedLinksCount:      5,
				Events: []*v1.Span_Event{
					{
						TimeUnixNano:           uint64(1_700_000_000_000_000_000 + spanSequence*1_000 + 100),
						Name:                   "event",
						Attributes:             []*v1_common.KeyValue{stringKeyValue("event.attribute", "event-value")},
						DroppedAttributesCount: 6,
					},
				},
				Links: []*v1.Span_Link{
					{
						TraceId:                append([]byte(nil), traceID...),
						SpanId:                 append([]byte(nil), spanID...),
						TraceState:             "link=value",
						Attributes:             []*v1_common.KeyValue{stringKeyValue("link.attribute", "link-value")},
						DroppedAttributesCount: 7,
						Flags:                  0x100,
					},
				},
				Status: &v1.Status{Code: v1.Status_STATUS_CODE_ERROR, Message: "status-message"},
			}
			if spanIndex > 0 {
				parentSpanID := make([]byte, 8)
				binary.BigEndian.PutUint64(parentSpanID, spanSequence-1)
				span.ParentSpanId = parentSpanID
			}
			spans = append(spans, span)
		}
	}
	resourceSpans.ScopeSpans[0].Spans = spans

	wireTrace := tempopb.Trace{ResourceSpans: []*v1.ResourceSpans{resourceSpans}}
	wire, err := wireTrace.Marshal()
	if err != nil {
		tb.Fatal(err)
	}
	traces, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(wire)
	if err != nil {
		tb.Fatal(err)
	}
	return traces
}

func referenceRebatchedTrace(tb testing.TB, traces ptrace.Traces, maxAttributeBytes int) *tempopb.Trace {
	tb.Helper()

	wire, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(traces)
	if err != nil {
		tb.Fatal(err)
	}

	var trace tempopb.Trace
	if err := trace.Unmarshal(wire); err != nil {
		tb.Fatal(err)
	}

	_, rebatched, _, _, err := requestsByTraceID(trace.ResourceSpans, "test", traces.SpanCount(), maxAttributeBytes)
	if err != nil {
		tb.Fatal(err)
	}
	if len(rebatched) != 1 {
		tb.Fatalf("expected one rebatched trace, got %d", len(rebatched))
	}
	return rebatched[0].trace
}

func stringKeyValue(key, value string) *v1_common.KeyValue {
	return &v1_common.KeyValue{
		Key: key,
		Value: &v1_common.AnyValue{Value: &v1_common.AnyValue_StringValue{
			StringValue: value,
		}},
	}
}
