package distributor

import (
	"context"
	"encoding/binary"
	"flag"
	"testing"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	dslog "github.com/grafana/dskit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/grafana/tempo/modules/distributor/receiver"
	"github.com/grafana/tempo/modules/overrides"
	"github.com/grafana/tempo/pkg/tempopb"
	v1_common "github.com/grafana/tempo/pkg/tempopb/common/v1"
	v1_resource "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	v1 "github.com/grafana/tempo/pkg/tempopb/trace/v1"
)

const (
	benchmarkTraceCount    = 37
	benchmarkSpansPerTrace = 6
)

// BenchmarkPushTracesAcceptedNormalBatch measures the accepted distributor path
// for the existing normal-production-like shape of 37 trace IDs with six spans
// each. Setup, pdata decoding, and downstream transport are outside the timed
// region so the benchmark attributes work to PushTraces' ingress processing.
func BenchmarkPushTracesAcceptedNormalBatch(b *testing.B) {
	d := newPushTracesBenchmarkDistributor(b)
	traces := makePushTracesBenchmarkTraces(b)
	ctx := user.InjectOrgID(context.Background(), "benchmark")

	b.ReportAllocs()
	for b.Loop() {
		response, err := d.PushTraces(ctx, traces)
		if err != nil {
			b.Fatal(err)
		}
		if response != nil {
			b.Fatalf("PushTraces returned unexpected response: %#v", response)
		}
	}
}

// BenchmarkPdataToTempopbWireBridge keeps the production wire bridge isolated
// for CPU and allocation-profile attribution. It consumes the decoded graph so
// the marshaling and unmarshaling work cannot be optimized away.
func BenchmarkPdataToTempopbWireBridge(b *testing.B) {
	traces := makePushTracesBenchmarkTraces(b)
	marshaler := ptrace.ProtoMarshaler{}

	b.ReportAllocs()
	for b.Loop() {
		encoded, err := marshaler.MarshalTraces(traces)
		if err != nil {
			b.Fatal(err)
		}

		decoded := tempopb.Trace{}
		if err := decoded.Unmarshal(encoded); err != nil {
			b.Fatal(err)
		}
		if len(decoded.ResourceSpans) != 1 {
			b.Fatalf("expected one resource batch, got %d", len(decoded.ResourceSpans))
		}
	}
}

// TestPushTracesPreservesRebatchedDataAndPdataOwnership compares the produced
// rebatched graph with the established wire-compatible conversion and verifies
// that attribute truncation cannot mutate pdata subsequently sent to forwarders.
func TestPushTracesPreservesRebatchedDataAndPdataOwnership(t *testing.T) {
	traces := makeRichPushTracesInput(t)
	original := marshalPdataTraces(t, traces)

	expectedPdata, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(original)
	require.NoError(t, err)
	expected := tempopb.Trace{}
	encoded, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(expectedPdata)
	require.NoError(t, err)
	require.NoError(t, expected.Unmarshal(encoded))
	_, expectedRebatched, _, _, err := requestsByTraceID(expected.ResourceSpans, "test", expectedPdata.SpanCount(), 8)
	require.NoError(t, err)
	require.Len(t, expectedRebatched, 1)

	d := newPushTracesBenchmarkDistributor(t)
	d.cfg.MaxAttributeBytes = 8
	var pushed *tempopb.PushBytesRequest
	d.localPushTargets.LiveStore = func(_ context.Context, req *tempopb.PushBytesRequest) (*tempopb.PushResponse, error) {
		pushed = req
		return &tempopb.PushResponse{}, nil
	}

	response, err := d.PushTraces(user.InjectOrgID(context.Background(), "test"), traces)
	require.NoError(t, err)
	require.Nil(t, response)
	require.NotNil(t, pushed)
	require.Len(t, pushed.Traces, 1)

	actual := tempopb.Trace{}
	require.NoError(t, actual.Unmarshal(pushed.Traces[0].Slice))
	require.True(t, proto.Equal(expectedRebatched[0].trace, &actual))
	require.Equal(t, original, marshalPdataTraces(t, traces))
}

func newPushTracesBenchmarkDistributor(t testing.TB) *Distributor {
	t.Helper()

	limits := overrides.Config{}
	limits.RegisterFlagsAndApplyDefaults(flag.NewFlagSet("pushtraces-benchmark-limits", flag.ContinueOnError))
	limits.Defaults.Ingestion.RateLimitBytes = 1 << 30
	limits.Defaults.Ingestion.BurstSizeBytes = 1 << 30
	overridesSvc, err := overrides.NewOverrides(limits, nil, prometheus.NewRegistry())
	require.NoError(t, err)

	cfg := Config{}
	cfg.RegisterFlagsAndApplyDefaults("pushtraces-benchmark", flag.NewFlagSet("pushtraces-benchmark", flag.ContinueOnError))
	cfg.MaxAttributeBytes = 0

	loggingLevel := dslog.Level{}
	require.NoError(t, loggingLevel.Set("error"))
	d, err := New(
		cfg,
		LocalPushTargets{},
		nil,
		overridesSvc,
		receiver.MultiTenancyMiddleware(),
		log.NewNopLogger(),
		loggingLevel,
		prometheus.NewPedanticRegistry(),
	)
	require.NoError(t, err)

	return d
}

func makePushTracesBenchmarkTraces(t testing.TB) ptrace.Traces {
	t.Helper()

	spanCount := benchmarkTraceCount * benchmarkSpansPerTrace
	spans := make([]*v1.Span, 0, spanCount)
	var sequence uint64
	for traceNumber := 0; traceNumber < benchmarkTraceCount; traceNumber++ {
		traceID := make([]byte, 16)
		binary.BigEndian.PutUint64(traceID[8:], uint64(traceNumber+1))
		for spanNumber := 0; spanNumber < benchmarkSpansPerTrace; spanNumber++ {
			sequence++
			spanID := make([]byte, 8)
			binary.BigEndian.PutUint64(spanID, sequence)
			spans = append(spans, &v1.Span{
				TraceId:           traceID,
				SpanId:            spanID,
				Name:              "ingest",
				Kind:              v1.Span_SPAN_KIND_SERVER,
				StartTimeUnixNano: uint64(sequence) * 1_000_000,
				EndTimeUnixNano:   uint64(sequence+1) * 1_000_000,
				Attributes: []*v1_common.KeyValue{
					stringAttribute("http.method", "POST"),
					stringAttribute("service.version", "2026.07"),
				},
			})
		}
	}

	return pdataTracesFromTempo(t, &tempopb.Trace{ResourceSpans: []*v1.ResourceSpans{{
		Resource: &v1_resource.Resource{Attributes: []*v1_common.KeyValue{
			stringAttribute("service.name", "checkout"),
			stringAttribute("deployment.environment", "production"),
		}},
		ScopeSpans: []*v1.ScopeSpans{{
			Scope: &v1_common.InstrumentationScope{Name: "http", Version: "1.0.0"},
			Spans: spans,
		}},
	}}})
}

func makeRichPushTracesInput(t testing.TB) ptrace.Traces {
	t.Helper()

	traceID := []byte{0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f}
	spanID := []byte{0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27}
	parentSpanID := []byte{0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37}
	linkSpanID := []byte{0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47}

	trace := &tempopb.Trace{ResourceSpans: []*v1.ResourceSpans{{
		Resource: &v1_resource.Resource{
			Attributes: []*v1_common.KeyValue{
				stringAttribute("service.name", "checkout"),
				stringAttribute("resource-long", "0123456789abcdef"),
				{
					Key: "resource-nested",
					Value: &v1_common.AnyValue{Value: &v1_common.AnyValue_KvlistValue{KvlistValue: &v1_common.KeyValueList{
						Values: []*v1_common.KeyValue{stringAttribute("nested", "value")},
					}}},
				},
			},
			DroppedAttributesCount: 2,
		},
		SchemaUrl: "https://schema.example/resource/1.0",
		ScopeSpans: []*v1.ScopeSpans{{
			Scope: &v1_common.InstrumentationScope{
				Name:    "checkout/http",
				Version: "1.2.3",
				Attributes: []*v1_common.KeyValue{
					stringAttribute("scope-long", "abcdefghijklmnop"),
				},
				DroppedAttributesCount: 3,
			},
			SchemaUrl: "https://schema.example/scope/1.0",
			Spans: []*v1.Span{{
				TraceId:                traceID,
				SpanId:                 spanID,
				ParentSpanId:           parentSpanID,
				TraceState:             "vendor=value",
				Flags:                  0x201,
				Name:                   "checkout",
				Kind:                   v1.Span_SPAN_KIND_SERVER,
				StartTimeUnixNano:      1_000_000,
				EndTimeUnixNano:        2_000_000,
				DroppedAttributesCount: 4,
				DroppedEventsCount:     5,
				DroppedLinksCount:      6,
				Attributes: []*v1_common.KeyValue{
					stringAttribute("span-long", "qrstuvwxyzabcdef"),
					{
						Key: "span-array",
						Value: &v1_common.AnyValue{Value: &v1_common.AnyValue_ArrayValue{ArrayValue: &v1_common.ArrayValue{
							Values: []*v1_common.AnyValue{
								{Value: &v1_common.AnyValue_BoolValue{BoolValue: true}},
								{Value: &v1_common.AnyValue_IntValue{IntValue: 42}},
								{Value: &v1_common.AnyValue_BytesValue{BytesValue: []byte{1, 2, 3, 4}}},
							},
						}}},
					},
				},
				Events: []*v1.Span_Event{{
					TimeUnixNano:           1_500_000,
					Name:                   "validated",
					DroppedAttributesCount: 7,
					Attributes: []*v1_common.KeyValue{
						stringAttribute("event-long", "ghijklmnopqrstuv"),
					},
				}},
				Links: []*v1.Span_Link{{
					TraceId:                traceID,
					SpanId:                 linkSpanID,
					TraceState:             "linked=value",
					Flags:                  0x301,
					DroppedAttributesCount: 8,
					Attributes: []*v1_common.KeyValue{
						stringAttribute("link-long", "wxyzabcdefghijkl"),
					},
				}},
				Status: &v1.Status{Code: v1.Status_STATUS_CODE_ERROR, Message: "failed validation"},
			}},
		}},
	}}}

	return pdataTracesFromTempo(t, trace)
}

func pdataTracesFromTempo(t testing.TB, trace *tempopb.Trace) ptrace.Traces {
	t.Helper()

	encoded, err := trace.Marshal()
	require.NoError(t, err)
	traces, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(encoded)
	require.NoError(t, err)
	return traces
}

func marshalPdataTraces(t testing.TB, traces ptrace.Traces) []byte {
	t.Helper()

	encoded, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(traces)
	require.NoError(t, err)
	return encoded
}

func stringAttribute(key, value string) *v1_common.KeyValue {
	return &v1_common.KeyValue{
		Key: key,
		Value: &v1_common.AnyValue{Value: &v1_common.AnyValue_StringValue{
			StringValue: value,
		}},
	}
}
