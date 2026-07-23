package distributor

import (
	"bytes"
	"context"
	"encoding/hex"
	"flag"
	"reflect"
	"testing"

	"github.com/gogo/protobuf/proto"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/grafana/tempo/modules/overrides"
	"github.com/grafana/tempo/pkg/tempopb"
	v1_common "github.com/grafana/tempo/pkg/tempopb/common/v1"
	v1_resource "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	v1 "github.com/grafana/tempo/pkg/tempopb/trace/v1"
)

func TestRequestsByTraceIDFromPdataMatchesWireRebatch(t *testing.T) {
	source := pdataRebatchTestTrace()
	traces := pdataTracesFromTrace(t, source)

	encoded, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(traces)
	if err != nil {
		t.Fatal(err)
	}
	wireDetails := pdataRebatchWireDetailsFromPayload(encoded)
	if wireDetails.requiresLegacyRebatch {
		t.Fatal("test trace unexpectedly selected the legacy path")
	}
	legacyTrace := &tempopb.Trace{}
	if err := legacyTrace.Unmarshal(encoded); err != nil {
		t.Fatal(err)
	}

	const maxAttributeBytes = 4
	legacyTokens, legacyTraces, legacyTruncated, legacyExample, err := requestsByTraceID(legacyTrace.ResourceSpans, "pdata-rebatch-test", traces.SpanCount(), maxAttributeBytes)
	if err != nil {
		t.Fatal(err)
	}
	directTokens, directTraces, directTruncated, directExample, err := requestsByTraceIDFromPdata(traces, "pdata-rebatch-test", traces.SpanCount(), maxAttributeBytes)
	if err != nil {
		t.Fatal(err)
	}
	inputAfter, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(traces)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, inputAfter) {
		t.Fatal("direct rebatching mutated pdata")
	}

	if !reflect.DeepEqual(legacyTruncated, directTruncated) {
		t.Fatalf("truncated attributes differ: legacy=%+v direct=%+v", legacyTruncated, directTruncated)
	}
	if !reflect.DeepEqual(legacyExample, directExample) {
		t.Fatalf("truncation example differs: legacy=%+v direct=%+v", legacyExample, directExample)
	}

	legacyByID := rebatchedTracesByID(legacyTokens, legacyTraces)
	directByID := rebatchedTracesByID(directTokens, directTraces)
	if len(legacyByID) != len(directByID) {
		t.Fatalf("got %d direct traces, want %d", len(directByID), len(legacyByID))
	}
	for traceID, legacy := range legacyByID {
		direct, ok := directByID[traceID]
		if !ok {
			t.Fatalf("missing direct trace %s", traceID)
		}
		if legacy.token != direct.token || legacy.start != direct.start || legacy.end != direct.end || legacy.spanCount != direct.spanCount {
			t.Fatalf("metadata for trace %s differs: legacy=%+v direct=%+v", traceID, legacy, direct)
		}
		if !proto.Equal(legacy.trace, direct.trace) {
			t.Fatalf("trace %s differs after direct rebatching\nlegacy: %s\n direct: %s", traceID, legacy.trace, direct.trace)
		}
	}
}

func TestPdataRebatchWireDetailsSelectLegacy(t *testing.T) {
	t.Run("resource entity references", func(t *testing.T) {
		trace := pdataRebatchTestTrace()
		trace.ResourceSpans[0].Resource.EntityRefs = []*v1_common.EntityRef{{
			Type:   "service",
			IdKeys: []string{"service.name"},
		}}
		payload, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(pdataTracesFromTrace(t, trace))
		if err != nil {
			t.Fatal(err)
		}
		if !pdataRebatchWireDetailsFromPayload(payload).requiresLegacyRebatch {
			t.Fatal("resource entity references unexpectedly selected the direct path")
		}
	})

	t.Run("string-table fields", func(t *testing.T) {
		stringIndexTrace := pdataRebatchTestTrace()
		stringIndexTrace.ResourceSpans[0].Resource.Attributes = append(stringIndexTrace.ResourceSpans[0].Resource.Attributes, &v1_common.KeyValue{
			Key: "string-index",
			Value: &v1_common.AnyValue{Value: &v1_common.AnyValue_StringValueStrindex{
				StringValueStrindex: 1,
			}},
		})
		stringIndexPayload, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(pdataTracesFromTrace(t, stringIndexTrace))
		if err != nil {
			t.Fatal(err)
		}
		if !pdataRebatchWireDetailsFromPayload(stringIndexPayload).requiresLegacyRebatch {
			t.Fatal("string-table value unexpectedly selected the direct path")
		}

		keyIndexTrace := pdataRebatchTestTrace()
		keyIndexTrace.ResourceSpans[0].Resource.Attributes = append(keyIndexTrace.ResourceSpans[0].Resource.Attributes, &v1_common.KeyValue{
			Key:         "literal-key",
			KeyStrindex: 1,
			Value:       &v1_common.AnyValue{Value: &v1_common.AnyValue_StringValue{StringValue: "indexed key"}},
		})
		keyIndexTrace.ResourceSpans[0].Resource.Attributes = append(keyIndexTrace.ResourceSpans[0].Resource.Attributes, &v1_common.KeyValue{
			Key: "nested-key",
			Value: &v1_common.AnyValue{Value: &v1_common.AnyValue_KvlistValue{KvlistValue: &v1_common.KeyValueList{Values: []*v1_common.KeyValue{{
				Key:         "nested-literal-key",
				KeyStrindex: 2,
				Value:       &v1_common.AnyValue{Value: &v1_common.AnyValue_StringValue{StringValue: "nested indexed key"}},
			}}}}},
		})
		keyIndexPayload, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(pdataTracesFromTrace(t, keyIndexTrace))
		if err != nil {
			t.Fatal(err)
		}
		if !pdataRebatchWireDetailsFromPayload(keyIndexPayload).requiresLegacyRebatch {
			t.Fatal("literal or nested string-table key unexpectedly selected the direct path")
		}
	})
}

func TestPushTracesPdataRebatchPreservesDeliveredWireForms(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		source func() *tempopb.Trace
	}{
		{
			name:   "public values",
			source: pdataRebatchTestTrace,
		},
		{
			name: "resource entity references",
			source: func() *tempopb.Trace {
				trace := pdataRebatchTestTrace()
				trace.ResourceSpans[0].Resource.EntityRefs = []*v1_common.EntityRef{{
					Type:   "service",
					IdKeys: []string{"service.name"},
				}}
				return trace
			},
		},
		{
			name: "profiling key string index",
			source: func() *tempopb.Trace {
				trace := pdataRebatchTestTrace()
				trace.ResourceSpans[0].Resource.Attributes = append(trace.ResourceSpans[0].Resource.Attributes, &v1_common.KeyValue{
					Key:         "literal-key",
					KeyStrindex: 1,
					Value:       &v1_common.AnyValue{Value: &v1_common.AnyValue_StringValue{StringValue: "indexed key"}},
				})
				return trace
			},
		},
		{
			name: "profiling value string index",
			source: func() *tempopb.Trace {
				trace := pdataRebatchTestTrace()
				trace.ResourceSpans[0].Resource.Attributes = append(trace.ResourceSpans[0].Resource.Attributes, &v1_common.KeyValue{
					Key:   "string-index",
					Value: &v1_common.AnyValue{Value: &v1_common.AnyValue_StringValueStrindex{StringValueStrindex: 1}},
				})
				return trace
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			limits := overrides.Config{}
			limits.RegisterFlagsAndApplyDefaults(&flag.FlagSet{})
			d := prepare(t, limits, nil)

			var delivered *tempopb.PushBytesRequest
			d.localPushTargets.LiveStore = func(_ context.Context, request *tempopb.PushBytesRequest) (*tempopb.PushResponse, error) {
				delivered = request
				return &tempopb.PushResponse{}, nil
			}

			traces := pdataTracesFromTrace(t, testCase.source())
			expected := legacyDeliveredPdataTraces(t, traces, "test", d.cfg.MaxAttributeBytes)
			if _, err := d.PushTraces(ctx, traces); err != nil {
				t.Fatal(err)
			}
			if delivered == nil {
				t.Fatal("local live-store did not receive routed fragments")
			}
			if len(delivered.Traces) != len(expected) || len(delivered.Ids) != len(expected) {
				t.Fatalf("got %d fragments and %d IDs, want %d", len(delivered.Traces), len(delivered.Ids), len(expected))
			}
			for index, encodedTrace := range delivered.Traces {
				traceID := hex.EncodeToString(delivered.Ids[index])
				want, ok := expected[traceID]
				if !ok {
					t.Fatalf("unexpected routed trace %s", traceID)
				}
				got := &tempopb.Trace{}
				if err := got.Unmarshal(encodedTrace.Slice); err != nil {
					t.Fatal(err)
				}
				if !proto.Equal(want, got) {
					t.Fatalf("routed trace %s differs from the legacy payload\nwant: %s\n got: %s", traceID, want, got)
				}
				delete(expected, traceID)
			}
			if len(expected) != 0 {
				t.Fatalf("missing routed traces: %v", expected)
			}
		})
	}
}

func legacyDeliveredPdataTraces(t *testing.T, traces ptrace.Traces, userID string, maxAttributeBytes int) map[string]*tempopb.Trace {
	t.Helper()
	payload, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(traces)
	if err != nil {
		t.Fatal(err)
	}
	legacy := &tempopb.Trace{}
	if err := legacy.Unmarshal(payload); err != nil {
		t.Fatal(err)
	}
	_, rebatched, _, _, err := requestsByTraceID(legacy.ResourceSpans, userID, traces.SpanCount(), maxAttributeBytes)
	if err != nil {
		t.Fatal(err)
	}
	result := make(map[string]*tempopb.Trace, len(rebatched))
	for _, trace := range rebatched {
		result[hex.EncodeToString(trace.id)] = trace.trace
	}
	return result
}

type rebatchedTraceInfo struct {
	token     uint32
	trace     *tempopb.Trace
	start     uint32
	end       uint32
	spanCount int
}

func rebatchedTracesByID(tokens []uint32, traces []*rebatchedTrace) map[string]rebatchedTraceInfo {
	result := make(map[string]rebatchedTraceInfo, len(traces))
	for index, trace := range traces {
		result[hex.EncodeToString(trace.id)] = rebatchedTraceInfo{
			token:     tokens[index],
			trace:     trace.trace,
			start:     trace.start,
			end:       trace.end,
			spanCount: trace.spanCount,
		}
	}
	return result
}

func pdataTracesFromTrace(t *testing.T, trace *tempopb.Trace) ptrace.Traces {
	t.Helper()
	encoded, err := trace.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	traces, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(encoded)
	if err != nil {
		t.Fatal(err)
	}
	return traces
}

func pdataRebatchTestTrace() *tempopb.Trace {
	traceIDA := pdataRebatchTraceID(1)
	traceIDB := pdataRebatchTraceID(2)

	return &tempopb.Trace{ResourceSpans: []*v1.ResourceSpans{{
		Resource: &v1_resource.Resource{
			Attributes: []*v1_common.KeyValue{
				pdataRebatchStringAttribute("service.name", "checkout"),
				pdataRebatchBoolAttribute("resource.bool", true),
				pdataRebatchDoubleAttribute("resource.double", 3.5),
				pdataRebatchBytesAttribute("resource.bytes", []byte("resource bytes")),
				pdataRebatchMapAttribute("resource.map"),
				pdataRebatchArrayAttribute("resource.array"),
			},
			DroppedAttributesCount: 2,
		},
		ScopeSpans: []*v1.ScopeSpans{{
			Scope: &v1_common.InstrumentationScope{
				Name:    "test-library",
				Version: "1.2.3",
				Attributes: []*v1_common.KeyValue{
					pdataRebatchStringAttribute("scope.string", "scope value"),
					pdataRebatchArrayAttribute("scope.array"),
				},
				DroppedAttributesCount: 3,
			},
			Spans: []*v1.Span{
				pdataRebatchSpan(traceIDA, pdataRebatchSpanID(1), "first"),
				pdataRebatchSpan(traceIDB, pdataRebatchSpanID(2), "second"),
			},
		}},
	}}}
}

func pdataRebatchSpan(traceID, spanID []byte, name string) *v1.Span {
	return &v1.Span{
		TraceId:                traceID,
		SpanId:                 spanID,
		ParentSpanId:           pdataRebatchSpanID(99),
		TraceState:             "vendor=state",
		Flags:                  0x101,
		Name:                   name,
		Kind:                   v1.Span_SPAN_KIND_SERVER,
		StartTimeUnixNano:      10_000_000_000,
		EndTimeUnixNano:        20_000_000_000,
		Attributes:             []*v1_common.KeyValue{pdataRebatchStringAttribute("span.string", "span value"), pdataRebatchBytesAttribute("span.bytes", []byte("span bytes")), pdataRebatchMapAttribute("span.map")},
		DroppedAttributesCount: 4,
		Events: []*v1.Span_Event{{
			TimeUnixNano:           15_000_000_000,
			Name:                   "event",
			Attributes:             []*v1_common.KeyValue{pdataRebatchBoolAttribute("event.bool", true), pdataRebatchArrayAttribute("event.array")},
			DroppedAttributesCount: 5,
		}},
		DroppedEventsCount: 6,
		Links: []*v1.Span_Link{{
			TraceId:                pdataRebatchTraceID(99),
			SpanId:                 pdataRebatchSpanID(100),
			TraceState:             "vendor=linked",
			Attributes:             []*v1_common.KeyValue{pdataRebatchDoubleAttribute("link.double", 2.5), pdataRebatchBytesAttribute("link.bytes", []byte("link bytes"))},
			DroppedAttributesCount: 7,
			Flags:                  0x201,
		}},
		DroppedLinksCount: 8,
		Status:            &v1.Status{Message: "failed", Code: v1.Status_STATUS_CODE_ERROR},
	}
}

func pdataRebatchStringAttribute(key, value string) *v1_common.KeyValue {
	return &v1_common.KeyValue{Key: key, Value: &v1_common.AnyValue{Value: &v1_common.AnyValue_StringValue{StringValue: value}}}
}

func pdataRebatchBoolAttribute(key string, value bool) *v1_common.KeyValue {
	return &v1_common.KeyValue{Key: key, Value: &v1_common.AnyValue{Value: &v1_common.AnyValue_BoolValue{BoolValue: value}}}
}

func pdataRebatchDoubleAttribute(key string, value float64) *v1_common.KeyValue {
	return &v1_common.KeyValue{Key: key, Value: &v1_common.AnyValue{Value: &v1_common.AnyValue_DoubleValue{DoubleValue: value}}}
}

func pdataRebatchBytesAttribute(key string, value []byte) *v1_common.KeyValue {
	return &v1_common.KeyValue{Key: key, Value: &v1_common.AnyValue{Value: &v1_common.AnyValue_BytesValue{BytesValue: value}}}
}

func pdataRebatchMapAttribute(key string) *v1_common.KeyValue {
	return &v1_common.KeyValue{Key: key, Value: &v1_common.AnyValue{Value: &v1_common.AnyValue_KvlistValue{KvlistValue: &v1_common.KeyValueList{Values: []*v1_common.KeyValue{
		pdataRebatchStringAttribute("nested.string", "nested value"),
		pdataRebatchBytesAttribute("nested.bytes", []byte("nested bytes")),
	}}}}}
}

func pdataRebatchArrayAttribute(key string) *v1_common.KeyValue {
	return &v1_common.KeyValue{Key: key, Value: &v1_common.AnyValue{Value: &v1_common.AnyValue_ArrayValue{ArrayValue: &v1_common.ArrayValue{Values: []*v1_common.AnyValue{
		{Value: &v1_common.AnyValue_StringValue{StringValue: "array value"}},
		{Value: &v1_common.AnyValue_IntValue{IntValue: 42}},
		{Value: &v1_common.AnyValue_KvlistValue{KvlistValue: &v1_common.KeyValueList{Values: []*v1_common.KeyValue{pdataRebatchBoolAttribute("nested.bool", true)}}}},
	}}}}}
}

func pdataRebatchTraceID(value byte) []byte {
	result := make([]byte, 16)
	result[15] = value
	return result
}

func pdataRebatchSpanID(value byte) []byte {
	result := make([]byte, 8)
	result[7] = value
	return result
}
