package distributor

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"testing"
	"time"

	kitlog "github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	dslog "github.com/grafana/dskit/log"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/grafana/tempo/modules/distributor/receiver"
	"github.com/grafana/tempo/modules/overrides"
	"github.com/grafana/tempo/pkg/tempopb"
	v1_common "github.com/grafana/tempo/pkg/tempopb/common/v1"
	v1_resource "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	v1 "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	"github.com/grafana/tempo/pkg/util/listtomap"
)

const benchmarkMaxAttributeBytes = 2048

// BenchmarkDistributorPushTraces measures the normal ingestion shape documented
// by the distributor rebatching benchmark: 37 trace IDs with 6 spans each. The
// payload includes resource, scope, span, event, and link fields; SetBytes reports
// its encoded OTLP size so throughput stays tied to the fixture's payload size.
func BenchmarkDistributorPushTraces(b *testing.B) {
	benchmarkPushTraces(b, 37, 6)
}

// BenchmarkDistributorPushTracesHighCardinality keeps the one-fragment-per-trace
// allocation risk visible for a batch with many independently routed trace IDs.
func BenchmarkDistributorPushTracesHighCardinality(b *testing.B) {
	benchmarkPushTraces(b, 5000, 1)
}

func benchmarkPushTraces(b *testing.B, traceCount, spansPerTrace int) {
	b.Helper()

	traces := benchmarkPushTracesInput(traceCount, spansPerTrace)
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

func TestPushTracesPreservesRoutedFragments(t *testing.T) {
	const maxAttributeBytes = 8

	traceIDA := benchmarkTraceID(1)
	traceIDB := benchmarkTraceID(2)
	source := &tempopb.Trace{
		ResourceSpans: []*v1.ResourceSpans{
			{
				Resource: &v1_resource.Resource{
					Attributes: []*v1_common.KeyValue{
						stringAttribute("resource-attribute", "resource-value-that-is-truncated"),
						intAttribute("resource.int", 42),
					},
					DroppedAttributesCount: 3,
				},
				ScopeSpans: []*v1.ScopeSpans{
					{
						Scope: &v1_common.InstrumentationScope{
							Name:    "test-library",
							Version: "1.2.3",
							Attributes: []*v1_common.KeyValue{
								stringAttribute("scope-attribute", "scope-value-that-is-truncated"),
							},
							DroppedAttributesCount: 2,
						},
						Spans: []*v1.Span{
							routedFragmentSpan(traceIDA, benchmarkSpanID(1), "first-span"),
							routedFragmentSpan(traceIDB, benchmarkSpanID(2), "second-span"),
						},
						SchemaUrl: "https://example.test/scope-schema",
					},
				},
				SchemaUrl: "https://example.test/resource-schema",
			},
		},
	}

	encoded, err := source.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	traces, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(encoded)
	if err != nil {
		t.Fatal(err)
	}
	before, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(traces)
	if err != nil {
		t.Fatal(err)
	}

	var delivered *tempopb.PushBytesRequest
	d := newPushTracesBenchmarkDistributor(t, maxAttributeBytes, LocalPushTargets{
		LiveStore: func(_ context.Context, req *tempopb.PushBytesRequest) (*tempopb.PushResponse, error) {
			delivered = req
			return &tempopb.PushResponse{}, nil
		},
	})
	response, err := d.PushTraces(user.InjectOrgID(context.Background(), "fragment-test"), traces)
	if err != nil {
		t.Fatal(err)
	}
	if response != nil {
		t.Fatalf("unexpected PushTraces response: %#v", response)
	}
	after, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(traces)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("PushTraces mutated the pdata input that forwarding middleware observes")
	}
	if delivered == nil {
		t.Fatal("local live-store was not called")
	}
	if len(delivered.Traces) != 2 || len(delivered.Ids) != 2 {
		t.Fatalf("got %d traces and %d IDs, want two routed fragments", len(delivered.Traces), len(delivered.Ids))
	}

	resource := source.ResourceSpans[0].Resource
	scope := source.ResourceSpans[0].ScopeSpans[0].Scope
	expected := map[string]*tempopb.Trace{
		hex.EncodeToString(traceIDA): expectedRoutedFragment(resource, scope, source.ResourceSpans[0].ScopeSpans[0].Spans[0], maxAttributeBytes),
		hex.EncodeToString(traceIDB): expectedRoutedFragment(resource, scope, source.ResourceSpans[0].ScopeSpans[0].Spans[1], maxAttributeBytes),
	}

	for i, encodedTrace := range delivered.Traces {
		traceID := hex.EncodeToString(delivered.Ids[i])
		want, ok := expected[traceID]
		if !ok {
			t.Fatalf("unexpected routed trace ID %q", traceID)
		}
		got := &tempopb.Trace{}
		if err := got.Unmarshal(encodedTrace.Slice); err != nil {
			t.Fatalf("unmarshal routed fragment %q: %v", traceID, err)
		}
		if !proto.Equal(want, got) {
			t.Fatalf("routed fragment %q differs from the expected truncated payload\nwant: %s\n got: %s", traceID, want, got)
		}
		delete(expected, traceID)
	}
	if len(expected) != 0 {
		t.Fatalf("missing routed fragments for trace IDs: %v", expected)
	}
}

func TestPushTracesDeliversRoutedBatchesToGenerator(t *testing.T) {
	const maxAttributeBytes = 8

	traceIDA := benchmarkTraceID(11)
	traceIDB := benchmarkTraceID(12)
	source := &tempopb.Trace{
		ResourceSpans: []*v1.ResourceSpans{
			{
				Resource: &v1_resource.Resource{Attributes: []*v1_common.KeyValue{stringAttribute("resource-attribute", "resource-value-that-is-truncated")}},
				ScopeSpans: []*v1.ScopeSpans{
					{
						Scope: &v1_common.InstrumentationScope{Attributes: []*v1_common.KeyValue{stringAttribute("scope-attribute", "scope-value-that-is-truncated")}},
						Spans: []*v1.Span{
							routedFragmentSpan(traceIDA, benchmarkSpanID(11), "first-generator-span"),
							routedFragmentSpan(traceIDB, benchmarkSpanID(12), "second-generator-span"),
						},
					},
				},
			},
		},
	}
	encoded, err := source.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	traces, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(encoded)
	if err != nil {
		t.Fatal(err)
	}

	generatorRequests := make(chan *tempopb.PushSpansRequest, 1)
	d := newPushTracesBenchmarkDistributor(t, maxAttributeBytes, LocalPushTargets{
		Generator: func(_ context.Context, req *tempopb.PushSpansRequest) (*tempopb.PushResponse, error) {
			generatorRequests <- req
			return &tempopb.PushResponse{}, nil
		},
		LiveStore: func(_ context.Context, _ *tempopb.PushBytesRequest) (*tempopb.PushResponse, error) {
			return &tempopb.PushResponse{}, nil
		},
	})
	if d.generatorForwarder == nil {
		t.Fatal("generator forwarder was not configured")
	}
	if err := services.StartAndAwaitRunning(context.Background(), d.generatorForwarder); err != nil {
		t.Fatal(err)
	}
	stopped := false
	defer func() {
		if !stopped {
			if err := services.StopAndAwaitTerminated(context.Background(), d.generatorForwarder); err != nil {
				t.Error(err)
			}
		}
	}()

	if _, err := d.PushTraces(user.InjectOrgID(context.Background(), "generator-fragment-test"), traces); err != nil {
		t.Fatal(err)
	}

	// generatorForwarder.stop drains its queues before returning, so stopping it
	// gives this test a deterministic delivery barrier without a wall-clock race.
	if err := services.StopAndAwaitTerminated(context.Background(), d.generatorForwarder); err != nil {
		t.Fatal(err)
	}
	stopped = true
	var request *tempopb.PushSpansRequest
	select {
	case request = <-generatorRequests:
	default:
		t.Fatal("generator queue did not drain the routed request")
	}
	if len(request.Batches) != 2 {
		t.Fatalf("got %d generator batches, want two trace-owned batches", len(request.Batches))
	}

	resource := source.ResourceSpans[0].Resource
	scope := source.ResourceSpans[0].ScopeSpans[0].Scope
	expected := map[string]*v1.ResourceSpans{
		hex.EncodeToString(traceIDA): expectedRoutedFragment(resource, scope, source.ResourceSpans[0].ScopeSpans[0].Spans[0], maxAttributeBytes).ResourceSpans[0],
		hex.EncodeToString(traceIDB): expectedRoutedFragment(resource, scope, source.ResourceSpans[0].ScopeSpans[0].Spans[1], maxAttributeBytes).ResourceSpans[0],
	}
	for _, batch := range request.Batches {
		if len(batch.ScopeSpans) != 1 || len(batch.ScopeSpans[0].Spans) != 1 {
			t.Fatalf("generator batch has unexpected shape: %#v", batch)
		}
		traceID := hex.EncodeToString(batch.ScopeSpans[0].Spans[0].TraceId)
		want, ok := expected[traceID]
		if !ok {
			t.Fatalf("unexpected generator trace ID %q", traceID)
		}
		if !proto.Equal(want, batch) {
			t.Fatalf("generator batch %q differs from the expected truncated payload\nwant: %s\n got: %s", traceID, want, batch)
		}
		delete(expected, traceID)
	}
	if len(expected) != 0 {
		t.Fatalf("missing generator batches for trace IDs: %v", expected)
	}
}

func newPushTracesBenchmarkDistributor(tb testing.TB, maxAttributeBytes int, targets LocalPushTargets) *Distributor {
	tb.Helper()

	limits := overrides.Config{}
	limits.RegisterFlagsAndApplyDefaults(&flag.FlagSet{})
	limits.Defaults.Ingestion.RateStrategy = overrides.LocalIngestionRateStrategy
	limits.Defaults.Ingestion.RateLimitBytes = int(^uint(0) >> 1)
	limits.Defaults.Ingestion.BurstSizeBytes = int(^uint(0) >> 1)
	if targets.Generator != nil {
		limits.Defaults.MetricsGenerator.Processors = listtomap.ListToMap{"service-graphs": {}}
	}

	overridesSvc, err := overrides.NewOverrides(limits, nil, prometheus.NewRegistry())
	if err != nil {
		tb.Fatalf("create overrides: %v", err)
	}

	loggingLevel := dslog.Level{}
	if err := loggingLevel.Set("error"); err != nil {
		tb.Fatalf("set logging level: %v", err)
	}

	d, err := New(
		Config{MaxAttributeBytes: maxAttributeBytes},
		targets,
		nil,
		overridesSvc,
		receiver.MultiTenancyMiddleware(),
		kitlog.NewNopLogger(),
		loggingLevel,
		prometheus.NewPedanticRegistry(),
	)
	if err != nil {
		tb.Fatalf("create distributor: %v", err)
	}
	return d
}

func benchmarkPushTracesInput(traceCount, spansPerTrace int) ptrace.Traces {
	resource := &v1_resource.Resource{
		Attributes: []*v1_common.KeyValue{
			stringAttribute("service.name", "benchmark-service"),
			stringAttribute("deployment.environment", "benchmark"),
		},
	}
	scope := &v1_common.InstrumentationScope{
		Name:    "benchmark-library",
		Version: "1.0.0",
		Attributes: []*v1_common.KeyValue{
			stringAttribute("telemetry.sdk.language", "go"),
		},
	}
	spans := make([]*v1.Span, 0, traceCount*spansPerTrace)
	var sequence uint64
	for traceNumber := range traceCount {
		traceID := benchmarkTraceID(uint64(traceNumber + 1))
		for spanNumber := range spansPerTrace {
			sequence++
			span := &v1.Span{
				TraceId:           traceID,
				SpanId:            benchmarkSpanID(sequence),
				ParentSpanId:      benchmarkSpanID(sequence + 1),
				TraceState:        "vendor=benchmark",
				Flags:             0x101,
				Name:              "benchmark-span",
				Kind:              v1.Span_SPAN_KIND_SERVER,
				StartTimeUnixNano: 1_700_000_000_000_000_000 + sequence*1_000,
				EndTimeUnixNano:   1_700_000_000_000_001_000 + sequence*1_000,
				Attributes: []*v1_common.KeyValue{
					stringAttribute("http.request.method", "GET"),
					intAttribute("http.response.status_code", 200),
					stringAttribute("peer.service", "benchmark-peer"),
				},
				Status: &v1.Status{Code: v1.Status_STATUS_CODE_OK},
			}
			if spanNumber%3 == 0 {
				span.Events = []*v1.Span_Event{{
					TimeUnixNano: sequence,
					Name:         "benchmark-event",
					Attributes:   []*v1_common.KeyValue{stringAttribute("event.name", "benchmark")},
				}}
			}
			if spanNumber%4 == 0 {
				span.Links = []*v1.Span_Link{{
					TraceId:    benchmarkTraceID(uint64(traceNumber + 10_000)),
					SpanId:     benchmarkSpanID(sequence + 10_000),
					TraceState: "vendor=linked",
					Attributes: []*v1_common.KeyValue{stringAttribute("link.type", "batch")},
				}}
			}
			spans = append(spans, span)
		}
	}

	source := &tempopb.Trace{ResourceSpans: []*v1.ResourceSpans{{
		Resource: resource,
		ScopeSpans: []*v1.ScopeSpans{{
			Scope: scope,
			Spans: spans,
		}},
	}}}
	encoded, err := source.Marshal()
	if err != nil {
		panic(err)
	}
	traces, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(encoded)
	if err != nil {
		panic(err)
	}
	return traces
}

func routedFragmentSpan(traceID, spanID []byte, name string) *v1.Span {
	return &v1.Span{
		TraceId:                traceID,
		SpanId:                 spanID,
		ParentSpanId:           benchmarkSpanID(99),
		TraceState:             "vendor=state",
		Flags:                  0x101,
		Name:                   name,
		Kind:                   v1.Span_SPAN_KIND_SERVER,
		StartTimeUnixNano:      10 * uint64(time.Second),
		EndTimeUnixNano:        20 * uint64(time.Second),
		Attributes:             []*v1_common.KeyValue{stringAttribute("span-attribute", "span-value-that-is-truncated"), intAttribute("span.int", 7)},
		DroppedAttributesCount: 4,
		Events: []*v1.Span_Event{{
			TimeUnixNano:           15 * uint64(time.Second),
			Name:                   "event",
			Attributes:             []*v1_common.KeyValue{stringAttribute("event-attribute", "event-value-that-is-truncated")},
			DroppedAttributesCount: 5,
		}},
		DroppedEventsCount: 6,
		Links: []*v1.Span_Link{{
			TraceId:                benchmarkTraceID(99),
			SpanId:                 benchmarkSpanID(100),
			TraceState:             "vendor=linked",
			Attributes:             []*v1_common.KeyValue{stringAttribute("link-attribute", "link-value-that-is-truncated")},
			DroppedAttributesCount: 7,
			Flags:                  0x201,
		}},
		DroppedLinksCount: 8,
		Status:            &v1.Status{Message: "failed", Code: v1.Status_STATUS_CODE_ERROR},
	}
}

func expectedRoutedFragment(resource *v1_resource.Resource, scope *v1_common.InstrumentationScope, span *v1.Span, maxAttributeBytes int) *tempopb.Trace {
	resourceCopy := proto.Clone(resource).(*v1_resource.Resource)
	scopeCopy := proto.Clone(scope).(*v1_common.InstrumentationScope)
	spanCopy := proto.Clone(span).(*v1.Span)

	truncateExpectedAttributes(resourceCopy.Attributes, maxAttributeBytes)
	truncateExpectedAttributes(scopeCopy.Attributes, maxAttributeBytes)
	truncateExpectedAttributes(spanCopy.Attributes, maxAttributeBytes)
	for _, event := range spanCopy.Events {
		truncateExpectedAttributes(event.Attributes, maxAttributeBytes)
	}
	for _, link := range spanCopy.Links {
		truncateExpectedAttributes(link.Attributes, maxAttributeBytes)
	}

	// requestsByTraceID intentionally rebuilds these wrappers, so the output does
	// not retain the input resource or scope schema URLs.
	return &tempopb.Trace{ResourceSpans: []*v1.ResourceSpans{{
		Resource: resourceCopy,
		ScopeSpans: []*v1.ScopeSpans{{
			Scope: scopeCopy,
			Spans: []*v1.Span{spanCopy},
		}},
	}}}
}

func truncateExpectedAttributes(attributes []*v1_common.KeyValue, maxAttributeBytes int) {
	for _, attribute := range attributes {
		if len(attribute.Key) > maxAttributeBytes {
			attribute.Key = attribute.Key[:maxAttributeBytes]
		}
		if value, ok := attribute.GetValue().Value.(*v1_common.AnyValue_StringValue); ok && len(value.StringValue) > maxAttributeBytes {
			value.StringValue = value.StringValue[:maxAttributeBytes]
		}
	}
}

func stringAttribute(key, value string) *v1_common.KeyValue {
	return &v1_common.KeyValue{
		Key:   key,
		Value: &v1_common.AnyValue{Value: &v1_common.AnyValue_StringValue{StringValue: value}},
	}
}

func intAttribute(key string, value int64) *v1_common.KeyValue {
	return &v1_common.KeyValue{
		Key:   key,
		Value: &v1_common.AnyValue{Value: &v1_common.AnyValue_IntValue{IntValue: value}},
	}
}

func benchmarkTraceID(value uint64) []byte {
	id := make([]byte, 16)
	binary.BigEndian.PutUint64(id[8:], value)
	return id
}

func benchmarkSpanID(value uint64) []byte {
	id := make([]byte, 8)
	binary.BigEndian.PutUint64(id, value)
	return id
}
