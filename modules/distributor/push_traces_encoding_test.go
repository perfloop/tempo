package distributor

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"testing"
	"time"

	kitlog "github.com/go-kit/log"
	"github.com/gogo/status"
	dslog "github.com/grafana/dskit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/grpc/codes"

	"github.com/grafana/tempo/modules/distributor/receiver"
	"github.com/grafana/tempo/modules/overrides"
	"github.com/grafana/tempo/pkg/tempopb"
	v1_common "github.com/grafana/tempo/pkg/tempopb/common/v1"
	v1_resource "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	v1 "github.com/grafana/tempo/pkg/tempopb/trace/v1"
)

const (
	pushTracesEncodingTraceCount    = 8
	pushTracesEncodingSpansPerTrace = 64
)

func TestPushTracesAdmissionEncodingBehavior(t *testing.T) {
	t.Run("accepted request preserves input and rebatched payload", func(t *testing.T) {
		traces := pushTracesEncodingInput(t, 0)
		before, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(traces)
		require.NoError(t, err)

		var got *tempopb.PushBytesRequest
		d := newPushTracesEncodingDistributor(t, pushTracesEncodingLimits(2_000_000_000, 2_000_000_000), LocalPushTargets{
			LiveStore: func(_ context.Context, req *tempopb.PushBytesRequest) (*tempopb.PushResponse, error) {
				got = req
				return &tempopb.PushResponse{}, nil
			},
		})

		response, err := d.PushTraces(pushTracesEncodingContext(), traces)
		require.NoError(t, err)
		require.Nil(t, response)
		require.NotNil(t, got)
		require.Len(t, got.Traces, pushTracesEncodingTraceCount)
		require.Len(t, got.Ids, pushTracesEncodingTraceCount)

		gotSpanCount := 0
		for _, traceBytes := range got.Traces {
			decoded := tempopb.Trace{}
			require.NoError(t, decoded.Unmarshal(traceBytes.Slice))
			gotSpanCount += pushTracesEncodingSpanCount(&decoded)
		}
		require.Equal(t, traces.SpanCount(), gotSpanCount)

		after, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(traces)
		require.NoError(t, err)
		require.Equal(t, before, after)
	})

	t.Run("rate rejected request reports canonical size and skips downstream", func(t *testing.T) {
		traces := pushTracesEncodingInput(t, 1)
		expectedSize := (&ptrace.ProtoMarshaler{}).TracesSize(traces)

		liveStoreCalls := 0
		d := newPushTracesEncodingDistributor(t, pushTracesEncodingLimits(1, 1), LocalPushTargets{
			LiveStore: func(_ context.Context, _ *tempopb.PushBytesRequest) (*tempopb.PushResponse, error) {
				liveStoreCalls++
				return &tempopb.PushResponse{}, nil
			},
		})

		response, err := d.PushTraces(pushTracesEncodingContext(), traces)
		require.Nil(t, response)
		require.Error(t, err)
		grpcStatus, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.ResourceExhausted, grpcStatus.Code())
		require.Contains(t, grpcStatus.Message(), fmt.Sprintf("batch size (%d bytes)", expectedSize))
		require.Zero(t, liveStoreCalls)
	})

	t.Run("missing tenant preserves validation error and skips downstream", func(t *testing.T) {
		traces := pushTracesEncodingInput(t, 2)

		liveStoreCalls := 0
		d := newPushTracesEncodingDistributor(t, pushTracesEncodingLimits(2_000_000_000, 2_000_000_000), LocalPushTargets{
			LiveStore: func(_ context.Context, _ *tempopb.PushBytesRequest) (*tempopb.PushResponse, error) {
				liveStoreCalls++
				return &tempopb.PushResponse{}, nil
			},
		})

		response, err := d.PushTraces(context.Background(), traces)
		require.Nil(t, response)
		require.Error(t, err)
		grpcStatus, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, grpcStatus.Code())
		require.Equal(t, user.ErrNoOrgID.Error(), grpcStatus.Message())
		require.Zero(t, liveStoreCalls)
	})
}

func BenchmarkPushTraces(b *testing.B) {
	inputs := make([]ptrace.Traces, 4)
	for i := range inputs {
		inputs[i] = pushTracesEncodingInput(b, byte(i+16))
	}

	b.Run("accepted_multi_span", func(b *testing.B) {
		d := newPushTracesEncodingDistributor(b, pushTracesEncodingLimits(2_000_000_000, 2_000_000_000), LocalPushTargets{})
		ctx := pushTracesEncodingContext()

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			response, err := d.PushTraces(ctx, inputs[i%len(inputs)])
			if err != nil {
				b.Fatal(err)
			}
			if response != nil {
				b.Fatalf("unexpected response: %#v", response)
			}
		}
	})

	b.Run("rate_rejected_multi_span", func(b *testing.B) {
		d := newPushTracesEncodingDistributor(b, pushTracesEncodingLimits(1, 1), LocalPushTargets{})
		ctx := pushTracesEncodingContext()

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			response, err := d.PushTraces(ctx, inputs[i%len(inputs)])
			if response != nil {
				b.Fatalf("unexpected response: %#v", response)
			}
			grpcStatus, ok := status.FromError(err)
			if !ok || grpcStatus.Code() != codes.ResourceExhausted {
				b.Fatalf("expected rate-limit error, got %v", err)
			}
		}
	})
}

func newPushTracesEncodingDistributor(tb testing.TB, limits overrides.Config, targets LocalPushTargets) *Distributor {
	tb.Helper()

	overridesSvc, err := overrides.NewOverrides(limits, nil, prometheus.NewRegistry())
	require.NoError(tb, err)

	cfg := Config{}
	cfg.MaxAttributeBytes = 1000
	cfg.DistributorRing.HeartbeatPeriod = 100 * time.Millisecond
	cfg.DistributorRing.InstanceID = "push-traces-encoding"
	cfg.DistributorRing.KVStore.Mock = nil
	cfg.DistributorRing.InstanceInterfaceNames = []string{"lo"}

	loggingLevel := dslog.Level{}
	require.NoError(tb, loggingLevel.Set("error"))

	d, err := New(
		cfg,
		targets,
		nil,
		overridesSvc,
		receiver.MultiTenancyMiddleware(),
		kitlog.NewNopLogger(),
		loggingLevel,
		prometheus.NewPedanticRegistry(),
	)
	require.NoError(tb, err)
	return d
}

func pushTracesEncodingLimits(rateLimitBytes, burstSizeBytes int) overrides.Config {
	limits := overrides.Config{}
	limits.RegisterFlagsAndApplyDefaults(&flag.FlagSet{})
	if rateLimitBytes > 0 {
		limits.Defaults.Ingestion.RateStrategy = overrides.LocalIngestionRateStrategy
		limits.Defaults.Ingestion.RateLimitBytes = rateLimitBytes
		limits.Defaults.Ingestion.BurstSizeBytes = burstSizeBytes
	}
	return limits
}

func pushTracesEncodingInput(tb testing.TB, variant byte) ptrace.Traces {
	tb.Helper()

	// Keep this fixture deterministic while varying trace IDs at runtime. The shape
	// represents a multi-trace request with resource, scope, span, event, and link data.
	trace := tempopb.Trace{ResourceSpans: make([]*v1.ResourceSpans, 0, pushTracesEncodingTraceCount)}
	for traceIndex := 0; traceIndex < pushTracesEncodingTraceCount; traceIndex++ {
		traceID := make([]byte, 16)
		traceID[0] = variant
		traceID[len(traceID)-1] = byte(traceIndex + 1)

		spans := make([]*v1.Span, 0, pushTracesEncodingSpansPerTrace)
		for spanIndex := 0; spanIndex < pushTracesEncodingSpansPerTrace; spanIndex++ {
			spanID := make([]byte, 8)
			binary.BigEndian.PutUint64(spanID, uint64(traceIndex*pushTracesEncodingSpansPerTrace+spanIndex+1))
			span := &v1.Span{
				Name:              "ingest.operation",
				TraceId:           traceID,
				SpanId:            spanID,
				Kind:              v1.Span_SPAN_KIND_CLIENT,
				Status:            &v1.Status{Code: v1.Status_STATUS_CODE_OK},
				StartTimeUnixNano: uint64(1_000_000 + spanIndex*1_000),
				EndTimeUnixNano:   uint64(1_000_500 + spanIndex*1_000),
				Attributes: []*v1_common.KeyValue{
					pushTracesEncodingAttribute("http.method", "POST"),
					pushTracesEncodingAttribute("http.route", "/v1/traces"),
					pushTracesEncodingAttribute("service.instance.id", "distributor-0"),
				},
			}
			if spanIndex%3 == 0 {
				span.Events = []*v1.Span_Event{{
					TimeUnixNano: uint64(1_000_250 + spanIndex*1_000),
					Name:         "serialization",
					Attributes: []*v1_common.KeyValue{
						pushTracesEncodingAttribute("event.phase", "encode"),
					},
				}}
			}
			if spanIndex%5 == 0 {
				span.Links = []*v1.Span_Link{{
					TraceId:    traceID,
					SpanId:     spanID,
					TraceState: "sampled",
					Attributes: []*v1_common.KeyValue{
						pushTracesEncodingAttribute("link.type", "parent"),
					},
				}}
			}
			spans = append(spans, span)
		}

		trace.ResourceSpans = append(trace.ResourceSpans, &v1.ResourceSpans{
			Resource: &v1_resource.Resource{Attributes: []*v1_common.KeyValue{
				pushTracesEncodingAttribute("service.name", "push-traces-benchmark"),
				pushTracesEncodingAttribute("deployment.environment", "test"),
			}},
			ScopeSpans: []*v1.ScopeSpans{
				{
					Scope: &v1_common.InstrumentationScope{Name: "benchmark-client", Version: "1.0.0"},
					Spans: spans[:pushTracesEncodingSpansPerTrace/2],
				},
				{
					Scope: &v1_common.InstrumentationScope{Name: "benchmark-worker", Version: "1.0.0"},
					Spans: spans[pushTracesEncodingSpansPerTrace/2:],
				},
			},
		})
	}

	encoded, err := trace.Marshal()
	require.NoError(tb, err)
	traces, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(encoded)
	require.NoError(tb, err)
	return traces
}

func pushTracesEncodingAttribute(key, value string) *v1_common.KeyValue {
	return &v1_common.KeyValue{
		Key:   key,
		Value: &v1_common.AnyValue{Value: &v1_common.AnyValue_StringValue{StringValue: value}},
	}
}

func pushTracesEncodingContext() context.Context {
	return user.InjectOrgID(context.Background(), "push-traces-encoding")
}

func pushTracesEncodingSpanCount(trace *tempopb.Trace) int {
	spanCount := 0
	for _, resourceSpans := range trace.ResourceSpans {
		for _, scopeSpans := range resourceSpans.ScopeSpans {
			spanCount += len(scopeSpans.Spans)
		}
	}
	return spanCount
}
