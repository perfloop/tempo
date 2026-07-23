package distributor

import (
	"context"
	"testing"

	"github.com/grafana/dskit/user"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// BenchmarkDistributorPushTracesRejectedLogging prices the validation-error
// path with discarded logging, rich attributes, and a small truncation limit.
// It consumes the emitted discarded logs so both revisions execute the full
// configured diagnostic behavior.
func BenchmarkDistributorPushTracesRejectedLogging(b *testing.B) {
	const traceCount = 37
	traces := benchmarkPushTracesRejectedLoggingInput(b, traceCount, 6)
	inputBytes := (&ptrace.ProtoMarshaler{}).TracesSize(traces)

	d := newPushTracesBenchmarkDistributor(b, 4, LocalPushTargets{})
	d.cfg.LogDiscardedSpans = LogSpansConfig{Enabled: true, IncludeAllAttributes: true}
	logs := &discardedLogCounter{}
	d.logger = logs

	ctx := user.InjectOrgID(context.Background(), "benchmark-tenant")
	b.SetBytes(int64(inputBytes))
	b.ReportAllocs()

	iterations := 0
	for b.Loop() {
		response, err := d.PushTraces(ctx, traces)
		if err == nil {
			b.Fatal("invalid SpanID unexpectedly succeeded")
		}
		if response != nil {
			b.Fatalf("unexpected PushTraces response: %#v", response)
		}
		iterations++
	}

	if logs.discarded == 0 {
		b.Fatal("discarded-span logging did not emit a log")
	}
}

func benchmarkPushTracesRejectedLoggingInput(tb testing.TB, traceCount, spansPerTrace int) ptrace.Traces {
	tb.Helper()

	traces := benchmarkPushTracesRichAttributesInput(tb, traceCount, spansPerTrace)
	resourceSpans := traces.ResourceSpans()
	resourceSpans.At(0).Resource().Attributes().PutStr("resource-oversized-key", "resource-oversized-value")

	span := resourceSpans.At(0).ScopeSpans().At(0).Spans().At(0)
	span.Attributes().PutStr("span-oversized-key", "span-oversized-value")
	span.SetSpanID(pcommon.SpanID{})
	return traces
}

type discardedLogCounter struct {
	discarded int
}

func (l *discardedLogCounter) Log(keyvals ...interface{}) error {
	for index := 0; index+1 < len(keyvals); index += 2 {
		key, keyIsString := keyvals[index].(string)
		message, messageIsString := keyvals[index+1].(string)
		if keyIsString && messageIsString && key == "msg" && message == "discarded" {
			l.discarded++
		}
	}
	return nil
}
