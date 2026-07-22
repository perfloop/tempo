package vparquet4

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/google/uuid"

	tempo_io "github.com/grafana/tempo/pkg/io"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
)

const (
	perfloopRowGroups      = 64
	perfloopTracesPerGroup = 256
	perfloopReadBufferSize = 4 * 1024
)

type perfloopSelectiveDurationFixture struct {
	block  *backendBlock
	hit    *tempopb.SearchRequest
	misses [2]*tempopb.SearchRequest
}

func TestPerfloopSelectiveDurationSearch(t *testing.T) {
	fixture := newPerfloopSelectiveDurationFixture(t, 8, 8)
	opts := perfloopSelectiveDurationOptions()

	hit, err := fixture.block.Search(context.Background(), fixture.hit, opts)
	if err != nil {
		t.Fatalf("searching indexed duration hit: %v", err)
	}
	if len(hit.Traces) != 1 {
		t.Fatalf("indexed duration hit returned %d traces, want 1", len(hit.Traces))
	}

	for _, miss := range fixture.misses {
		result, err := fixture.block.Search(context.Background(), miss, opts)
		if err != nil {
			t.Fatalf("searching indexed duration miss: %v", err)
		}
		if len(result.Traces) != 0 {
			t.Fatalf("indexed duration miss returned %d traces, want 0", len(result.Traces))
		}
	}
}

func BenchmarkPerfloopSelectiveDurationSearch(b *testing.B) {
	fixture := newPerfloopSelectiveDurationFixture(b, perfloopRowGroups, perfloopTracesPerGroup)
	opts := perfloopSelectiveDurationOptions()
	ctx := b.Context()

	var inspectedBytes uint64
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		result, err := fixture.block.Search(ctx, fixture.misses[i%len(fixture.misses)], opts)
		if err != nil {
			b.Fatalf("searching indexed duration miss: %v", err)
		}
		if len(result.Traces) != 0 {
			b.Fatalf("indexed duration miss returned %d traces, want 0", len(result.Traces))
		}
		inspectedBytes += result.Metrics.InspectedBytes
	}
	b.ReportMetric(float64(inspectedBytes)/float64(b.N), "inspected_bytes/op")
}

func newPerfloopSelectiveDurationFixture(tb testing.TB, rowGroups, tracesPerGroup int) perfloopSelectiveDurationFixture {
	tb.Helper()

	rawReader, rawWriter, _, err := local.New(&local.Config{Path: tb.TempDir()})
	if err != nil {
		tb.Fatalf("creating local backend: %v", err)
	}

	reader := backend.NewReader(rawReader)
	writer := backend.NewWriter(rawWriter)
	meta := backend.NewBlockMeta("perfloop", uuid.New(), VersionString)
	meta.TotalObjects = 1

	stream, newMeta := newStreamingBlock(context.Background(), &common.BlockConfig{
		BloomFP:             0.01,
		BloomShardSizeBytes: 100 * 1024,
	}, meta, reader, writer, tempo_io.NewBufferedWriter)

	for group := range rowGroups {
		for trace := range tracesPerGroup {
			durationMS := uint64(group*1000 + trace + 1)
			traceID := make([]byte, 16)
			binary.BigEndian.PutUint64(traceID[8:], uint64(group*tracesPerGroup+trace+1))
			start := uint64(time.Unix(1_700_000_000+int64(group), 0).UnixNano())

			err := stream.Add(&Trace{
				TraceID:           traceID,
				StartTimeUnixNano: start,
				EndTimeUnixNano:   start + durationMS*uint64(time.Millisecond),
				DurationNano:      durationMS * uint64(time.Millisecond),
				RootServiceName:   "perfloop-service",
				RootSpanName:      "perfloop-span",
			}, 0, 0)
			if err != nil {
				tb.Fatalf("adding trace to row group %d: %v", group, err)
			}
		}

		if group < rowGroups-1 {
			if _, err := stream.Flush(); err != nil {
				tb.Fatalf("flushing row group %d: %v", group, err)
			}
		}
	}

	if _, err := stream.Complete(); err != nil {
		tb.Fatalf("completing benchmark block: %v", err)
	}

	missDurationMS := uint32(rowGroups*1000 + tracesPerGroup + 1)
	return perfloopSelectiveDurationFixture{
		block: newBackendBlock(newMeta, reader),
		hit:   perfloopDurationRequest(uint32((rowGroups/2)*1000 + 1)),
		misses: [2]*tempopb.SearchRequest{
			perfloopDurationRequest(missDurationMS),
			perfloopDurationRequest(missDurationMS + 1),
		},
	}
}

func perfloopSelectiveDurationOptions() common.SearchOptions {
	opts := common.DefaultSearchOptions()
	opts.ReadBufferSize = perfloopReadBufferSize
	return opts
}

func perfloopDurationRequest(durationMS uint32) *tempopb.SearchRequest {
	return &tempopb.SearchRequest{
		MinDurationMs: durationMS,
		MaxDurationMs: durationMS,
	}
}
