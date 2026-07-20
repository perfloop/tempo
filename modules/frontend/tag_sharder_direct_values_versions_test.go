package frontend

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/google/uuid"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	livestore_client "github.com/grafana/tempo/modules/livestore/client"
	"github.com/grafana/tempo/modules/overrides"
	"github.com/grafana/tempo/modules/querier"
	"github.com/grafana/tempo/pkg/model"
	"github.com/grafana/tempo/pkg/util/test"
	"github.com/grafana/tempo/tempodb"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/grafana/tempo/tempodb/encoding/vparquet3"
	"github.com/grafana/tempo/tempodb/encoding/vparquet5"
	"github.com/grafana/tempo/tempodb/wal"
)

func newTagValueVersionFixture(tb testing.TB, version string) *tagValueBenchmarkFixture {
	tb.Helper()

	root := tb.TempDir()
	backendPath := filepath.Join(root, "traces")
	rawReader, _, _, err := local.New(&local.Config{Path: backendPath})
	require.NoError(tb, err)

	searchConfig := tempodb.SearchConfig{}
	dbConfig := &tempodb.Config{
		Backend: backend.Local,
		Local:   &local.Config{Path: backendPath},
		Block: &common.BlockConfig{
			BloomFP:             common.DefaultBloomFP,
			BloomShardSizeBytes: common.DefaultBloomShardSizeBytes,
			Version:             version,
			RowGroupSizeBytes:   1024,
			DedicatedColumns:    backend.DefaultDedicatedColumns(),
		},
		WAL: &wal.Config{
			Filepath:       filepath.Join(root, "wal"),
			IngestionSlack: time.Since(time.Time{}),
		},
		Search:        &searchConfig,
		BlocklistPoll: 0,
	}

	baseReader, writer, compactor, err := tempodb.New(dbConfig, nil, log.NewNopLogger())
	require.NoError(tb, err)
	tb.Cleanup(baseReader.Shutdown)

	meta := backend.NewBlockMeta(tagValueBenchmarkTenant, uuid.New(), version)
	meta.DedicatedColumns = backend.DefaultDedicatedColumns()
	head, err := writer.WAL().NewBlock(meta, model.CurrentEncoding)
	require.NoError(tb, err)
	for i := 0; i < 32; i++ {
		traceID := make([]byte, 16)
		traceID[15] = byte(i + 1)
		err = head.AppendTrace(traceID, test.MakeTraceWithSpanCount(1, 8, traceID), 10, 20, false)
		require.NoError(tb, err)
	}
	block, err := writer.CompleteBlock(context.Background(), head)
	require.NoError(tb, err)
	meta = block.BlockMeta()
	require.GreaterOrEqual(tb, meta.TotalRecords, uint32(4))

	targetBytesPerRequest := max(1, int(meta.Size_/8))
	require.GreaterOrEqual(tb, iterateTagJobs([]*backend.BlockMeta{meta}, targetBytesPerRequest, nil), 4)

	stats := &tagValueBenchmarkStats{}
	reader := &tagValueBenchmarkReader{
		Reader:        baseReader,
		backendReader: &tagValueBenchmarkBackendReader{Reader: backend.NewReader(rawReader), stats: stats},
		searchConfig:  searchConfig,
		stats:         stats,
		metas:         []*backend.BlockMeta{meta},
	}
	store := &tagValueBenchmarkStore{Reader: reader, Writer: writer, Compactor: compactor}
	limits, err := overrides.NewOverrides(overrides.Config{}, nil, prometheus.NewRegistry())
	require.NoError(tb, err)
	q, err := querier.New(querier.Config{Search: querier.SearchConfig{QueryTimeout: time.Minute}}, nil, livestore_client.Config{}, nil, false, store, limits)
	require.NoError(tb, err)

	next := &tagValueBenchmarkRoundTripper{querier: q}
	fixture := &tagValueBenchmarkFixture{
		frontend: frontendWithSettings(tb, next, reader, nil, nil, func(cfg *Config, _ *overrides.Config) {
			cfg.Search.Sharder.ConcurrentRequests = 4
			cfg.Search.Sharder.TargetBytesPerRequest = targetBytesPerRequest
		}),
		querier:          q,
		next:             next,
		stats:            stats,
		meta:             meta,
		directRequest:    tagValueBenchmarkHTTPRequest(""),
		conditionRequest: tagValueBenchmarkHTTPRequest("{ resource.service.name = `test-service` }"),
	}
	fixture.expectedValues = tagValueBenchmarkValues(tb, fixture.fullBlockResponse(tb))
	fixture.stats.reset()
	fixture.next.reset()

	return fixture
}

func TestTagValueSharderDirectBlockPlanPreservesOtherVersions(t *testing.T) {
	for _, version := range []string{vparquet3.VersionString, vparquet5.VersionString} {
		t.Run(version, func(t *testing.T) {
			fixture := newTagValueVersionFixture(t, version)

			full := fixture.fullBlockResponse(t)
			fixture.stats.reset()
			ranged, err := fixture.querier.SearchTagValuesBlocksV2(user.InjectOrgID(context.Background(), tagValueBenchmarkTenant), fixture.blockRequest("", 1, 1))
			require.NoError(t, err)
			require.Equal(t, tagValueBenchmarkValues(t, full), tagValueBenchmarkValues(t, ranged))
			require.NotNil(t, full.Metrics)
			require.NotNil(t, ranged.Metrics)
			require.Equal(t, full.Metrics.InspectedBytes, ranged.Metrics.InspectedBytes)

			fixture.next.capturePageRanges.Store(true)
			result := fixture.run(t, fixture.directRequest, true)
			ranges := fixture.next.ranges()
			require.Len(t, ranges, 1)
			require.Equal(t, 0, ranges[0].startPage)
			require.Equal(t, int(fixture.meta.TotalRecords), ranges[0].pages)
			require.Equal(t, uint64(1), result.childRequests)
			require.Equal(t, full.Metrics.InspectedBytes, result.inspectedBytes)
			t.Logf("direct block plan: version=%s reads=%d bytes=%d", fixture.meta.Version, result.backendReadCalls, result.backendReadBytes)
		})
	}
}
