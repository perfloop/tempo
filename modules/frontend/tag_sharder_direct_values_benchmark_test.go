package frontend

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"runtime/metrics"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/grafana/tempo/modules/frontend/pipeline"
	livestore_client "github.com/grafana/tempo/modules/livestore/client"
	"github.com/grafana/tempo/modules/overrides"
	"github.com/grafana/tempo/modules/querier"
	"github.com/grafana/tempo/modules/storage"
	"github.com/grafana/tempo/pkg/api"
	"github.com/grafana/tempo/pkg/collector"
	"github.com/grafana/tempo/pkg/model"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/pkg/util/test"
	"github.com/grafana/tempo/tempodb"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/grafana/tempo/tempodb/encoding/vparquet4"
	"github.com/grafana/tempo/tempodb/wal"
)

const (
	tagValueBenchmarkTenant = "tag-value-benchmark"
	tagValueBenchmarkName   = "resource.service.name"
)

// tagValueBenchmarkStats measures the request boundary below the frontend. The
// block reader is deliberately uncached: backendBlock.openForSearch creates its
// own reader for every child request, so each request records the storage work
// that a frontend cache miss would trigger.
type tagValueBenchmarkStats struct {
	backendReadCalls atomic.Uint64
	backendReadBytes atomic.Uint64
	inspectedBytes   atomic.Uint64
	directScans      atomic.Uint64
}

func (s *tagValueBenchmarkStats) reset() {
	s.backendReadCalls.Store(0)
	s.backendReadBytes.Store(0)
	s.inspectedBytes.Store(0)
	s.directScans.Store(0)
}

type tagValueBenchmarkBackendReader struct {
	backend.Reader
	stats *tagValueBenchmarkStats
}

func (r *tagValueBenchmarkBackendReader) ReadRange(ctx context.Context, name string, blockID uuid.UUID, tenantID string, offset uint64, buffer []byte, cacheInfo *backend.CacheInfo) error {
	r.stats.backendReadCalls.Add(1)
	r.stats.backendReadBytes.Add(uint64(len(buffer)))
	return r.Reader.ReadRange(ctx, name, blockID, tenantID, offset, buffer, cacheInfo)
}

// tagValueBenchmarkReader delegates the complete Tempo reader surface to a
// real local TempoDB, but intercepts only the direct V2 value leaf so the
// fixture can count its actual backend ReadRange calls. The implementation
// intentionally mirrors readerWriter.SearchTagValuesV2 and opens the real
// vParquet4 block for each child request.
type tagValueBenchmarkReader struct {
	tempodb.Reader
	backendReader backend.Reader
	searchConfig  tempodb.SearchConfig
	stats         *tagValueBenchmarkStats
	metas         []*backend.BlockMeta
}

func (r *tagValueBenchmarkReader) BlockMetas(string) []*backend.BlockMeta {
	return r.metas
}

func (r *tagValueBenchmarkReader) SearchTagValuesV2(ctx context.Context, meta *backend.BlockMeta, req *tempopb.SearchTagValuesRequest, opts common.SearchOptions) (*tempopb.SearchTagValuesV2Response, error) {
	block, err := encoding.OpenBlock(meta, r.backendReader)
	if err != nil {
		return nil, err
	}

	tag, err := traceql.ParseIdentifier(req.TagName)
	if err != nil {
		return nil, err
	}

	distinctValues := collector.NewDistinctValue(0, req.MaxTagValues, req.StaleValueThreshold, func(v tempopb.TagValue) int {
		return len(v.Type) + len(v.Value)
	})
	metricsCollector := collector.NewMetricsCollector()
	r.searchConfig.ApplyToOptions(&opts)
	err = block.SearchTagValuesV2(ctx, tag, traceql.MakeCollectTagValueFunc(distinctValues.Collect), metricsCollector.Add, opts)
	if err != nil {
		return nil, err
	}

	inspectedBytes := metricsCollector.TotalValue()
	r.stats.inspectedBytes.Add(inspectedBytes)
	r.stats.directScans.Add(1)

	response := &tempopb.SearchTagValuesV2Response{
		Metrics: &tempopb.MetadataMetrics{InspectedBytes: inspectedBytes},
	}
	for _, value := range distinctValues.Values() {
		v := value
		response.TagValues = append(response.TagValues, &v)
	}

	return response, nil
}

// tagValueBenchmarkStore is enough of modules/storage.Store for a Querier to
// use the real direct storage path. The embedded TempoDB interfaces retain the
// production behavior for the condition-bearing FetchTagValues branch.
type tagValueBenchmarkStore struct {
	services.Service
	tempodb.Reader
	tempodb.Writer
	tempodb.Compactor
}

var _ storage.Store = (*tagValueBenchmarkStore)(nil)

type tagValuePageRange struct {
	startPage int
	pages     int
}

type tagValueBenchmarkRoundTripper struct {
	querier *querier.Querier

	requests atomic.Uint64

	capturePageRanges atomic.Bool
	pageRangesLock    sync.Mutex
	pageRanges        []tagValuePageRange
}

var _ pipeline.RoundTripper = (*tagValueBenchmarkRoundTripper)(nil)

func (r *tagValueBenchmarkRoundTripper) RoundTrip(request pipeline.Request) (*http.Response, error) {
	r.requests.Add(1)

	if r.capturePageRanges.Load() {
		values := request.HTTPRequest().URL.Query()
		startPage, startErr := strconv.Atoi(values.Get("startPage"))
		pages, pagesErr := strconv.Atoi(values.Get("pagesToSearch"))
		if startErr == nil && pagesErr == nil {
			r.pageRangesLock.Lock()
			r.pageRanges = append(r.pageRanges, tagValuePageRange{startPage: startPage, pages: pages})
			r.pageRangesLock.Unlock()
		}
	}

	responseWriter := httptest.NewRecorder()
	r.querier.SearchTagValuesV2Handler(responseWriter, request.HTTPRequest())
	return responseWriter.Result(), nil
}

func (r *tagValueBenchmarkRoundTripper) reset() {
	r.requests.Store(0)
	r.pageRangesLock.Lock()
	r.pageRanges = r.pageRanges[:0]
	r.pageRangesLock.Unlock()
}

func (r *tagValueBenchmarkRoundTripper) ranges() []tagValuePageRange {
	r.pageRangesLock.Lock()
	defer r.pageRangesLock.Unlock()
	return slices.Clone(r.pageRanges)
}

type tagValueBenchmarkResult struct {
	values           []string
	childRequests    uint64
	backendReadCalls uint64
	backendReadBytes uint64
	inspectedBytes   uint64
	directScans      uint64
	responseHTTPCode int
}

type tagValueBenchmarkFixture struct {
	frontend *QueryFrontend
	querier  *querier.Querier
	next     *tagValueBenchmarkRoundTripper
	stats    *tagValueBenchmarkStats
	meta     *backend.BlockMeta

	directRequest    *http.Request
	conditionRequest *http.Request
	expectedValues   []string
}

func newTagValueBenchmarkFixture(tb testing.TB) *tagValueBenchmarkFixture {
	tb.Helper()

	root := tb.TempDir()
	backendPath := filepath.Join(root, "traces")
	rawReader, _, _, err := local.New(&local.Config{Path: backendPath})
	require.NoError(tb, err)

	// Leave the storage settings at Tempo's production defaults. In particular,
	// every direct child gets the normal 1 MiB parquet read buffer rather than a
	// benchmark-specific small buffer that would inflate backend read counts.
	searchConfig := tempodb.SearchConfig{}
	dbConfig := &tempodb.Config{
		Backend: backend.Local,
		Local:   &local.Config{Path: backendPath},
		Block: &common.BlockConfig{
			BloomFP:             common.DefaultBloomFP,
			BloomShardSizeBytes: common.DefaultBloomShardSizeBytes,
			Version:             vparquet4.VersionString,
			// A small row-group target makes the fixture a multi-page block
			// without manufacturing an oversized byte slice.
			RowGroupSizeBytes: 1024,
			DedicatedColumns:  backend.DefaultDedicatedColumns(),
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

	meta := backend.NewBlockMeta(tagValueBenchmarkTenant, uuid.New(), vparquet4.VersionString)
	meta.DedicatedColumns = backend.DefaultDedicatedColumns()
	head, err := writer.WAL().NewBlock(meta, model.CurrentEncoding)
	require.NoError(tb, err)

	// Every trace has resource.service.name=test-service. The result is
	// deliberately consumed below so the benchmark cannot measure a discarded
	// direct storage response.
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

	// Derive the test-only configured target from the completed block's true
	// metadata. This preserves the size/row-group relationship the production
	// planner uses while making this local fixture reliably fan out.
	targetBytesPerRequest := max(1, int(meta.Size_/8))
	pageJobs := iterateTagJobs([]*backend.BlockMeta{meta}, targetBytesPerRequest, nil)
	require.GreaterOrEqual(tb, pageJobs, 4)

	stats := &tagValueBenchmarkStats{}
	countingReader := &tagValueBenchmarkBackendReader{
		Reader: backend.NewReader(rawReader),
		stats:  stats,
	}
	reader := &tagValueBenchmarkReader{
		Reader:        baseReader,
		backendReader: countingReader,
		searchConfig:  searchConfig,
		stats:         stats,
		metas:         []*backend.BlockMeta{meta},
	}
	store := &tagValueBenchmarkStore{
		Reader:    reader,
		Writer:    writer,
		Compactor: compactor,
	}
	limits, err := overrides.NewOverrides(overrides.Config{}, nil, prometheus.NewRegistry())
	require.NoError(tb, err)
	q, err := querier.New(querier.Config{
		Search: querier.SearchConfig{QueryTimeout: time.Minute},
	}, nil, livestore_client.Config{}, nil, false, store, limits)
	require.NoError(tb, err)

	next := &tagValueBenchmarkRoundTripper{querier: q}
	frontend := frontendWithSettings(tb, next, reader, nil, nil, func(cfg *Config, _ *overrides.Config) {
		cfg.Search.Sharder.ConcurrentRequests = 4
		cfg.Search.Sharder.TargetBytesPerRequest = targetBytesPerRequest
	})

	fixture := &tagValueBenchmarkFixture{
		frontend:         frontend,
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

func tagValueBenchmarkHTTPRequest(query string) *http.Request {
	path := fmt.Sprintf("/api/v2/search/tag/%s/values?start=1&end=30", tagValueBenchmarkName)
	if query != "" {
		path += "&q=" + url.QueryEscape(query)
	}

	request := httptest.NewRequest(http.MethodGet, path, nil)
	request.Header.Set(api.HeaderAccept, api.HeaderAcceptJSON)
	request = mux.SetURLVars(request, map[string]string{api.MuxVarTagName: tagValueBenchmarkName})
	return request.WithContext(user.InjectOrgID(request.Context(), tagValueBenchmarkTenant))
}

func (f *tagValueBenchmarkFixture) fullBlockResponse(tb testing.TB) *tempopb.SearchTagValuesV2Response {
	tb.Helper()
	response, err := f.querier.SearchTagValuesBlocksV2(user.InjectOrgID(context.Background(), tagValueBenchmarkTenant), f.blockRequest("", 0, 0))
	require.NoError(tb, err)
	return response
}

func (f *tagValueBenchmarkFixture) blockRequest(query string, startPage, pages int) *tempopb.SearchTagValuesBlockRequest {
	return &tempopb.SearchTagValuesBlockRequest{
		SearchReq: &tempopb.SearchTagValuesRequest{
			TagName: tagValueBenchmarkName,
			Query:   query,
			Start:   1,
			End:     30,
		},
		BlockID:       f.meta.BlockID.String(),
		StartPage:     uint32(startPage),
		PagesToSearch: uint32(pages),
		IndexPageSize: f.meta.IndexPageSize,
		TotalRecords:  f.meta.TotalRecords,
		Version:       f.meta.Version,
		Size_:         f.meta.Size_,
		FooterSize:    f.meta.FooterSize,
	}
}

func (f *tagValueBenchmarkFixture) runDirectLeafFanout(tb testing.TB, width int) tagValueBenchmarkResult {
	tb.Helper()
	require.Positive(tb, width)
	require.LessOrEqual(tb, uint32(width), f.meta.TotalRecords)
	f.stats.reset()

	for startPage := 0; startPage < width; startPage++ {
		response, err := f.querier.SearchTagValuesBlocksV2(user.InjectOrgID(context.Background(), tagValueBenchmarkTenant), f.blockRequest("", startPage, 1))
		require.NoError(tb, err)
		require.Equal(tb, f.expectedValues, tagValueBenchmarkValues(tb, response))
	}

	return tagValueBenchmarkResult{
		backendReadCalls: f.stats.backendReadCalls.Load(),
		backendReadBytes: f.stats.backendReadBytes.Load(),
		inspectedBytes:   f.stats.inspectedBytes.Load(),
		directScans:      f.stats.directScans.Load(),
	}
}

func (f *tagValueBenchmarkFixture) run(tb testing.TB, request *http.Request, direct bool) tagValueBenchmarkResult {
	tb.Helper()
	f.stats.reset()
	f.next.reset()

	responseWriter := httptest.NewRecorder()
	f.frontend.SearchTagsValuesV2Handler.ServeHTTP(responseWriter, request)

	response := &tempopb.SearchTagValuesV2Response{}
	if responseWriter.Code == http.StatusOK {
		err := jsonpb.Unmarshal(bytes.NewReader(responseWriter.Body.Bytes()), response)
		require.NoError(tb, err)
	} else {
		tb.Fatalf("tag-value benchmark request failed: status=%d body=%s", responseWriter.Code, responseWriter.Body.String())
	}

	result := tagValueBenchmarkResult{
		values:           tagValueBenchmarkValues(tb, response),
		childRequests:    f.next.requests.Load(),
		backendReadCalls: f.stats.backendReadCalls.Load(),
		backendReadBytes: f.stats.backendReadBytes.Load(),
		inspectedBytes:   f.stats.inspectedBytes.Load(),
		directScans:      f.stats.directScans.Load(),
		responseHTTPCode: responseWriter.Code,
	}

	require.Equal(tb, f.expectedValues, result.values)
	if direct {
		require.Equal(tb, result.childRequests, result.directScans)
		require.NotNil(tb, response.Metrics)
		require.Equal(tb, result.inspectedBytes, response.Metrics.InspectedBytes)
	}

	return result
}

func tagValueBenchmarkValues(tb testing.TB, response *tempopb.SearchTagValuesV2Response) []string {
	tb.Helper()
	values := make([]string, 0, len(response.TagValues))
	for _, value := range response.TagValues {
		values = append(values, value.Type+":"+value.Value)
	}
	slices.Sort(values)
	return values
}

func TestTagValueSharderDirectLeafIgnoresPageRange(t *testing.T) {
	fixture := newTagValueBenchmarkFixture(t)

	full := fixture.fullBlockResponse(t)
	fixture.stats.reset()
	ranged, err := fixture.querier.SearchTagValuesBlocksV2(user.InjectOrgID(context.Background(), tagValueBenchmarkTenant), fixture.blockRequest("", 1, 1))
	require.NoError(t, err)
	require.Equal(t, tagValueBenchmarkValues(t, full), tagValueBenchmarkValues(t, ranged))
	require.NotNil(t, full.Metrics)
	require.NotNil(t, ranged.Metrics)
	require.Equal(t, full.Metrics.InspectedBytes, ranged.Metrics.InspectedBytes)
	require.Equal(t, uint64(1), fixture.stats.directScans.Load())
}

func TestTagValueDirectLeafFanoutScalesStorageWork(t *testing.T) {
	fixture := newTagValueBenchmarkFixture(t)

	one := fixture.runDirectLeafFanout(t, 1)
	wide := fixture.runDirectLeafFanout(t, 4)

	require.Equal(t, uint64(1), one.directScans)
	require.Equal(t, uint64(4), wide.directScans)
	require.Equal(t, one.backendReadCalls*4, wide.backendReadCalls)
	require.Equal(t, one.backendReadBytes*4, wide.backendReadBytes)
	require.Equal(t, one.inspectedBytes*4, wide.inspectedBytes)
	t.Logf("fanout sweep: width=1 reads=%d bytes=%d inspected=%d; width=4 reads=%d bytes=%d inspected=%d", one.backendReadCalls, one.backendReadBytes, one.inspectedBytes, wide.backendReadCalls, wide.backendReadBytes, wide.inspectedBytes)
}

func TestTagValueSharderDirectBlockUsesRealStorage(t *testing.T) {
	fixture := newTagValueBenchmarkFixture(t)

	spanRecorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))
	previousProvider := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(previousProvider)
		require.NoError(t, provider.Shutdown(context.Background()))
	})

	result := fixture.run(t, fixture.directRequest, true)
	require.Equal(t, http.StatusOK, result.responseHTTPCode)
	require.Greater(t, result.childRequests, uint64(0))
	require.Greater(t, result.backendReadCalls, uint64(0))
	require.Greater(t, result.backendReadBytes, uint64(0))
	require.Greater(t, result.inspectedBytes, uint64(0))

	directLeafSpans := 0
	for _, span := range spanRecorder.Ended() {
		if span.Name() == "parquet.backendBlock.SearchTagValuesV2" {
			directLeafSpans++
		}
	}
	require.Equal(t, int(result.directScans), directLeafSpans)
}

func TestTagValueSharderKeepsConditionedValueQueriesPaged(t *testing.T) {
	fixture := newTagValueBenchmarkFixture(t)
	fixture.next.capturePageRanges.Store(true)
	result := fixture.run(t, fixture.conditionRequest, false)

	require.Equal(t, http.StatusOK, result.responseHTTPCode)
	ranges := fixture.next.ranges()
	require.GreaterOrEqual(t, len(ranges), 2)

	starts := make(map[int]struct{}, len(ranges))
	for _, pageRange := range ranges {
		require.Greater(t, pageRange.pages, 0)
		starts[pageRange.startPage] = struct{}{}
	}
	require.GreaterOrEqual(t, len(starts), 2)
}

func BenchmarkTagValueSharderDirectBlock(b *testing.B) {
	fixture := newTagValueBenchmarkFixture(b)
	benchmarkTagValueSharderDirectBlock(b, fixture)
}

func BenchmarkTagValueSharderDirectBlockLatency(b *testing.B) {
	fixture := newTagValueBenchmarkFixture(b)
	benchmarkTagValueSharderDirectBlock(b, fixture)
}

func benchmarkTagValueSharderDirectBlock(b *testing.B, fixture *tagValueBenchmarkFixture) {
	var (
		totalRequests  uint64
		totalReadCalls uint64
		totalReadBytes uint64
		totalInspected uint64
		cpuStart       float64
		cpuStarted     bool
	)

	for b.Loop() {
		if !cpuStarted {
			cpuStart = goUserCPUSeconds()
			cpuStarted = true
		}
		result := fixture.run(b, fixture.directRequest, true)
		totalRequests += result.childRequests
		totalReadCalls += result.backendReadCalls
		totalReadBytes += result.backendReadBytes
		totalInspected += result.inspectedBytes
	}

	cpuNanos := (goUserCPUSeconds() - cpuStart) * float64(time.Second)
	operations := float64(b.N)
	b.ReportMetric(float64(totalRequests)/operations, "backend_requests/op")
	b.ReportMetric(float64(totalReadCalls)/operations, "backend_read_calls/op")
	b.ReportMetric(float64(totalReadBytes)/operations, "backend_read_bytes/op")
	b.ReportMetric(float64(totalInspected)/operations, "inspected_bytes/op")
	b.ReportMetric(float64(totalInspected)/float64(totalRequests), "inspected_bytes/child")
	b.ReportMetric(cpuNanos/operations, "go_cpu_ns/op")
}

func goUserCPUSeconds() float64 {
	sample := []metrics.Sample{{Name: "/cpu/classes/user:cpu-seconds"}}
	metrics.Read(sample)
	return sample[0].Value.Float64()
}
