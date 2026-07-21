package frontend

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"testing"
	"time"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gorilla/mux"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/modules/frontend/pipeline"
	"github.com/grafana/tempo/modules/overrides"
	"github.com/grafana/tempo/pkg/api"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/tempodb/backend"
)

const (
	tagValueV2PlanPageCount = uint32(16)
	tagValueV2PlanTagName   = ".service.name"
)

func tagValueV2PlanMeta() *backend.BlockMeta {
	return &backend.BlockMeta{
		StartTime:    time.Unix(100, 0),
		EndTime:      time.Unix(200, 0),
		Size_:        uint64(defaultTargetBytesPerRequest) * uint64(tagValueV2PlanPageCount),
		TotalRecords: tagValueV2PlanPageCount,
		BlockID:      backend.MustParse("00000000-0000-0000-0000-000000000123"),
		Version:      "vParquet4",
	}
}

func newTagValueV2PlanSharder(t testing.TB) searchTagSharder {
	t.Helper()

	o, err := overrides.NewOverrides(overrides.Config{}, nil, prometheus.NewRegistry())
	require.NoError(t, err)

	return searchTagSharder{
		cfg: SearchSharderConfig{
			TargetBytesPerRequest: defaultTargetBytesPerRequest,
		},
		reader:    &mockReader{metas: []*backend.BlockMeta{tagValueV2PlanMeta()}},
		overrides: o,
	}
}

func newTagValueV2PlanRequest(t testing.TB, query string) *http.Request {
	t.Helper()

	values := url.Values{
		"start": []string{"100"},
		"end":   []string{"200"},
		"q":     []string{query},
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v2/search/tag/"+tagValueV2PlanTagName+"/values?"+values.Encode(), nil)
	return mux.SetURLVars(req, map[string]string{api.MuxVarTagName: tagValueV2PlanTagName})
}

func tagValueV2PlanRequests(t testing.TB, query string) []pipeline.Request {
	t.Helper()

	parentRequest := newTagValueV2PlanRequest(t, query)
	searchReq, err := parseTagValuesRequestV2(parentRequest)
	require.NoError(t, err)

	reqCh := make(chan pipeline.Request)
	totalJobs := newTagValueV2PlanSharder(t).backendRequests(
		context.Background(),
		"tenant",
		pipeline.NewHTTPRequest(parentRequest),
		searchReq,
		reqCh,
		func(error) {},
	)

	var requests []pipeline.Request
	for req := range reqCh {
		requests = append(requests, req)
	}

	require.NotEmpty(t, requests)
	require.Equal(t, totalJobs, len(requests))
	return requests
}

func tagValueV2PlanValues(startPage, pagesToSearch uint32) []*tempopb.TagValue {
	endPage := tagValueV2PlanPageCount
	if pagesToSearch != 0 {
		endPage = min(startPage+pagesToSearch, tagValueV2PlanPageCount)
	}

	values := make([]*tempopb.TagValue, 0, endPage-startPage)
	for page := startPage; page < endPage; page++ {
		values = append(values, &tempopb.TagValue{Type: "string", Value: fmt.Sprintf("value-%02d", page)})
	}

	return values
}

func tagValueV2PlanResponse(req pipeline.Request) (*http.Response, error) {
	blockReq, err := api.ParseSearchTagValuesBlockRequestV2(req.HTTPRequest())
	if err != nil {
		return nil, fmt.Errorf("parse tag-values block request: %w", err)
	}

	body, err := (&jsonpb.Marshaler{}).MarshalToString(&tempopb.SearchTagValuesV2Response{
		TagValues: tagValueV2PlanValues(blockReq.StartPage, blockReq.PagesToSearch),
		Metrics:   &tempopb.MetadataMetrics{InspectedBytes: 1},
	})
	if err != nil {
		return nil, fmt.Errorf("marshal tag-values response: %w", err)
	}

	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewBufferString(body)),
	}, nil
}

func TestTagValueV2UnfilteredBackendPlanPreservesValues(t *testing.T) {
	f := frontendWithSettings(t,
		pipeline.RoundTripperFunc(tagValueV2PlanResponse),
		&mockReader{metas: []*backend.BlockMeta{tagValueV2PlanMeta()}},
		nil,
		nil,
	)

	req := newTagValueV2PlanRequest(t, "{}")
	req = req.WithContext(user.InjectOrgID(req.Context(), "tenant"))
	responseWriter := httptest.NewRecorder()
	f.SearchTagsValuesV2Handler.ServeHTTP(responseWriter, req)

	response := responseWriter.Result()
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)

	actual := &tempopb.SearchTagValuesV2Response{}
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.NoError(t, jsonpb.Unmarshal(bytes.NewReader(body), actual))

	got := make([]string, 0, len(actual.TagValues))
	for _, value := range actual.TagValues {
		got = append(got, value.Value)
	}
	sort.Strings(got)

	want := make([]string, 0, tagValueV2PlanPageCount)
	for page := uint32(0); page < tagValueV2PlanPageCount; page++ {
		want = append(want, fmt.Sprintf("value-%02d", page))
	}
	require.Equal(t, want, got)
}

func TestTagValueV2ConditionalBackendPlanRemainsPageScoped(t *testing.T) {
	for _, req := range tagValueV2PlanRequests(t, `{ .service.name = "checkout" }`) {
		blockReq, err := api.ParseSearchTagValuesBlockRequestV2(req.HTTPRequest())
		require.NoError(t, err)
		require.NotZero(t, blockReq.PagesToSearch)
	}
}

func BenchmarkTagValueV2UnfilteredBackendPlan(b *testing.B) {
	sharder := newTagValueV2PlanSharder(b)
	parentRequest := newTagValueV2PlanRequest(b, "{}")
	searchReq, err := parseTagValuesRequestV2(parentRequest)
	require.NoError(b, err)

	ctx := context.Background()
	var totalRequests int
	b.ResetTimer()
	for b.Loop() {
		reqCh := make(chan pipeline.Request)
		totalJobs := sharder.backendRequests(ctx, "tenant", pipeline.NewHTTPRequest(parentRequest), searchReq, reqCh, func(error) {})

		requests := 0
		for range reqCh {
			requests++
		}
		if requests == 0 || requests < totalJobs {
			b.Fatalf("backend plan produced %d requests for %d jobs", requests, totalJobs)
		}
		totalRequests += requests
	}
	b.ReportMetric(float64(totalRequests)/float64(b.N), "backend_requests/op")
}
