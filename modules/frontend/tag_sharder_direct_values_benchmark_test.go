package frontend

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/mux"

	"github.com/grafana/tempo/modules/frontend/pipeline"
	"github.com/grafana/tempo/pkg/api"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/tempodb/backend"
)

const (
	directValuesBenchmarkTargetBytes = 1024
	directValuesBenchmarkPages       = 32
)

// BenchmarkTagValueSharderDirectBlock measures planning an unfiltered V2
// tag-value lookup over a block that would otherwise produce 32 page jobs.
func BenchmarkTagValueSharderDirectBlock(b *testing.B) {
	req := httptest.NewRequest("GET", "http://tempo/api/v2/search/tag/span.foo/values?start=100&end=200&q=%7B%7D", nil)
	req = mux.SetURLVars(req, map[string]string{api.MuxVarTagName: "span.foo"})

	searchReq, err := parseTagValuesRequestV2(req)
	if err != nil {
		b.Fatal(err)
	}

	benchmarkTagSharderRequests(b, searchReq, directValuesBenchmarkPages)
}

// BenchmarkTagValueSharderPageFanoutWidth keeps the regular tag-name page plan
// as a controlled width sweep. The planner should emit one request per page
// when no direct-value plan is selected.
func BenchmarkTagValueSharderPageFanoutWidth(b *testing.B) {
	for _, pages := range []int{1, 8, directValuesBenchmarkPages} {
		b.Run(fmt.Sprintf("pages_%d", pages), func(b *testing.B) {
			benchmarkTagSharderRequests(b, &tagsSearchRequest{
				request: tempopb.SearchTagsRequest{Start: 100, End: 200},
			}, pages)
		})
	}
}

func benchmarkTagSharderRequests(b *testing.B, searchReq tagSearchReq, pages int) {
	meta := &backend.BlockMeta{
		BlockID:      backend.MustParse("00000000-0000-0000-0000-000000000032"),
		StartTime:    time.Unix(100, 0),
		EndTime:      time.Unix(200, 0),
		Size_:        uint64(pages * directValuesBenchmarkTargetBytes),
		TotalRecords: uint32(pages),
	}
	sharder := searchTagSharder{
		cfg:    SearchSharderConfig{TargetBytesPerRequest: directValuesBenchmarkTargetBytes},
		reader: &mockReader{metas: []*backend.BlockMeta{meta}},
	}
	parent := pipeline.NewHTTPRequest(httptest.NewRequest("GET", "http://tempo/querier", nil))
	ctx := context.Background()
	errFn := func(err error) { panic(err) }

	requests := 0
	b.ResetTimer()
	for b.Loop() {
		reqCh := make(chan pipeline.Request)
		planned := sharder.backendRequests(ctx, "tenant", parent, searchReq, reqCh, errFn)

		emitted := 0
		requestURIBytes := 0
		for req := range reqCh {
			emitted++
			requestURIBytes += len(req.HTTPRequest().RequestURI)
		}
		if emitted != planned {
			b.Fatalf("planned %d backend requests, emitted %d", planned, emitted)
		}
		if requestURIBytes == 0 {
			b.Fatal("backend planner emitted an empty request URI")
		}
		requests += emitted
	}

	b.ReportMetric(float64(requests)/float64(b.N), "backend_requests/op")
}
