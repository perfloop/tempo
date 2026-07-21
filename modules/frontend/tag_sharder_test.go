package frontend

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"testing"
	"time"

	//nolint:all deprecated

	"github.com/go-kit/log"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/modules/frontend/combiner"
	"github.com/grafana/tempo/modules/frontend/pipeline"
	"github.com/grafana/tempo/modules/overrides"
	"github.com/grafana/tempo/pkg/api"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/tempodb/backend"
)

// TestTagValueSearchRequestHashIncludesLimits ensures the frontend cache hash
// distinguishes requests that differ only by MaxTagValues / StaleValueThreshold.
// Without this, propagating those params (tempo-squad#1355) would let a request
// with a small limit serve its truncated cached response to a request with a
// larger limit.
func TestTagValueSearchRequestHashIncludesLimits(t *testing.T) {
	newReq := func(mut func(*tempopb.SearchTagValuesRequest)) *tagValueSearchRequest {
		r := tempopb.SearchTagValuesRequest{TagName: "span.foo", Query: "{ span.bar = `baz` }"}
		mut(&r)
		return &tagValueSearchRequest{request: r}
	}

	base := newReq(func(*tempopb.SearchTagValuesRequest) {})
	withLimit := newReq(func(r *tempopb.SearchTagValuesRequest) { r.MaxTagValues = 100 })
	withStale := newReq(func(r *tempopb.SearchTagValuesRequest) { r.StaleValueThreshold = 50 })

	require.NotEqual(t, base.hash(), withLimit.hash(), "hash must vary with MaxTagValues")
	require.NotEqual(t, base.hash(), withStale.hash(), "hash must vary with StaleValueThreshold")

	emptyQuery := newReq(func(r *tempopb.SearchTagValuesRequest) { r.Query = "" })
	matchAllQuery := newReq(func(r *tempopb.SearchTagValuesRequest) { r.Query = "{ true }" })
	require.Equal(t, emptyQuery.hash(), matchAllQuery.hash(), "match-all queries must share a cache key")
}

const tagValuePlanRegressionPageCount = uint32(16)

func tagValuePlanRegressionMeta() *backend.BlockMeta {
	return &backend.BlockMeta{
		StartTime:    time.Unix(100, 0),
		EndTime:      time.Unix(200, 0),
		Size_:        uint64(defaultTargetBytesPerRequest) * uint64(tagValuePlanRegressionPageCount),
		TotalRecords: tagValuePlanRegressionPageCount,
		BlockID:      backend.MustParse("00000000-0000-0000-0000-000000000456"),
		Version:      "vParquet4",
	}
}

func newTagValuePlanRegressionRequest(t testing.TB, query string) *http.Request {
	t.Helper()

	values := url.Values{
		"start": []string{"100"},
		"end":   []string{"200"},
		"q":     []string{query},
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v2/search/tag/.service.name/values?"+values.Encode(), nil)
	return mux.SetURLVars(req, map[string]string{api.MuxVarTagName: ".service.name"})
}

func tagValuePlanRegressionRequests(t testing.TB, parentRequest *http.Request, searchReq tagSearchReq) []pipeline.Request {
	t.Helper()

	o, err := overrides.NewOverrides(overrides.Config{}, nil, prometheus.NewRegistry())
	require.NoError(t, err)

	sharder := searchTagSharder{
		cfg: SearchSharderConfig{
			TargetBytesPerRequest: defaultTargetBytesPerRequest,
		},
		reader:    &mockReader{metas: []*backend.BlockMeta{tagValuePlanRegressionMeta()}},
		overrides: o,
	}

	reqCh := make(chan pipeline.Request)
	totalJobs := sharder.backendRequests(
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
	require.Equal(t, totalJobs, len(requests))
	return requests
}

func TestParseTagValuesRequestV2MarksFullBlockBackendPlan(t *testing.T) {
	parentRequest := newTagValuePlanRegressionRequest(t, "{}")
	searchReq, err := parseTagValuesRequestV2(parentRequest)
	require.NoError(t, err)

	tagValuesReq, ok := searchReq.(*tagValueSearchRequest)
	require.True(t, ok)
	require.True(t, tagValuesReq.v2)

	requests := tagValuePlanRegressionRequests(t, parentRequest, searchReq)
	require.Len(t, requests, 1)

	blockReq, err := api.ParseSearchTagValuesBlockRequestV2(requests[0].HTTPRequest())
	require.NoError(t, err)
	require.Equal(t, uint32(0), blockReq.StartPage)
	require.Equal(t, tagValuePlanRegressionPageCount, blockReq.PagesToSearch)
}

func TestTagValueV2ConditionalBackendPlanKeepsPageRanges(t *testing.T) {
	parentRequest := newTagValuePlanRegressionRequest(t, `{ .service.name = "checkout" }`)
	searchReq, err := parseTagValuesRequestV2(parentRequest)
	require.NoError(t, err)

	requests := tagValuePlanRegressionRequests(t, parentRequest, searchReq)
	require.Len(t, requests, int(tagValuePlanRegressionPageCount))
	for page, req := range requests {
		blockReq, err := api.ParseSearchTagValuesBlockRequestV2(req.HTTPRequest())
		require.NoError(t, err)
		require.Equal(t, uint32(page), blockReq.StartPage)
		require.Equal(t, uint32(1), blockReq.PagesToSearch)
	}
}

func TestTagValueV1BackendPlanKeepsPageRanges(t *testing.T) {
	parentRequest := newTagValuePlanRegressionRequest(t, "{}")
	searchReq, err := parseTagValuesRequest(parentRequest)
	require.NoError(t, err)

	requests := tagValuePlanRegressionRequests(t, parentRequest, searchReq)
	require.Len(t, requests, int(tagValuePlanRegressionPageCount))
	for page, req := range requests {
		blockReq, err := api.ParseSearchTagValuesBlockRequest(req.HTTPRequest())
		require.NoError(t, err)
		require.Equal(t, uint32(page), blockReq.StartPage)
		require.Equal(t, uint32(1), blockReq.PagesToSearch)
	}
}

type fakeReq struct {
	startValue uint32
	endValue   uint32
}

func (r *fakeReq) start() uint32 {
	return r.startValue
}

func (r *fakeReq) end() uint32 {
	return r.endValue
}

func (r *fakeReq) newWithRange(start, end uint32) tagSearchReq {
	return &fakeReq{
		startValue: start,
		endValue:   end,
	}
}

func (r *fakeReq) hash() uint64 {
	return 0
}

func (r *fakeReq) keyPrefix() string {
	return ""
}

func (r *fakeReq) buildSearchTagRequest(subR *http.Request) (*http.Request, error) {
	newReq := subR.Clone(subR.Context())
	q := subR.URL.Query()
	q.Set("start", strconv.FormatUint(uint64(r.startValue), 10))
	q.Set("end", strconv.FormatUint(uint64(r.endValue), 10))
	newReq.URL.RawQuery = q.Encode()

	return newReq, nil
}

func (r *fakeReq) buildTagSearchBlockRequest(subR *http.Request, blockID string,
	startPage int, pages int, _ *backend.BlockMeta,
) (*http.Request, error) {
	newReq := subR.Clone(subR.Context())
	q := subR.URL.Query()
	q.Set("size", "209715200")
	q.Set("blockID", blockID)
	q.Set("startPage", strconv.FormatUint(uint64(startPage), 10))
	q.Set("pagesToSearch", strconv.FormatUint(uint64(pages), 10))
	q.Set("encoding", "gzip")
	q.Set("indexPageSize", strconv.FormatUint(0, 10))
	q.Set("totalRecords", strconv.FormatUint(2, 10))
	q.Set("version", "wdwad")
	q.Set("footerSize", strconv.FormatUint(0, 10))

	newReq.URL.RawQuery = q.Encode()

	return newReq, nil
}

func TestTagsBackendRequestsDoNotHitBackendIfStartIsAfterQueryBackendAfter(t *testing.T) {
	bm := backend.NewBlockMeta("test", uuid.New(), "wdwad")
	startTime := time.Now().Add(-1 * time.Minute).Unix()
	endTime := time.Now().Unix()
	s := &searchTagSharder{
		cfg: SearchSharderConfig{
			QueryBackendAfter: 2 * time.Minute,
		},
		reader: &mockReader{metas: []*backend.BlockMeta{bm}},
	}

	r := httptest.NewRequest("GET", fmt.Sprintf("/?start=%d&end=%d", startTime, endTime), nil)
	stopCh := make(chan struct{})
	defer close(stopCh)
	reqCh := make(chan pipeline.Request)
	req := fakeReq{
		startValue: uint32(startTime),
		endValue:   uint32(endTime),
	}
	s.backendRequests(context.TODO(), "test", pipeline.NewHTTPRequest(r), &req, reqCh, func(_ error) {})

	assert.Empty(t, reqCh)
}

func TestTagsBackendRequests(t *testing.T) {
	bm := backend.NewBlockMeta("test", uuid.New(), "wdwad")
	bm.StartTime = time.Unix(100, 0)
	bm.EndTime = time.Unix(200, 0)
	bm.Size_ = defaultTargetBytesPerRequest * 2
	bm.TotalRecords = 2

	s := &searchTagSharder{
		cfg:    SearchSharderConfig{},
		reader: &mockReader{metas: []*backend.BlockMeta{bm}},
	}

	type params struct {
		start int
		end   int
	}

	tests := []struct {
		name             string
		params           *params
		expectedReqsURIs []string
		expectedError    error
	}{
		{
			name: "start and end same as block",
			params: &params{
				100, 200,
			},
			expectedReqsURIs: []string{
				"/querier?blockID=" + bm.BlockID.String() + "&encoding=gzip&end=200&footerSize=0&indexPageSize=0&pagesToSearch=1&size=209715200&start=100&startPage=0&totalRecords=2&version=wdwad",
				"/querier?blockID=" + bm.BlockID.String() + "&encoding=gzip&end=200&footerSize=0&indexPageSize=0&pagesToSearch=1&size=209715200&start=100&startPage=1&totalRecords=2&version=wdwad",
			},
			expectedError: nil,
		},
		{
			name: "start and end in block",
			params: &params{
				110, 150,
			},
			expectedReqsURIs: []string{
				"/querier?blockID=" + bm.BlockID.String() + "&encoding=gzip&end=150&footerSize=0&indexPageSize=0&pagesToSearch=1&size=209715200&start=110&startPage=0&totalRecords=2&version=wdwad",
				"/querier?blockID=" + bm.BlockID.String() + "&encoding=gzip&end=150&footerSize=0&indexPageSize=0&pagesToSearch=1&size=209715200&start=110&startPage=1&totalRecords=2&version=wdwad",
			},
			expectedError: nil,
		},
		{
			name: "start and end out of block",
			params: &params{
				10, 20,
			},
			expectedReqsURIs: make([]string, 0),
			expectedError:    nil,
		},
		{
			name:             "no params",
			expectedReqsURIs: make([]string, 0),
			expectedError:    nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := "/?"
			if tc.params != nil {
				request = fmt.Sprintf("/?start=%d&end=%d", tc.params.start, tc.params.end)
			}
			r := httptest.NewRequest("GET", request, nil)

			stopCh := make(chan struct{})
			defer close(stopCh)
			reqCh := make(chan pipeline.Request)
			req := fakeReq{}
			if tc.params != nil {
				req.startValue = uint32(tc.params.start)
				req.endValue = uint32(tc.params.end)

			}
			s.backendRequests(context.TODO(), "test", pipeline.NewHTTPRequest(r), &req, reqCh, func(err error) {
				require.Equal(t, tc.expectedError, err)
			})

			actualReqURIs := []string{}
			for r := range reqCh {
				actualReqURIs = append(actualReqURIs, r.HTTPRequest().RequestURI)
			}
			require.Equal(t, tc.expectedReqsURIs, actualReqURIs)
		})
	}
}

func TestTagsIngesterRequest(t *testing.T) {
	now := int(time.Now().Unix())
	tenMinutesAgo := int(time.Now().Add(-10 * time.Minute).Unix())
	fifteenMinutesAgo := int(time.Now().Add(-15 * time.Minute).Unix())
	twentyMinutesAgo := int(time.Now().Add(-20 * time.Minute).Unix())

	urlStartReq := "/?start="
	startPart := "&start="

	tests := []struct {
		request           string
		queryBackendAfter time.Duration
		expectedURI       string
		expectedError     error
		start             int
		end               int
	}{
		// start/end is outside queryBackendAfter
		{
			request:           "/?start=10&end=20",
			queryBackendAfter: 10 * time.Minute,
			expectedURI:       "",
			start:             10,
			end:               20,
		},
		// start/end is inside queryBackendAfter
		{
			request:           urlStartReq + strconv.Itoa(tenMinutesAgo) + "&end=" + strconv.Itoa(now),
			queryBackendAfter: 30 * time.Minute,
			expectedURI:       "/querier?end=" + strconv.Itoa(now) + startPart + strconv.Itoa(tenMinutesAgo),
			start:             tenMinutesAgo,
			end:               now,
		},
		// queryBackendAfter = 0 results in no ingester query
		{
			request: urlStartReq + strconv.Itoa(tenMinutesAgo) + "&end=" + strconv.Itoa(now),
			start:   tenMinutesAgo,
			end:     now,
		},
		// start/end = 20 - 10 mins ago - break across query backend after
		//  ingester start/End = 15 - 10 mins ago
		{
			request:           urlStartReq + strconv.Itoa(twentyMinutesAgo) + "&end=" + strconv.Itoa(tenMinutesAgo),
			queryBackendAfter: 15 * time.Minute,
			expectedURI:       "/querier?end=" + strconv.Itoa(tenMinutesAgo) + startPart + strconv.Itoa(fifteenMinutesAgo),
			start:             twentyMinutesAgo,
			end:               tenMinutesAgo,
		},
		// start/end = 10 - now mins ago - break across query backend after
		//  ingester start/End = 10 - now mins ago
		//  backend start/End = 15 - 10 mins ago
		{
			request:           urlStartReq + strconv.Itoa(tenMinutesAgo) + "&end=" + strconv.Itoa(now),
			queryBackendAfter: 15 * time.Minute,
			expectedURI:       "/querier?end=" + strconv.Itoa(now) + startPart + strconv.Itoa(tenMinutesAgo),
			start:             tenMinutesAgo,
			end:               now,
		},
		// start/end = 20 - now mins ago - break across query backend after
		//  ingester start/End = 15 - now mins ago
		//  backend start/End = 20 - 5 mins ago
		{
			request:           urlStartReq + strconv.Itoa(twentyMinutesAgo) + "&end=" + strconv.Itoa(now),
			queryBackendAfter: 15 * time.Minute,
			expectedURI:       "/querier?end=" + strconv.Itoa(now) + startPart + strconv.Itoa(fifteenMinutesAgo),
			start:             twentyMinutesAgo,
			end:               now,
		},
		{
			request:           "/?",
			queryBackendAfter: 15 * time.Minute,
			expectedURI:       "/querier?end=0&start=0",
		},
	}

	for _, tc := range tests {
		s := &searchTagSharder{
			cfg: SearchSharderConfig{
				QueryBackendAfter: tc.queryBackendAfter,
			},
		}

		req := httptest.NewRequest("GET", tc.request, nil)
		pipelineReq := pipeline.NewHTTPRequest(req)

		searchReq := fakeReq{
			startValue: uint32(tc.start),
			endValue:   uint32(tc.end),
		}

		copyReq := searchReq
		actualReq, err := s.ingesterRequest("test", pipelineReq, &searchReq)
		if tc.expectedError != nil {
			assert.Equal(t, tc.expectedError, err)
			continue
		}
		assert.NoError(t, err)
		if tc.expectedURI == "" {
			assert.Nil(t, actualReq)
		} else {
			assert.Equal(t, tc.expectedURI, actualReq.HTTPRequest().RequestURI)
		}

		// it may seem odd to test that the searchReq is not modified, but this is to prevent an issue that
		// occurs if the ingesterRequest method is changed to take a searchReq pointer
		require.True(t, reflect.DeepEqual(copyReq, searchReq))
	}
}

func TestTagsSearchSharderRoundTripBadRequest(t *testing.T) {
	next := pipeline.AsyncRoundTripperFunc[combiner.PipelineResponse](func(_ pipeline.Request) (pipeline.Responses[combiner.PipelineResponse], error) {
		return nil, nil
	})

	o, err := overrides.NewOverrides(overrides.Config{}, nil, prometheus.NewRegistry())
	require.NoError(t, err)

	sharder := newAsyncTagSharder(&mockReader{}, o, SearchSharderConfig{
		ConcurrentRequests:    defaultConcurrentRequests,
		TargetBytesPerRequest: defaultTargetBytesPerRequest,
		MostRecentShards:      defaultMostRecentShards,
		MaxDuration:           5 * time.Minute,
	}, parseTagsRequest, nil, log.NewNopLogger())
	testRT := sharder.Wrap(next)

	// no org id
	req := httptest.NewRequest("GET", "/?start=1000&end=1100", nil)
	resp, err := testRT.RoundTrip(pipeline.NewHTTPRequest(req))
	testBadRequestFromResponses(t, resp, err, "no org id")

	// start/end outside of max duration
	req = httptest.NewRequest("GET", "/?start=1000&end=1500", nil)
	req = req.WithContext(user.InjectOrgID(req.Context(), "blerg"))
	resp, err = testRT.RoundTrip(pipeline.NewHTTPRequest(req))
	testBadRequestFromResponses(t, resp, err, "range specified by start and end exceeds 5m0s. received start=1000 end=1500")

	// bad request
	req = httptest.NewRequest("GET", "/?start=asdf&end=1500", nil)
	req = req.WithContext(user.InjectOrgID(req.Context(), "blerg"))
	resp, err = testRT.RoundTrip(pipeline.NewHTTPRequest(req))
	testBadRequestFromResponses(t, resp, err, "invalid start: strconv.ParseInt: parsing \"asdf\": invalid syntax")

	// test max duration error with overrides
	o, err = overrides.NewOverrides(overrides.Config{
		Defaults: overrides.Overrides{
			Read: overrides.ReadOverrides{
				MaxSearchDuration: model.Duration(time.Minute),
			},
		},
	}, nil, prometheus.NewRegistry())
	require.NoError(t, err)

	sharder = newAsyncTagSharder(&mockReader{}, o, SearchSharderConfig{
		ConcurrentRequests:    defaultConcurrentRequests,
		TargetBytesPerRequest: defaultTargetBytesPerRequest,
		MostRecentShards:      defaultMostRecentShards,
		MaxDuration:           5 * time.Minute,
	}, parseTagsRequest, nil, log.NewNopLogger())
	testRT = sharder.Wrap(next)

	req = httptest.NewRequest("GET", "/?start=1000&end=1500", nil)
	req = req.WithContext(user.InjectOrgID(req.Context(), "blerg"))
	resp, err = testRT.RoundTrip(pipeline.NewHTTPRequest(req))
	testBadRequestFromResponses(t, resp, err, "range specified by start and end exceeds 1m0s. received start=1000 end=1500")
}
