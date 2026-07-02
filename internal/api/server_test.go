package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/snappy"
	"github.com/vshyshkovskyi/miniscope/internal/remotewrite"
	"github.com/vshyshkovskyi/miniscope/internal/tsdb"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	// Short flush window so tests exercise the head -> on-disk-block path, not
	// just the in-memory head.
	db, err := tsdb.Open(t.TempDir(), 60_000)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return httptest.NewServer(NewServer(db).Routes())
}

func TestJSONWriteThenQuery(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	body := `{"series":[{"labels":{"__name__":"cpu","host":"h1"},
		"samples":[{"t":1000,"v":10},{"t":2000,"v":11},{"t":3000,"v":12}]}]}`
	resp, err := http.Post(ts.URL+"/write", "application/json", bytes.NewBufferString(body))
	if err != nil || resp.StatusCode != http.StatusNoContent {
		t.Fatalf("write: err=%v status=%v", err, resp.StatusCode)
	}

	got := query(t, ts.URL+"/query?match=__name__=cpu&match=host=h1&start=1500&end=3000")
	if len(got) != 1 {
		t.Fatalf("want 1 series, got %d", len(got))
	}
	if n := len(got[0].Samples); n != 2 { // start=1500 prunes t=1000
		t.Fatalf("want 2 samples in [1500,3000], got %d", n)
	}
}

func TestRemoteWriteThenQuery(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	// Build a real remote-write payload: protobuf-encode, then snappy-compress.
	series := []remotewrite.TimeSeries{{
		Labels: []remotewrite.Label{
			{Name: "__name__", Value: "http_requests_total"},
			{Name: "method", Value: "GET"},
		},
		Samples: []remotewrite.Sample{
			{Value: 100, TimestampMs: 1000},
			{Value: 105, TimestampMs: 2000},
		},
	}}
	payload := snappy.Encode(nil, remotewrite.Marshal(series))

	resp, err := http.Post(ts.URL+"/api/v1/write", "application/x-protobuf", bytes.NewReader(payload))
	if err != nil || resp.StatusCode != http.StatusNoContent {
		t.Fatalf("remote-write: err=%v status=%v", err, resp.StatusCode)
	}

	got := query(t, ts.URL+"/query?match=__name__=http_requests_total&match=method=GET")
	if len(got) != 1 {
		t.Fatalf("want 1 series, got %d", len(got))
	}
	if len(got[0].Samples) != 2 {
		t.Fatalf("want 2 samples, got %d", len(got[0].Samples))
	}
	if got[0].Samples[1].V != 105 {
		t.Fatalf("value round-trip failed: got %v want 105", got[0].Samples[1].V)
	}
}

type respSeries struct {
	Labels  map[string]string `json:"labels"`
	Samples []struct {
		T int64   `json:"t"`
		V float64 `json:"v"`
	} `json:"samples"`
}

func query(t *testing.T, url string) []respSeries {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("query: err=%v status=%v", err, resp.StatusCode)
	}
	defer resp.Body.Close()
	var out struct {
		Series []respSeries `json:"series"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode query response: %v", err)
	}
	return out.Series
}
