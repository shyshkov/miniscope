// Package api exposes the engine over HTTP: a JSON ingest endpoint, a
// Prometheus remote-write endpoint, and a JSON query endpoint. This is the
// ingestion-pipeline face of the POC — the part that turns the library into a
// service you can curl or point a real Prometheus at.
package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/golang/snappy"
	"github.com/vshyshkovskyi/miniscope/internal/remotewrite"
	"github.com/vshyshkovskyi/miniscope/internal/tsdb"
)

// Server fronts the persistent engine. It writes into a *tsdb.DB, so ingest
// flows all the way through: head -> sealed block file on disk -> pruned query.
// That makes this the real ingest path, not an in-RAM stand-in that loses data
// on restart.
type Server struct {
	db              *tsdb.DB
	samplesReceived atomic.Int64
}

func NewServer(db *tsdb.DB) *Server { return &Server{db: db} }

func (s *Server) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /write", s.handleJSONWrite)          // simple JSON ingest
	mux.HandleFunc("POST /api/v1/write", s.handleRemoteWrite) // Prometheus remote-write
	mux.HandleFunc("GET /query", s.handleQuery)               // selector + time range
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, "ok; samples_received=%d\n", s.samplesReceived.Load())
	})
	return mux
}

// --- JSON ingest -----------------------------------------------------------

type jsonWriteRequest struct {
	Series []struct {
		Labels  map[string]string `json:"labels"`
		Samples []struct {
			T int64   `json:"t"`
			V float64 `json:"v"`
		} `json:"samples"`
	} `json:"series"`
}

func (s *Server) handleJSONWrite(w http.ResponseWriter, r *http.Request) {
	var req jsonWriteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
		return
	}
	var n int64
	for _, ser := range req.Series {
		if len(ser.Labels) == 0 {
			http.Error(w, "series with no labels", http.StatusBadRequest)
			return
		}
		labels := tsdb.NewLabels(ser.Labels)
		for _, smp := range ser.Samples {
			if err := s.db.Append(labels, smp.T, smp.V); err != nil {
				http.Error(w, "append: "+err.Error(), http.StatusInternalServerError)
				return
			}
			n++
		}
	}
	s.samplesReceived.Add(n)
	w.WriteHeader(http.StatusNoContent)
}

// --- Prometheus remote-write ----------------------------------------------

func (s *Server) handleRemoteWrite(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	// Remote-write bodies are snappy block-compressed protobuf.
	raw, err := snappy.Decode(nil, body)
	if err != nil {
		http.Error(w, "snappy decode: "+err.Error(), http.StatusBadRequest)
		return
	}
	series, err := remotewrite.Unmarshal(raw)
	if err != nil {
		http.Error(w, "protobuf decode: "+err.Error(), http.StatusBadRequest)
		return
	}
	var n int64
	for _, ts := range series {
		lm := make(map[string]string, len(ts.Labels))
		for _, l := range ts.Labels {
			lm[l.Name] = l.Value
		}
		if len(lm) == 0 {
			continue
		}
		labels := tsdb.NewLabels(lm)
		for _, smp := range ts.Samples {
			if err := s.db.Append(labels, smp.TimestampMs, smp.Value); err != nil {
				http.Error(w, "append: "+err.Error(), http.StatusInternalServerError)
				return
			}
			n++
		}
	}
	s.samplesReceived.Add(n)
	w.WriteHeader(http.StatusNoContent)
}

// --- Query -----------------------------------------------------------------

type jsonSeries struct {
	Labels  map[string]string `json:"labels"`
	Samples []struct {
		T int64   `json:"t"`
		V float64 `json:"v"`
	} `json:"samples"`
}

// handleQuery: GET /query?match=__name__=cpu&match=host=host-07&start=..&end=..
// Repeated match params are AND-ed; start/end are unix-millis (default: all time).
func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var sel tsdb.Selector
	for _, m := range q["match"] {
		name, value, ok := strings.Cut(m, "=")
		if !ok {
			http.Error(w, "match must be name=value, got "+strconv.Quote(m), http.StatusBadRequest)
			return
		}
		sel = append(sel, tsdb.Matcher{Name: name, Value: value})
	}
	mint, maxt := int64(-1<<63), int64(1<<63-1)
	if v := q.Get("start"); v != "" {
		mint, _ = strconv.ParseInt(v, 10, 64)
	}
	if v := q.Get("end"); v != "" {
		maxt, _ = strconv.ParseInt(v, 10, 64)
	}

	res, scanned, pruned := s.db.Query(sel, mint, maxt)
	out := make([]jsonSeries, 0, len(res))
	for _, sr := range res {
		js := jsonSeries{Labels: map[string]string{}}
		for _, l := range sr.Labels {
			js.Labels[l.Name] = l.Value
		}
		for _, p := range sr.Samples {
			js.Samples = append(js.Samples, struct {
				T int64   `json:"t"`
				V float64 `json:"v"`
			}{p.T, p.V})
		}
		out = append(out, js)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"series": out,
		// Expose the pruning outcome so a caller can see block-skipping work:
		// how many on-disk blocks the query had to read vs. skipped on metadata.
		"stats": map[string]int{"blocks_scanned": scanned, "blocks_pruned": pruned},
	})
}
