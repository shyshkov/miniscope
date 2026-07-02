// Command metricsd runs the miniscope ingestion + query service.
//
//	go run ./cmd/metricsd            # listens on :9090
//	curl -XPOST :9090/write -d '{"series":[{"labels":{"__name__":"up","host":"h1"},"samples":[{"t":1,"v":1}]}]}'
//	curl ':9090/query?match=__name__=up&match=host=h1'
//
// It also accepts Prometheus remote-write at POST /api/v1/write, so a real
// Prometheus can be pointed at it with:
//
//	remote_write:
//	  - url: http://localhost:9090/api/v1/write
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os/signal"
	"syscall"

	"github.com/vshyshkovskyi/miniscope/internal/api"
	"github.com/vshyshkovskyi/miniscope/internal/tsdb"
)

func main() {
	addr := flag.String("addr", ":9090", "listen address")
	dir := flag.String("dir", "./miniscope-data", "block storage directory (stand-in for object storage)")
	blockDur := flag.Int64("block-dur", 3600_000, "flush window in ms; head seals to a block file each window")
	flag.Parse()

	// Open the persistent engine. Ingest now flows head -> sealed block file ->
	// pruned query, and survives restart by reloading blocks from -dir.
	db, err := tsdb.Open(*dir, *blockDur)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	srv := api.NewServer(db)

	log.Printf("miniscope metricsd listening on %s (data dir %s, block window %dms)", *addr, *dir, *blockDur)
	log.Printf("  POST /write          (json)")
	log.Printf("  POST /api/v1/write   (prometheus remote-write)")
	log.Printf("  GET  /query?match=name=value&start=&end=")

	httpSrv := &http.Server{Addr: *addr, Handler: srv.Routes()}

	// Flush the in-memory head to disk on Ctrl-C so the trailing window isn't
	// lost — the durability guarantee a real ingester must make on shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		log.Printf("shutting down: flushing head to disk...")
		if err := db.Flush(); err != nil {
			log.Printf("flush on shutdown: %v", err)
		}
		httpSrv.Close()
	}()

	if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
	db.Close()
}
