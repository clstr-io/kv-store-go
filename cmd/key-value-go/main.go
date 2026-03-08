package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/clstr-io/key-value-go/internal/api"
	"github.com/clstr-io/key-value-go/internal/store"
)

func main() {
	log.Print("starting Key-Value Store...")

	port := flag.String("port", "", "Port to run the server on")
	workingDir := flag.String("working-dir", "", "Directory for data persistence")
	flag.Parse()

	if *port == "" {
		log.Fatal("--port parameter is required")
	}

	if *workingDir == "" {
		log.Fatal("--working-dir parameter is required")
	}

	ds, err := store.NewDiskStore(*workingDir)
	if err != nil {
		log.Fatalf("failed to create disk store: %v", err)
	}

	server := api.New(ds)

	go func() {
		err = server.Serve(":" + *port)
		if err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	log.Printf("server started on port %s", *port)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT) // Graceful shutdown on SIGTERM or Ctrl+C

	<-quit
	log.Print("shutting down...")

	// Graceful shutdown: wait up to 15s for in-flight requests to complete.
	// This should be generous for most workloads (requests typically complete
	// in <500ms), but prevents hanging indefinitely on stuck operations.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	err = server.Shutdown(ctx)
	if err != nil {
		log.Printf("error while shutting down: %v", err)
	}

	// Print stats before closing store so we see final performance metrics.
	server.PrintStats()

	// Close flushes pending batches, snapshots state, and truncates WAL.
	err = ds.Close()
	if err != nil {
		log.Printf("failed to close log: %v", err)
	}

	log.Print("server stopped")
}
