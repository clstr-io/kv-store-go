package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/clstr-io/key-value-go/internal/api"
	"github.com/clstr-io/key-value-go/internal/store"
)

const port = "8080"

func main() {
	log.Print("Starting Key-Value Store...")

	dir := os.Getenv("DATA_DIR")
	if dir == "" {
		dir = "/app/data"
	}

	ds, err := store.NewDiskStore(dir)
	if err != nil {
		log.Fatalf("Failed to create disk store: %v", err)
	}

	var peers []string
	peersEnv := os.Getenv("PEERS")
	if peersEnv != "" {
		for _, peer := range strings.Split(peersEnv, ",") {
			peers = append(peers, fmt.Sprintf("http://%s", peer))
		}
	}

	server := api.New(ds)

	go func() {
		err = server.Serve(":" + port)
		if err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	log.Printf("Server started on port %s", port)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT) // Graceful shutdown on SIGTERM or Ctrl+C

	<-quit
	log.Print("Shutting down...")

	// Graceful shutdown: wait up to 15s for in-flight requests to complete.
	// This should be generous for most workloads (requests typically complete
	// in <500ms), but prevents hanging indefinitely on stuck operations.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	err = server.Shutdown(ctx)
	if err != nil {
		log.Printf("Error while shutting down: %v", err)
	}

	// Print stats before closing store so we see final performance metrics.
	server.PrintStats()

	// Close flushes pending batches, snapshots state, and truncates WAL.
	err = ds.Close()
	if err != nil {
		log.Printf("Failed to close log: %v", err)
	}

	log.Print("Server stopped")
}
