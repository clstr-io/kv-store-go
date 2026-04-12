package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/clstr-io/key-value-go/internal/api"
)

func main() {
	log.Print("Starting Key-Value Store...")

	addr := os.Getenv("ADDR")

	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = "/app/data"
	}

	var peers []string
	if peersEnv := os.Getenv("PEERS"); peersEnv != "" {
		for _, peer := range strings.Split(peersEnv, ",") {
			peers = append(peers, fmt.Sprintf("http://%s", peer))
		}
	}

	server, err := api.New(addr, peers, dataDir)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	err = server.Serve(ctx)
	if err != nil {
		log.Println(err)
	}

	log.Print("Server stopped")
}
