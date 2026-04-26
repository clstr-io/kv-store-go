package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"time"

	"github.com/clstr-io/key-value-go/internal/raft"
	"github.com/clstr-io/key-value-go/internal/store"
)

const (
	// maxValueSize limits the size of values to 10 MB.
	maxValueSize = 10 * 1024 * 1024

	shutdownTimeout = 5 * time.Second
)

var (
	// keyPattern validates that keys contain only alphanumeric plus : _ . - characters.
	// This prevents path traversal issues.
	keyPattern = regexp.MustCompile(`^[a-zA-Z0-9:_.-]+$`)
)

// Server represents the key-value server.
type Server struct {
	api  *http.Server
	node *raft.Node
}

// New creates a new instance of the Server.
func New(id string, peers []string, dataDir string) (*Server, error) {
	n, err := raft.NewNode(id, peers, dataDir)
	if err != nil {
		return nil, err
	}

	s := &Server{node: n}
	s.setupRoutes()

	return s, nil
}

func (s *Server) setupRoutes() {
	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Path[len("/kv/"):]
		if len(key) == 0 {
			http.Error(w, "key cannot be empty", http.StatusBadRequest)
			return
		}

		if !keyPattern.MatchString(key) {
			http.Error(w, "key contains invalid characters", http.StatusBadRequest)
			return
		}

		switch r.Method {
		case http.MethodPut:
			s.handleSet(w, r)
		case http.MethodGet:
			s.handleGet(w, r)
		case http.MethodDelete:
			s.handleDelete(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/clear", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodDelete:
			s.handleClear(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/raft/request-vote", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			s.handleVoteRequest(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/raft/append-entries", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			s.handleAppendEntries(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/cluster/info", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			s.handleClusterInfo(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	s.api = &http.Server{Addr: ":8080", Handler: mux}
}

// redirectIfFollower redirects the request to the leader if this node is not the leader.
// Returns true if a redirect was sent.
func (s *Server) redirectIfFollower(w http.ResponseWriter, r *http.Request) bool {
	leaderID, isLeader := s.node.LeaderID()
	if isLeader {
		return false
	}

	if leaderID == "" {
		http.Error(w, "no leader elected", http.StatusServiceUnavailable)
		return true
	}

	location := "http://" + leaderID + r.RequestURI
	log.Printf("Redirecting to %s", location)
	w.Header().Set("Location", location)
	w.WriteHeader(http.StatusTemporaryRedirect)
	return true
}

// handleSet handles the HTTP PUT request for setting a key-value pair.
func (s *Server) handleSet(w http.ResponseWriter, r *http.Request) {
	if s.redirectIfFollower(w, r) {
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxValueSize)

	value, err := io.ReadAll(r.Body)
	if err != nil {
		msg := fmt.Sprintf("cannot read request body: %v", err)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}
	if len(value) == 0 {
		http.Error(w, "value cannot be empty", http.StatusBadRequest)
		return
	}

	key := r.URL.Path[len("/kv/"):]
	err = s.node.Set(key, string(value))
	if err != nil {
		msg := fmt.Sprintf("cannot set key: %v", err)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
}

// handleGet handles the HTTP GET request for retrieving a key-value pair.
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	if s.redirectIfFollower(w, r) {
		return
	}

	key := r.URL.Path[len("/kv/"):]
	value, err := s.node.Get(key)
	if err != nil {
		if errors.Is(err, &store.NotFoundError{}) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		msg := fmt.Sprintf("cannot get key: %v", err)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte(value))
}

// handleDelete handles the HTTP DELETE request for deleting a key-value pair.
func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	if s.redirectIfFollower(w, r) {
		return
	}

	key := r.URL.Path[len("/kv/"):]
	err := s.node.Delete(key)
	if err != nil {
		msg := fmt.Sprintf("cannot delete key: %v", err)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleClear handles the HTTP POST request for clearing the store.
func (s *Server) handleClear(w http.ResponseWriter, r *http.Request) {
	if s.redirectIfFollower(w, r) {
		return
	}

	err := s.node.Clear()
	if err != nil {
		msg := fmt.Sprintf("cannot clear store: %v", err)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleVoteRequest(w http.ResponseWriter, r *http.Request) {
	var req raft.VoteRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	resp := s.node.Vote(req)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	var req raft.AppendEntriesRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	resp := s.node.AppendEntries(req)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleClusterInfo(w http.ResponseWriter, _ *http.Request) {
	info := s.node.Info()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(info)
}

// Serve starts the HTTP server and the Raft loop, blocking until ctx is cancelled.
func (s *Server) Serve(ctx context.Context) error {
	go s.node.Loop(ctx)

	go func() {
		err := s.api.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			log.Printf("Server error: %v", err)
		}
	}()

	log.Printf("Listening on :8080")

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	err := s.node.Shutdown()
	if err != nil {
		return err
	}

	return s.api.Shutdown(shutdownCtx)
}
