package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"

	"github.com/clstr-io/key-value-go/internal/store"
)

const (
	// maxValueSize limits the size of values to prevent DoS attacks (10 MB).
	maxValueSize = 10 * 1024 * 1024
)

var (
	// keyPattern validates that keys contain only alphanumeric plus : _ . - characters.
	// This prevents path traversal issues.
	keyPattern = regexp.MustCompile(`^[a-zA-Z0-9:_.-]+$`)
)

// Store defines the interface for a generic key-value store.
type Store interface {
	// Set adds or updates a key-value pair in the store.
	Set(key string, value string) error

	// Get retrieves the value associated with a given key.
	Get(key string) (string, error)

	// Delete removes a key-value pair from the store.
	Delete(key string) error

	// Clear removes all key-value pairs from the store.
	Clear() error

	// Close performs a clean shutdown of the store.
	Close() error
}

// Server represents the key-value server.
type Server struct {
	api   *http.Server
	store Store
	stats *requestStats
}

// New creates a new instance of the Server.
func New(store Store) *Server {
	return &Server{
		store: store,
		stats: newRequestStats(),
	}
}

// Serve starts the HTTP server and handles key-value store operations.
func (s *Server) Serve(addr string) error {
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
			s.set(w, r)
		case http.MethodGet:
			s.get(w, r)
		case http.MethodDelete:
			s.delete(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/clear", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodDelete:
			s.clear(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	s.api = &http.Server{Addr: addr, Handler: loggingMiddleware(s.stats)(mux)}

	return s.api.ListenAndServe()
}

// set handles the HTTP PUT request for setting a key-value pair.
func (s *Server) set(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxValueSize)

	value, err := io.ReadAll(r.Body)
	if err != nil {
		msg := fmt.Sprintf("unable to read request body: %v", err)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}
	if len(value) == 0 {
		http.Error(w, "value cannot be empty", http.StatusBadRequest)
		return
	}

	key := r.URL.Path[len("/kv/"):]
	err = s.store.Set(key, string(value))
	if err != nil {
		msg := fmt.Sprintf("unable to set key: %v", err)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// get handles the HTTP GET request for retrieving a key-value pair.
func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[len("/kv/"):]
	value, err := s.store.Get(key)
	if err != nil {
		if errors.Is(err, &store.NotFoundError{}) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		msg := fmt.Sprintf("unable to get key: %v", err)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	w.Write([]byte(value))
}

// delete handles the HTTP DELETE request for deleting a key-value pair.
func (s *Server) delete(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[len("/kv/"):]
	err := s.store.Delete(key)
	if err != nil {
		msg := fmt.Sprintf("unable to delete key: %v", err)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// clear handles the HTTP POST request for clearing the store.
func (s *Server) clear(w http.ResponseWriter, _ *http.Request) {
	err := s.store.Clear()
	if err != nil {
		msg := fmt.Sprintf("unable to clear store: %v", err)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// Shutdown gracefully shuts down the server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.api.Shutdown(ctx)
}

// PrintStats prints aggregate request statistics.
func (s *Server) PrintStats() {
	s.stats.printStats()
}
