package api

import (
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// responseWriter wraps http.ResponseWriter to capture the status code.
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// routeStats holds statistics for a specific route pattern.
type routeStats struct {
	count       int64
	totalDur    time.Duration
	minDur      time.Duration
	maxDur      time.Duration
	statusCodes map[int]int64
}

// requestStats tracks aggregate statistics for all requests.
// This provides observability into performance characteristics:
//   - Avg/min/max latencies reveal batching behavior under different loads
//   - Status code distributions help identify validation vs server errors
//   - Per-route stats show which operations dominate the workload
type requestStats struct {
	mu     sync.Mutex
	routes map[string]*routeStats
}

func newRequestStats() *requestStats {
	return &requestStats{
		routes: make(map[string]*routeStats),
	}
}

// normalizeRoute converts a full path to a route pattern.
// This aggregates stats across all keys (e.g., /kv/user:123 → /kv/)
// so we can see overall GET/PUT/DELETE performance rather than per-key metrics.
func normalizeRoute(path string) string {
	if strings.HasPrefix(path, "/kv/") {
		return "/kv/"
	}

	return path
}

// record adds a request to the statistics.
func (rs *requestStats) record(method, path string, status int, duration time.Duration) {
	route := fmt.Sprintf("%s %s", method, normalizeRoute(path))

	rs.mu.Lock()
	defer rs.mu.Unlock()

	stats, ok := rs.routes[route]
	if !ok {
		stats = &routeStats{
			statusCodes: make(map[int]int64),
			minDur:      duration,
			maxDur:      duration,
		}
		rs.routes[route] = stats
	}

	stats.count++
	stats.totalDur += duration
	if duration < stats.minDur {
		stats.minDur = duration
	}

	if duration > stats.maxDur {
		stats.maxDur = duration
	}

	stats.statusCodes[status]++
}

// fmtDuration formats a duration to 2 decimal places using the most appropriate unit.
func fmtDuration(d time.Duration) string {
	switch {
	case d < time.Microsecond:
		return fmt.Sprintf("%.2fns", float64(d))
	case d < time.Millisecond:
		return fmt.Sprintf("%.2fµs", float64(d)/float64(time.Microsecond))
	case d < time.Second:
		return fmt.Sprintf("%.2fms", float64(d)/float64(time.Millisecond))
	default:
		return fmt.Sprintf("%.2fs", float64(d)/float64(time.Second))
	}
}

// printStats prints aggregate request statistics.
func (rs *requestStats) printStats() {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if len(rs.routes) == 0 {
		return
	}

	type routeInfo struct {
		route  string
		method string
		path   string
		stats  *routeStats
	}

	routes := make([]routeInfo, 0, len(rs.routes))
	for route, stats := range rs.routes {
		parts := strings.SplitN(route, " ", 2)
		if len(parts) == 2 {
			routes = append(routes, routeInfo{
				route:  route,
				method: parts[0],
				path:   parts[1],
				stats:  stats,
			})
		}
	}

	sort.Slice(routes, func(i, j int) bool {
		return routes[i].stats.count > routes[j].stats.count
	})

	log.Println("Request Statistics:")
	for _, r := range routes {
		avgDur := r.stats.totalDur / time.Duration(r.stats.count)

		type codeCount struct {
			code  int
			count int64
		}

		codeCounts := make([]codeCount, 0, len(r.stats.statusCodes))
		for code, count := range r.stats.statusCodes {
			codeCounts = append(codeCounts, codeCount{code, count})
		}
		sort.Slice(codeCounts, func(i, j int) bool {
			if codeCounts[i].count != codeCounts[j].count {
				return codeCounts[i].count > codeCounts[j].count
			}
			return codeCounts[i].code < codeCounts[j].code
		})

		statusParts := make([]string, 0, len(codeCounts))
		for _, cc := range codeCounts {
			statusParts = append(statusParts, fmt.Sprintf("%d: %d", cc.code, cc.count))
		}
		statusStr := strings.Join(statusParts, ", ")

		log.Printf("  %s: %d requests, avg %s, min %s, max %s (%s)",
			r.route, r.stats.count, fmtDuration(avgDur), fmtDuration(r.stats.minDur), fmtDuration(r.stats.maxDur), statusStr)
	}
}

// loggingMiddleware records request statistics.
func loggingMiddleware(stats *requestStats) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			wrapped := &responseWriter{
				ResponseWriter: w,
				statusCode:     http.StatusOK,
			}

			next.ServeHTTP(wrapped, r)

			duration := time.Since(start)
			stats.record(r.Method, r.URL.Path, wrapped.statusCode, duration)
		})
	}
}
