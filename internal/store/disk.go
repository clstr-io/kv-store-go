package store

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
)

const (
	// maxBatchSize limits how many operations we buffer before forcing an fsync.
	maxBatchSize = 1_000
)

// LogEntry represents a single operation in the log.
type LogEntry struct {
	Op    string `json:"op"` // "set", "delete", "clear"
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

// batchRequest represents a pending write request waiting for fsync.
type batchRequest struct {
	entry   LogEntry
	errChan chan error
}

// DiskStore provides durable storage that survives restarts.
//
// Architecture:
//   - Write-ahead log (WAL): All mutations are logged before applying to memory
//   - Batching: Operations are batched and flushed together to amortize fsync cost
//   - Recovery: Load latest snapshot, then replay WAL entries
//
// Locking strategy:
//   - walMu: Serializes writes to the WAL file during batch flush
type DiskStore struct {
	memory *memoryStore

	snapshotPath string
	walPath      string
	wal          *os.File
	walMu        sync.Mutex // serializes writes to WAL file

	batchChan chan *batchRequest
	batchDone chan struct{}

	closeOnce sync.Once
}

// NewDiskStore creates a new persistent store that saves data to the specified directory.
// Any previously saved state is restored on initialization.
func NewDiskStore(dataDir string) (*DiskStore, error) {
	snapshotPath := filepath.Join(dataDir, "snapshot.json")
	logPath := filepath.Join(dataDir, "wal.jsonl")

	logFile, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	ds := &DiskStore{
		memory:       newMemoryStore(),
		snapshotPath: snapshotPath,
		walPath:      logPath,
		wal:          logFile,
		batchChan:    make(chan *batchRequest, 2*maxBatchSize),
		batchDone:    make(chan struct{}),
	}

	// Clean up any stale temp file from a previous interrupted snapshot write.
	os.Remove(snapshotPath + ".tmp")

	err = ds.loadSnapshot()
	if err != nil {
		logFile.Close()
		return nil, fmt.Errorf("failed to load snapshot: %w", err)
	}

	err = ds.replayWAL()
	if err != nil {
		logFile.Close()
		return nil, fmt.Errorf("failed to replay WAL: %w", err)
	}

	go ds.batchWriter()

	return ds, nil
}

// loadSnapshot restores the store's state from disk.
func (ds *DiskStore) loadSnapshot() error {
	data, err := os.ReadFile(ds.snapshotPath)
	if err != nil {
		if os.IsNotExist(err) {
			// No snapshot yet, start with empty store
			return nil
		}

		return fmt.Errorf("failed to read snapshot: %w", err)
	}

	var snapshot map[string]string
	err = json.Unmarshal(data, &snapshot)
	if err != nil {
		return fmt.Errorf("failed to unmarshal snapshot: %w", err)
	}

	ds.memory.data = snapshot

	return nil
}

// saveSnapshot captures the current state to disk atomically.
func (ds *DiskStore) saveSnapshot() error {
	ds.memory.mu.RLock()
	data, err := json.Marshal(ds.memory.data)
	ds.memory.mu.RUnlock()
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	return atomicWriteFile(ds.snapshotPath, data, 0644)
}

// replayWAL recovers operations from the WAL.
func (ds *DiskStore) replayWAL() error {
	f, err := os.Open(ds.walPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		return fmt.Errorf("failed to open WAL for replay: %w", err)
	}
	defer f.Close()

	lineNum := 0
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		if line == "" {
			continue
		}

		var entry LogEntry
		err := json.Unmarshal([]byte(line), &entry)
		if err != nil {
			// Fail-fast on corruption rather than silently skipping entries.
			return fmt.Errorf("corrupted WAL entry at line %d: %w", lineNum, err)
		}

		switch entry.Op {
		case "set":
			ds.memory.Set(entry.Key, entry.Value)
		case "delete":
			ds.memory.Delete(entry.Key)
		case "clear":
			ds.memory.Clear()
		default:
			return fmt.Errorf("unknown operation %q at line %d", entry.Op, lineNum)
		}
	}

	err = scanner.Err()
	if err != nil {
		return fmt.Errorf("failed to read WAL: %w", err)
	}

	return nil
}

// batchWriter runs in the background to batch WAL writes and fsync.
// It blocks until the first request arrives, then drains all immediately
// available requests before flushing. This means sequential writes each get
// their own prompt fsync, while concurrent bursts share a single fsync.
func (ds *DiskStore) batchWriter() {
	batch := make([]*batchRequest, 0, 2*maxBatchSize)

	for {
		req, ok := <-ds.batchChan
		if !ok {
			close(ds.batchDone)
			return
		}
		batch = append(batch, req)

	drain:
		for len(batch) < maxBatchSize {
			select {
			case req, ok := <-ds.batchChan:
				if !ok {
					ds.flushBatch(batch)
					close(ds.batchDone)
					return
				}

				batch = append(batch, req)
			default:
				break drain
			}
		}

		ds.flushBatch(batch)
		batch = batch[:0]
	}
}

// flushBatch writes all entries in the batch and performs a single fsync.
func (ds *DiskStore) flushBatch(batch []*batchRequest) {
	ds.walMu.Lock()
	defer ds.walMu.Unlock()

	var writeErr error
	for _, req := range batch {
		data, err := json.Marshal(req.entry)
		if err != nil {
			writeErr = fmt.Errorf("failed to marshal WAL entry: %w", err)
			break
		}

		_, err = ds.wal.Write(append(data, '\n'))
		if err != nil {
			writeErr = fmt.Errorf("failed to write to WAL: %w", err)
			break
		}
	}

	var syncErr error
	if writeErr == nil {
		syncErr = ds.wal.Sync()
		if syncErr != nil {
			syncErr = fmt.Errorf("failed to sync WAL: %w", syncErr)
		}
	}

	finalErr := writeErr
	if finalErr == nil {
		finalErr = syncErr
	}

	for _, req := range batch {
		req.errChan <- finalErr
	}
}

// appendToWAL records an operation to the WAL.
func (ds *DiskStore) appendToWAL(entry LogEntry) error {
	req := &batchRequest{entry: entry, errChan: make(chan error, 1)}
	ds.batchChan <- req
	return <-req.errChan
}

// Set adds or updates a key-value pair in the store.
func (ds *DiskStore) Set(key, value string) error {
	err := ds.appendToWAL(LogEntry{Op: "set", Key: key, Value: value})
	if err != nil {
		return err
	}

	return ds.memory.Set(key, value)
}

// Get retrieves the value associated with a given key.
func (ds *DiskStore) Get(key string) (string, error) {
	return ds.memory.Get(key)
}

// Delete removes a key-value pair from the store.
func (ds *DiskStore) Delete(key string) error {
	err := ds.appendToWAL(LogEntry{Op: "delete", Key: key})
	if err != nil {
		return err
	}

	return ds.memory.Delete(key)
}

// Clear removes all key-value pairs from the store.
func (ds *DiskStore) Clear() error {
	err := ds.appendToWAL(LogEntry{Op: "clear"})
	if err != nil {
		return err
	}

	return ds.memory.Clear()
}

// Close performs a clean shutdown of the store.
func (ds *DiskStore) Close() error {
	var closeErr error

	ds.closeOnce.Do(func() {
		close(ds.batchChan)
		<-ds.batchDone

		err := ds.saveSnapshot()
		if err != nil {
			log.Printf("Failed to save snapshot: %v", err)
			closeErr = err
		}

		err = ds.wal.Close()
		if err != nil {
			closeErr = err
			return
		}

		err = os.Truncate(ds.walPath, 0)
		if err != nil {
			log.Printf("Failed to truncate WAL: %v", err)
		}
	})

	return closeErr
}

// atomicWriteFile writes data to a file using a temp file + fsync + rename
// pattern. This ensures readers never see a partially-written file.
func atomicWriteFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	tmpPath := path + ".tmp"

	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	_, err = f.Write(data)
	if err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to write temp file: %w", err)
	}

	err = f.Sync()
	if err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to fsync temp file: %w", err)
	}

	err = f.Close()
	if err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	err = os.Rename(tmpPath, path)
	if err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("failed to open directory for fsync: %w", err)
	}
	defer d.Close()

	err = d.Sync()
	if err != nil {
		return fmt.Errorf("failed to fsync directory: %w", err)
	}

	return nil
}
