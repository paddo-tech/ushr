// Package ledger is an append-only store of completed ushr jobs. The controller
// appends a record per workflow_job webhook; `ushr cost` reads them to total
// runner-minutes without re-scanning GitHub.
package ledger

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Record is one completed job served by an ushr runner.
type Record struct {
	JobID       int64     `json:"job_id"`
	Org         string    `json:"org"`
	Repo        string    `json:"repo"`
	Workflow    string    `json:"workflow,omitempty"`
	RunID       int64     `json:"run_id,omitempty"`
	Conclusion  string    `json:"conclusion,omitempty"`
	Labels      []string  `json:"labels"`
	RunnerName  string    `json:"runner_name"`
	CreatedAt   time.Time `json:"created_at,omitzero"`
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
}

// Ledger appends records as JSON lines to a file. Append is safe for concurrent
// use; the file is the single source of truth, opened per write.
type Ledger struct {
	mu   sync.Mutex
	path string
}

// DefaultPath is where the ledger lives when config doesn't set webhook.ledger_path.
func DefaultPath() string {
	return filepath.Join(os.Getenv("HOME"), ".local", "share", "ushr", "jobs.jsonl")
}

// PathOrDefault resolves a configured ledger path, falling back to DefaultPath.
func PathOrDefault(configured string) string {
	if configured != "" {
		return configured
	}
	return DefaultPath()
}

// Open returns a Ledger writing to path, creating the parent directory.
func Open(path string) (*Ledger, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir ledger dir: %w", err)
	}
	return &Ledger{path: path}, nil
}

// Append writes one record as a JSON line.
func (l *Ledger) Append(r Record) error {
	line, err := json.Marshal(r)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	f, err := os.OpenFile(l.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open ledger: %w", err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("append ledger: %w", err)
	}
	return nil
}

// Read returns every record. A missing file is an empty ledger, not an error.
func Read(path string) ([]Record, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var out []Record
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var r Record
		if err := json.Unmarshal(line, &r); err != nil {
			return nil, fmt.Errorf("parse ledger line: %w", err)
		}
		out = append(out, r)
	}
	return out, sc.Err()
}
