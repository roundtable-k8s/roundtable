/*
Copyright 2026 dapperdivers.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package nats

import (
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// CredsEnv is the environment variable that names the NATS credentials file.
// The --nats-creds flag of the operator and the gateway wins over it.
const CredsEnv = "NATS_CREDS_FILE"

// CredsOption returns the connection option that authenticates with the NATS
// credentials file (.creds, nkey and JWT) at path. An empty path returns nil
// (no credentials). A file that cannot be read is an error naming the path;
// it never degrades to an unauthenticated connection.
func CredsOption(path string) (nats.Option, error) {
	if path == "" {
		return nil, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("NATS credentials file %q: %w", path, err)
	}
	_ = f.Close()
	return nats.UserCredentials(path), nil
}

// Config holds NATS connection configuration.
type Config struct {
	// URL is the NATS server URL (e.g., "nats://nats.database.svc:4222").
	URL string

	// CredsFile is the path of a NATS credentials file. Empty means no
	// credentials.
	CredsFile string

	// RetryOnFailedConnect enables automatic retry on initial connection failure.
	RetryOnFailedConnect bool

	// MaxReconnects is the maximum number of reconnect attempts. -1 for infinite.
	MaxReconnects int

	// ReconnectWait is the duration to wait between reconnect attempts.
	ReconnectWait time.Duration

	// ResultsTTL is how long entries of the mission-results KV bucket are
	// kept. Zero means DefaultResultsTTL.
	ResultsTTL time.Duration
}

const (
	// ResultsBucket is the NATS KV bucket holding retained mission results.
	ResultsBucket = "mission-results"

	// DefaultResultsTTL is the retention of ResultsBucket when none is set.
	DefaultResultsTTL = 30 * 24 * time.Hour

	// defaultKVTTL is the retention of every other KV bucket.
	defaultKVTTL = 30 * 24 * time.Hour
)

// ResultsTTLOrDefault returns ResultsTTL, or DefaultResultsTTL when unset.
func (c Config) ResultsTTLOrDefault() time.Duration {
	if c.ResultsTTL > 0 {
		return c.ResultsTTL
	}
	return DefaultResultsTTL
}

// kvTTL returns the TTL a KV bucket should have.
func (c Config) kvTTL(bucket string) time.Duration {
	if bucket == ResultsBucket {
		return c.ResultsTTLOrDefault()
	}
	return defaultKVTTL
}

// DefaultConfig returns a Config with sensible defaults for the Round Table operator.
func DefaultConfig() Config {
	return Config{
		URL:                  "nats://nats.database.svc:4222",
		RetryOnFailedConnect: true,
		MaxReconnects:        -1, // Infinite reconnects
		ReconnectWait:        2 * time.Second,
	}
}

// StreamConfig defines JetStream stream configuration.
type StreamConfig struct {
	// Name is the stream name (e.g., "fleet_a_tasks").
	Name string

	// Subjects is the list of subjects this stream captures (e.g., ["fleet-a.tasks.>"]).
	Subjects []string

	// Retention policy for message retention.
	Retention RetentionPolicy

	// MaxAge is the maximum age of messages in the stream (0 = unlimited).
	MaxAge time.Duration

	// MaxMsgs is the maximum number of messages (0 = unlimited).
	MaxMsgs int64

	// Storage type (File or Memory).
	Storage StorageType

	// Discard policy when limits are exceeded.
	Discard DiscardPolicy
}

// RetentionPolicy defines how messages are retained.
type RetentionPolicy string

const (
	// RetentionLimits retains messages based on limits (MaxAge, MaxMsgs).
	RetentionLimits RetentionPolicy = "Limits"

	// RetentionWorkQueue auto-deletes messages after acknowledgment.
	// Recommended for ephemeral mission tables.
	RetentionWorkQueue RetentionPolicy = "WorkQueue"

	// RetentionInterest retains messages as long as there are consumers.
	RetentionInterest RetentionPolicy = "Interest"
)

// ToNATS converts RetentionPolicy to jetstream.RetentionPolicy.
func (r RetentionPolicy) ToNATS() jetstream.RetentionPolicy {
	switch r {
	case RetentionLimits:
		return jetstream.LimitsPolicy
	case RetentionWorkQueue:
		return jetstream.WorkQueuePolicy
	case RetentionInterest:
		return jetstream.InterestPolicy
	default:
		return jetstream.WorkQueuePolicy
	}
}

// StorageType defines stream storage backend.
type StorageType string

const (
	// StorageFile uses file-based storage.
	StorageFile StorageType = "File"

	// StorageMemory uses memory-based storage (faster but not persistent).
	StorageMemory StorageType = "Memory"
)

// ToNATS converts StorageType to jetstream.StorageType.
func (s StorageType) ToNATS() jetstream.StorageType {
	switch s {
	case StorageMemory:
		return jetstream.MemoryStorage
	default:
		return jetstream.FileStorage
	}
}

// DiscardPolicy defines what happens when stream limits are exceeded.
type DiscardPolicy string

const (
	// DiscardOld removes the oldest messages when limits are hit.
	DiscardOld DiscardPolicy = "Old"

	// DiscardNew rejects new messages when limits are hit.
	DiscardNew DiscardPolicy = "New"
)

// ToNATS converts DiscardPolicy to jetstream.DiscardPolicy.
func (d DiscardPolicy) ToNATS() jetstream.DiscardPolicy {
	switch d {
	case DiscardNew:
		return jetstream.DiscardNew
	default:
		return jetstream.DiscardOld
	}
}
