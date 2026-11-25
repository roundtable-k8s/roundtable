package nats

import (
	"testing"
	"time"
)

func TestResultsTTL(t *testing.T) {
	cfg := DefaultConfig()
	if got := cfg.kvTTL(ResultsBucket); got != 30*24*time.Hour {
		t.Errorf("default results TTL = %v, want 30d", got)
	}
	cfg.ResultsTTL = 48 * time.Hour
	if got := cfg.kvTTL(ResultsBucket); got != 48*time.Hour {
		t.Errorf("configured results TTL = %v, want 48h", got)
	}
	if got := cfg.kvTTL("chain-outputs"); got != defaultKVTTL {
		t.Errorf("other bucket TTL = %v, want %v", got, defaultKVTTL)
	}
}
