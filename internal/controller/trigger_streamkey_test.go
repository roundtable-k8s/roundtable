package controller

import (
	"testing"
	"time"
)

func TestStreamSeqKeyOf(t *testing.T) {
	a := streamSeqKeyOf(time.Unix(100, 5), 1)
	b := streamSeqKeyOf(time.Unix(200, 5), 1)
	if a == b || a != "100000000005/1" {
		t.Fatalf("keys %q %q: a recreated stream must change the key", a, b)
	}
}
