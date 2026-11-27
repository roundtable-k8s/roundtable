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

func TestStreamCreatedCacheDropsOnSequenceReset(t *testing.T) {
	c := &streamCreatedCache{}
	created := time.Unix(100, 5)
	c.set("S", created, 500)
	if got, ok := c.get("S", 501); !ok || !got.Equal(created) {
		t.Fatalf("get(501) = %v, %v; want cached", got, ok)
	}
	if _, ok := c.get("S", 1); ok {
		t.Fatal("a sequence below the last seen must drop the cached creation time")
	}
	if _, ok := c.get("S", 2); ok {
		t.Fatal("the cache must stay empty until set again")
	}
}
