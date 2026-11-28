package main

import (
	"testing"

	"github.com/nats-io/nats.go"
)

func TestGatewayNATSOptionsUnchangedWithoutCreds(t *testing.T) {
	if got := len(gatewayNATSOptions(nil)); got != 4 {
		t.Fatalf("options without creds = %d, want 4", got)
	}
	if got := len(gatewayNATSOptions(nats.UserCredentials("/x"))); got != 5 {
		t.Fatalf("options with creds = %d, want 5", got)
	}
}
