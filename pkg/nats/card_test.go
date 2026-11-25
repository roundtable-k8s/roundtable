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
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

func connect(t *testing.T) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(runJS(t).ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func TestCardStore(t *testing.T) {
	nc := connect(t)
	store, err := NewCardStore(nc, "test-cards")
	if err != nil {
		t.Fatal(err)
	}

	js, _ := nc.JetStream()
	kv, err := js.KeyValue("test-cards")
	if err != nil {
		t.Fatal(err)
	}
	status, err := kv.Status()
	if err != nil {
		t.Fatal(err)
	}
	if status.History() != 1 || status.TTL() != 0 {
		t.Fatalf("bucket history=%d ttl=%s, want 1 and none", status.History(), status.TTL())
	}

	if card, err := store.Get("dev.galahad"); err != nil || card != nil {
		t.Fatalf("Get missing = %v, %v; want nil, nil", card, err)
	}
	card := &AgentCard{
		SchemaVersion: CardSchemaVersion, Name: "galahad",
		RoundTable: CardMeta{Table: "dev", Knight: "galahad"},
	}
	for _, key := range []string{"dev.galahad", "dev.kay", "dev-2.bors", "devx.x"} {
		if err := store.Put(key, card); err != nil {
			t.Fatal(err)
		}
	}
	got, err := store.Get("dev.galahad")
	if err != nil || got == nil || got.Name != "galahad" {
		t.Fatalf("Get = %+v, %v", got, err)
	}

	keys, err := store.TableKeys("dev")
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"dev.galahad", "dev.kay"}; !reflect.DeepEqual(keys, want) {
		t.Fatalf("TableKeys = %v, want %v", keys, want)
	}

	if err := store.Delete("dev.kay"); err != nil {
		t.Fatal(err)
	}
	// Deleting a missing key is a no-op and leaves no delete marker.
	if err := store.Delete("dev.nobody"); err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Get("dev.nobody"); err != nats.ErrKeyNotFound {
		t.Fatalf("missing key after Delete: %v", err)
	}
	keys, _ = store.TableKeys("dev")
	if want := []string{"dev.galahad"}; !reflect.DeepEqual(keys, want) {
		t.Fatalf("TableKeys after delete = %v, want %v", keys, want)
	}

	// Reopening the existing bucket works.
	if _, err := NewCardStore(nc, "test-cards"); err != nil {
		t.Fatal(err)
	}
}

func TestSameCardIgnoresUpdatedAt(t *testing.T) {
	a := &AgentCard{Name: "a", RoundTable: CardMeta{Model: "m", UpdatedAt: "2026-01-01T00:00:00Z"}}
	b := &AgentCard{Name: "a", RoundTable: CardMeta{Model: "m", UpdatedAt: "2026-02-01T00:00:00Z"}}
	if !SameCard(a, b) {
		t.Fatal("cards differing only in updatedAt should be the same")
	}
	b.RoundTable.Model = "other"
	if SameCard(a, b) {
		t.Fatal("cards with different models should differ")
	}
	if SameCard(nil, a) || !SameCard(nil, nil) {
		t.Fatal("nil handling")
	}
}

func addKnightService(t *testing.T, nc *nats.Conn, table, knight string, skills []RuntimeSkill) {
	t.Helper()
	meta := map[string]string{"table": table, "knight": knight}
	if skills != nil {
		data, _ := json.Marshal(skills)
		meta["skills"] = string(data)
	}
	svc, err := micro.AddService(nc, micro.Config{Name: KnightServiceName, Version: "1.0.0", Metadata: meta})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = svc.Stop() })
}

func TestRequestServiceInfoAndKnightSkills(t *testing.T) {
	nc := connect(t)

	// No responders: empty, no error, well before the timeout.
	start := time.Now()
	infos, err := RequestServiceInfo(nc, KnightServiceName, 2*time.Second)
	if err != nil || len(infos) != 0 {
		t.Fatalf("no responders: %v, %v", infos, err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("no responders took %s", time.Since(start))
	}

	addKnightService(t, nc, "dev", "Galahad", []RuntimeSkill{
		{Name: "cve-triage", Description: "Triage CVEs", Category: "security"},
	})
	addKnightService(t, nc, "dev", "Galahad", []RuntimeSkill{
		{Name: "brief", Description: "Daily brief", Category: "security"},
		{Name: "cve-triage", Description: "Triage CVEs", Category: "security"},
	})
	addKnightService(t, nc, "other", "galahad", []RuntimeSkill{{Name: "wrong-table"}})
	addKnightService(t, nc, "", "kay", []RuntimeSkill{{Name: "cook"}})
	addKnightService(t, nc, "dev", "bors", nil) // runtime without skills metadata

	infos, err = RequestServiceInfo(nc, KnightServiceName, 300*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 5 {
		t.Fatalf("got %d INFO responses, want 5", len(infos))
	}

	skills, ok := KnightSkills(infos, "dev", "galahad")
	want := []RuntimeSkill{
		{Name: "brief", Description: "Daily brief", Category: "security"},
		{Name: "cve-triage", Description: "Triage CVEs", Category: "security"},
	}
	if !ok || !reflect.DeepEqual(skills, want) {
		t.Fatalf("galahad skills = %v, %v; want %v", skills, ok, want)
	}
	// An empty table in the metadata matches any table.
	if skills, ok := KnightSkills(infos, "dev", "kay"); !ok || len(skills) != 1 || skills[0].Name != "cook" {
		t.Fatalf("kay skills = %v, %v", skills, ok)
	}
	if _, ok := KnightSkills(infos, "dev", "bors"); ok {
		t.Fatal("a knight without skills metadata must not count as reported")
	}
	if _, ok := KnightSkills(infos, "dev", "lancelot"); ok {
		t.Fatal("lancelot did not answer")
	}
}

func TestCardsBucketFromEnv(t *testing.T) {
	t.Setenv(CardsBucketEnv, "")
	if got := CardsBucketFromEnv(); got != DefaultCardsBucket {
		t.Fatalf("default = %q", got)
	}
	t.Setenv(CardsBucketEnv, "my-cards")
	if got := CardsBucketFromEnv(); got != "my-cards" {
		t.Fatalf("override = %q", got)
	}
}
