package nats

import (
	"encoding/json"
	"strings"
	"testing"
)

// natsSubjectMatches implements NATS wildcard matching for '*' and '>'.
func natsSubjectMatches(pattern, subject string) bool {
	p, s := strings.Split(pattern, "."), strings.Split(subject, ".")
	for i, tok := range p {
		if tok == ">" {
			return len(s) > i
		}
		if i >= len(s) {
			return false
		}
		if tok != "*" && tok != s[i] {
			return false
		}
	}
	return len(p) == len(s)
}

func TestActivitySubjects(t *testing.T) {
	if got := ActivitySubject("fleet-a", "Galahad", "t1"); got != "fleet-a.activity.Galahad.t1" {
		t.Errorf("ActivitySubject = %q", got)
	}
	if got := ActivityPublishSubject("fleet-a", "Galahad"); got != "fleet-a.activity.Galahad.>" {
		t.Errorf("ActivityPublishSubject = %q", got)
	}
	if got := IntrospectPublishSubject("fleet-a", "Galahad"); got != "fleet-a.introspect.Galahad.>" {
		t.Errorf("IntrospectPublishSubject = %q", got)
	}
	if !natsSubjectMatches(ActivityPublishSubject("p", "K"), ActivitySubject("p", "K", "t")) {
		t.Error("publish permission must match the activity subject")
	}
}

// Activity events must never be captured by JetStream (core NATS only).
func TestActivityNotCapturedByStreams(t *testing.T) {
	const prefix = "fleet-a"
	streams := []string{
		StreamSubject(prefix, "tasks"),
		StreamSubject(prefix, "results"),
	}
	subj := ActivitySubject(prefix, "Galahad", "task-1")
	for _, s := range streams {
		if natsSubjectMatches(s, subj) {
			t.Errorf("stream subject %q captures activity subject %q", s, subj)
		}
	}
	// Sanity: the matcher does match what it should.
	if !natsSubjectMatches(streams[0], TaskSubject(prefix, "sec", "Galahad")) {
		t.Error("matcher broken: tasks stream should match task subject")
	}
}

func TestActivityEventJSON(t *testing.T) {
	slot, conc := 1, 2
	ev := ActivityEvent{V: ActivityVersion, Type: ActivityTaskStarted, Knight: "Galahad",
		Pod: "p", TaskID: "t", Seq: 1, Slot: &slot, Concurrency: &conc}
	b, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	for _, k := range []string{"v", "type", "ts", "knight", "pod", "taskId", "seq", "slot", "concurrency"} {
		if _, ok := m[k]; !ok {
			t.Errorf("missing key %q in %s", k, b)
		}
	}
	if _, ok := m["node"]; ok {
		t.Error("empty node should be omitted")
	}
	if len(b) > MaxActivityEventBytes {
		t.Error("event exceeds bound")
	}
}
