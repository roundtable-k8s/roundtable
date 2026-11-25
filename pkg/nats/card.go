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
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

// Agent cards (#393): the RoundTable controller keeps one card per knight in a
// NATS KV bucket, key "<table>.<knight>". The top level follows A2A's
// AgentCard so it can be served to outside A2A agents later; everything Round
// Table specific lives under "roundtable". See docs/agent-cards.md.

const (
	// CardSchemaVersion is the version of the AgentCard JSON layout.
	CardSchemaVersion = 1

	// DefaultCardsBucket is the KV bucket the cards live in when the operator's
	// CARDS_KV_BUCKET env var is unset.
	DefaultCardsBucket = "roundtable-cards"

	// CardsBucketEnv names the operator env var that overrides the bucket.
	CardsBucketEnv = "CARDS_KV_BUCKET"

	// KnightServiceName is the NATS micro service name every knight pod
	// registers under; its INFO metadata carries the loaded skills.
	KnightServiceName = "roundtable-knight"

	// CardSkillsFromSpec marks cards whose skills are the spec's categories.
	CardSkillsFromSpec = "spec"
	// CardSkillsFromRuntime marks cards whose skills came from a running knight.
	CardSkillsFromRuntime = "runtime"
)

// AgentCard is a knight's card, close to A2A's AgentCard.
type AgentCard struct {
	SchemaVersion      int          `json:"schemaVersion"`
	Name               string       `json:"name"`
	Description        string       `json:"description"`
	Version            string       `json:"version"`
	DefaultInputModes  []string     `json:"defaultInputModes"`
	DefaultOutputModes []string     `json:"defaultOutputModes"`
	Skills             []AgentSkill `json:"skills"`
	RoundTable         CardMeta     `json:"roundtable"`
}

// AgentSkill is one skill on a card (A2A AgentSkill).
type AgentSkill struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Tags        []string `json:"tags"`
	Examples    []string `json:"examples"`
}

// CardMeta holds the Round Table specific card fields.
type CardMeta struct {
	Table         string   `json:"table"`
	Knight        string   `json:"knight"`
	Domain        string   `json:"domain"`
	Model         string   `json:"model"`
	Tools         []string `json:"tools"`
	Tags          []string `json:"tags,omitempty"`
	TaskSubject   string   `json:"taskSubject"`
	Runtime       string   `json:"runtime"`
	Phase         string   `json:"phase"`
	ReadyReplicas int32    `json:"readyReplicas"`
	Suspended     bool     `json:"suspended"`

	// SkillsSource is CardSkillsFromSpec or CardSkillsFromRuntime.
	SkillsSource string `json:"skillsSource"`
	// RuntimeSkills are the skills the knight last reported over its micro
	// service, kept so the card still has them while the knight is at zero.
	RuntimeSkills []RuntimeSkill `json:"runtimeSkills,omitempty"`

	// UpdatedAt is when the card content last changed (RFC 3339).
	UpdatedAt string `json:"updatedAt"`
}

// RuntimeSkill is a skill a running knight reports in its micro service
// metadata ("skills": JSON array of these).
type RuntimeSkill struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Category    string `json:"category,omitempty"`
}

// CardKey is the KV key of a knight's card.
func CardKey(table, knight string) string {
	return table + "." + knight
}

// CardsBucketFromEnv returns CARDS_KV_BUCKET, or DefaultCardsBucket.
func CardsBucketFromEnv() string {
	if b := os.Getenv(CardsBucketEnv); b != "" {
		return b
	}
	return DefaultCardsBucket
}

// SameCard reports whether two cards have the same content, ignoring UpdatedAt.
func SameCard(a, b *AgentCard) bool {
	if a == nil || b == nil {
		return a == b
	}
	ac, bc := *a, *b
	ac.RoundTable.UpdatedAt, bc.RoundTable.UpdatedAt = "", ""
	aj, errA := json.Marshal(ac)
	bj, errB := json.Marshal(bc)
	return errA == nil && errB == nil && string(aj) == string(bj)
}

// CardStore reads and writes cards in the cards KV bucket.
type CardStore struct {
	kv nats.KeyValue
}

// NewCardStore opens the cards bucket, creating it (history 1, no TTL) if missing.
func NewCardStore(nc *nats.Conn, bucket string) (*CardStore, error) {
	if nc == nil {
		return nil, errors.New("NATS not connected")
	}
	js, err := nc.JetStream()
	if err != nil {
		return nil, fmt.Errorf("JetStream context failed: %w", err)
	}
	kv, err := js.KeyValue(bucket)
	if errors.Is(err, nats.ErrBucketNotFound) {
		kv, err = js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket:      bucket,
			Description: "Round Table agent cards, key <table>.<knight>",
			History:     1,
		})
	}
	if err != nil {
		return nil, fmt.Errorf("cards bucket %s: %w", bucket, err)
	}
	return &CardStore{kv: kv}, nil
}

// Get returns the card at key, or nil when there is none (or it is unreadable).
func (s *CardStore) Get(key string) (*AgentCard, error) {
	entry, err := s.kv.Get(key)
	if errors.Is(err, nats.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	card := &AgentCard{}
	if err := json.Unmarshal(entry.Value(), card); err != nil {
		return nil, nil //nolint:nilerr // an unreadable card is rewritten
	}
	return card, nil
}

// Put writes card at key.
func (s *CardStore) Put(key string, card *AgentCard) error {
	data, err := json.Marshal(card)
	if err != nil {
		return err
	}
	_, err = s.kv.Put(key, data)
	return err
}

// Delete removes the card at key; a missing key is not an error.
func (s *CardStore) Delete(key string) error {
	if _, err := s.kv.Get(key); errors.Is(err, nats.ErrKeyNotFound) {
		return nil
	}
	if err := s.kv.Delete(key); err != nil && !errors.Is(err, nats.ErrKeyNotFound) {
		return err
	}
	return nil
}

// TableKeys lists the card keys of a table ("<table>.*"), sorted.
func (s *CardStore) TableKeys(table string) ([]string, error) {
	keys, err := s.kv.Keys()
	if errors.Is(err, nats.ErrNoKeysFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	prefix := table + "."
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		if strings.HasPrefix(k, prefix) && !strings.Contains(k[len(prefix):], ".") {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out, nil
}

// ServiceInfo is the part of a NATS micro service INFO response the
// operator reads.
type ServiceInfo struct {
	Name     string            `json:"name"`
	ID       string            `json:"id"`
	Version  string            `json:"version"`
	Metadata map[string]string `json:"metadata"`
}

// Skills parses the "skills" metadata entry (a JSON array of RuntimeSkill).
func (i ServiceInfo) Skills() ([]RuntimeSkill, error) {
	raw := i.Metadata["skills"]
	if raw == "" {
		return nil, nil
	}
	var skills []RuntimeSkill
	if err := json.Unmarshal([]byte(raw), &skills); err != nil {
		return nil, fmt.Errorf("service %s: skills metadata: %w", i.ID, err)
	}
	return skills, nil
}

// RequestServiceInfo sends $SRV.INFO.<service> and collects every response
// that arrives within timeout. No responders gives an empty slice.
func RequestServiceInfo(nc *nats.Conn, service string, timeout time.Duration) ([]ServiceInfo, error) {
	if nc == nil {
		return nil, errors.New("NATS not connected")
	}
	inbox := nc.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		return nil, err
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.PublishRequest("$SRV.INFO."+service, inbox, nil); err != nil {
		return nil, err
	}

	var infos []ServiceInfo
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return infos, nil
		}
		msg, err := sub.NextMsg(remaining)
		if errors.Is(err, nats.ErrTimeout) || errors.Is(err, nats.ErrNoResponders) {
			return infos, nil
		}
		if err != nil {
			return infos, err
		}
		var info ServiceInfo
		if len(msg.Data) == 0 || json.Unmarshal(msg.Data, &info) != nil {
			continue
		}
		infos = append(infos, info)
	}
}

// KnightSkills returns the union (by name, sorted) of the skills reported by
// the knight's running pods, matched on the "knight" metadata (case-insensitive:
// runtimes report the capitalised KNIGHT_NAME) and the "table" metadata (an
// empty table is accepted: ROUNDTABLE_TABLE is unset for knights that join a
// table through its selector). ok is false when no pod of the knight answered
// with a "skills" entry.
func KnightSkills(infos []ServiceInfo, table, knight string) (skills []RuntimeSkill, ok bool) {
	byName := map[string]RuntimeSkill{}
	for _, info := range infos {
		if !strings.EqualFold(info.Metadata["knight"], knight) {
			continue
		}
		if t := info.Metadata["table"]; t != "" && t != table {
			continue
		}
		if _, has := info.Metadata["skills"]; !has {
			continue // a runtime that does not report skills yet
		}
		reported, err := info.Skills()
		if err != nil {
			continue
		}
		ok = true
		for _, s := range reported {
			if s.Name == "" {
				continue
			}
			if _, seen := byName[s.Name]; !seen {
				byName[s.Name] = s
			}
		}
	}
	if !ok {
		return nil, false
	}
	skills = make([]RuntimeSkill, 0, len(byName))
	for _, s := range byName {
		skills = append(skills, s)
	}
	sort.Slice(skills, func(i, j int) bool { return skills[i].Name < skills[j].Name })
	return skills, true
}
