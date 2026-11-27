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

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
	"github.com/roundtable-k8s/roundtable/pkg/nats/contract/contracttest"
)

// fakeNATSClient is an in-memory natspkg.Client that records publishes and
// fails subjects matched by failSubject.
type fakeNATSClient struct {
	mu          sync.Mutex
	published   map[string][]byte
	failSubject func(subject string) bool
}

func newFakeNATSClient() *fakeNATSClient {
	return &fakeNATSClient{published: map[string][]byte{}}
}

func (f *fakeNATSClient) Connect() error    { return nil }
func (f *fakeNATSClient) Close() error      { return nil }
func (f *fakeNATSClient) IsConnected() bool { return true }

func (f *fakeNATSClient) Publish(subject string, data []byte) error {
	if f.failSubject != nil && f.failSubject(subject) {
		return fmt.Errorf("NATS publish to %s failed: nats: no response from stream", subject)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.published[subject] = data
	return nil
}

// PublishJSON records v. A task payload (chain step, fan-out item, loop body,
// mission task) must validate against the knight task contract schema; one
// that does not fails the publish, so the test that built it fails.
func (f *fakeNATSClient) PublishJSON(subject string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if _, ok := v.(natspkg.TaskPayload); ok {
		if err := contracttest.ValidateTask(data); err != nil {
			return fmt.Errorf("task payload breaks the knight task contract: %w", err)
		}
	}
	return f.Publish(subject, data)
}

func (f *fakeNATSClient) CreateStream(context.Context, natspkg.StreamConfig) error { return nil }
func (f *fakeNATSClient) EnsureStream(context.Context, natspkg.StreamConfig) (bool, error) {
	return false, nil
}
func (f *fakeNATSClient) DeleteStream(context.Context, string) error { return nil }
func (f *fakeNATSClient) StreamInfo(context.Context, string) (*jetstream.StreamInfo, error) {
	return nil, fmt.Errorf("not implemented")
}
func (f *fakeNATSClient) EnsureConsumer(context.Context, string, jetstream.ConsumerConfig) (natspkg.ConsumerAction, error) {
	return natspkg.ConsumerUnchanged, nil
}
func (f *fakeNATSClient) RemoveConsumer(context.Context, string, string) error { return nil }
func (f *fakeNATSClient) DeleteConsumer(context.Context, string, string) error { return nil }
func (f *fakeNATSClient) FetchOne(context.Context, string, jetstream.ConsumerConfig, time.Duration) (jetstream.Msg, error) {
	return nil, fmt.Errorf("not implemented")
}
func (f *fakeNATSClient) KVPut(context.Context, string, string, []byte) error { return nil }
func (f *fakeNATSClient) KVGet(context.Context, string, string) ([]byte, error) {
	return nil, fmt.Errorf("not found")
}
func (f *fakeNATSClient) KVDelete(context.Context, string, string) error   { return nil }
func (f *fakeNATSClient) KVKeys(context.Context, string) ([]string, error) { return nil, nil }

// fakeMsg is a jetstream.Msg carrying only a subject and data, as returned
// by the fakes' FetchOne (already acked). Other methods are not used.
type fakeMsg struct {
	jetstream.Msg
	subject string
	data    []byte
}

func (m *fakeMsg) Subject() string                           { return m.subject }
func (m *fakeMsg) Data() []byte                              { return m.data }
func (m *fakeMsg) Headers() nats.Header                      { return nil }
func (m *fakeMsg) Metadata() (*jetstream.MsgMetadata, error) { return nil, jetstream.ErrNotJSMessage }

func (f *fakeNATSClient) PublishJSONCtx(_ context.Context, subject string, v any, _ ...natspkg.PublishOption) error {
	return f.PublishJSON(subject, v)
}

func (f *fakeNATSClient) PublishJSONSeqCtx(_ context.Context, subject string, v any, _ ...natspkg.PublishOption) (uint64, error) {
	return 0, f.PublishJSON(subject, v)
}
