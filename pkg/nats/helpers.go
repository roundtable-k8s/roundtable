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

	"github.com/roundtable-k8s/roundtable/internal/util"
)

// TaskSubject constructs a NATS subject for publishing tasks to a knight.
// Format: {prefix}.tasks.{domain}.{knight}
func TaskSubject(prefix, domain, knight string) string {
	return fmt.Sprintf("%s.tasks.%s.%s", prefix, domain, knight)
}

// ResultSubject constructs a NATS subject for task results.
// Format: {prefix}.results.{taskID}
func ResultSubject(prefix, taskID string) string {
	return fmt.Sprintf("%s.results.%s", prefix, taskID)
}

// ResultSubjectWildcard constructs a NATS subject pattern for polling task results.
// Format: {prefix}.results.{taskPrefix}.*
func ResultSubjectWildcard(prefix, taskPrefix string) string {
	return fmt.Sprintf("%s.results.%s.*", prefix, taskPrefix)
}

// StreamSubject constructs a NATS subject pattern for stream capture.
// Format: {prefix}.{streamType}.>
func StreamSubject(prefix, streamType string) string {
	return fmt.Sprintf("%s.%s.>", prefix, streamType)
}

// ChainConsumerName generates a consumer name for chain result polling.
// Format: chain-poll-{chainName}-{stepName}-{timestamp}
func ChainConsumerName(chainName, stepName string) string {
	return fmt.Sprintf("chain-poll-%s-%s", chainName, stepName)
}

// KnightConsumerName generates a consumer name for a knight.
// Format: knight-{knightName}
func KnightConsumerName(knightName string) string {
	return fmt.Sprintf("knight-%s", knightName)
}

// KnightTaskConsumerName is the durable JetStream consumer a knight runtime
// creates on the tasks stream. Format: {Knight}-consumer. The runtimes build it
// from KNIGHT_NAME, which the operator sets to the capitalised Knight name, so
// the name is capitalised here too. This is the single source of truth for the
// consumer name (internal/knight.TaskConsumerName delegates to it).
//
// Knights ack a task only after its result is published, so the consumer's ack
// floor is a completion signal; its delivered sequence is the pickup signal.
func KnightTaskConsumerName(knightName string) string {
	return util.Capitalize(knightName) + "-consumer"
}

// EventsStreamSubject is the subject a table's events stream captures.
// Format: {prefix}.events.>
func EventsStreamSubject(prefix string) string {
	return StreamSubject(prefix, "events")
}

// EventsSubjectPrefix is the prefix every event subject of a table starts
// with. Format: {prefix}.events.
func EventsSubjectPrefix(prefix string) string {
	return prefix + ".events."
}

// EventSubject constructs the subject of a named event.
// Format: {prefix}.events.{name}
func EventSubject(prefix, name string) string {
	return EventsSubjectPrefix(prefix) + name
}

// TriggerConsumerName is the durable JetStream consumer a NATS-source Trigger
// reads its events through. Format: trigger-{namespace}_{name} (Kubernetes
// names have no "_", so the name is unambiguous).
func TriggerConsumerName(namespace, name string) string {
	return "trigger-" + namespace + "_" + name
}

// WebhookEventSubject is the subject the gateway publishes a webhook
// Trigger's deliveries to and the Trigger reads them from.
// Format: {prefix}.events.webhook.{namespace}.{name}. The namespace keeps
// same-named Triggers in different namespaces apart on a shared prefix.
func WebhookEventSubject(prefix, namespace, name string) string {
	return EventSubject(prefix, "webhook."+namespace+"."+name)
}

// GitHubEventSubject is the subject the operator publishes a github-source
// Trigger's polled items to and the Trigger reads them from.
// Format: {prefix}.events.github.{namespace}.{name}.
func GitHubEventSubject(prefix, namespace, name string) string {
	return EventSubject(prefix, "github."+namespace+"."+name)
}

// EventKeyHeader carries an event's default dedupe key when it differs from
// its Nats-Msg-Id (a github-source item: "<owner/name>#<number>").
const EventKeyHeader = "Roundtable-Event-Key"
