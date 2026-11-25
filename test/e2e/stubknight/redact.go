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

package main

import "regexp"

// redactedMarker replaces every secret, as the knights' redactSecrets does.
const redactedMarker = "[REDACTED]"

// secretPatterns is the knights' shared secret pattern list (pi-knight
// src/hooks.ts SECRET_PATTERNS), the same list as the chart collector's
// telemetry.collector.redaction.blockedValues default.
var secretPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)(?:api[_-]?key|apikey|secret[_-]?key|access[_-]?token|auth[_-]?token|bearer)` +
		`\s*[:=]\s*['"]?[A-Za-z0-9_\-/.+=]{20,}`),
	regexp.MustCompile(`(?i)-----BEGIN (?:RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----`),
	regexp.MustCompile(`ghp_[A-Za-z0-9_]{36,}`),
	regexp.MustCompile(`ghs_[A-Za-z0-9_]{36,}`),
	regexp.MustCompile(`sk-[A-Za-z0-9]{40,}`),
	regexp.MustCompile(`xoxb-[A-Za-z0-9\-]+`),
	regexp.MustCompile(`AKIA[A-Z0-9]{16}`),
}

// redactSecrets replaces every known secret pattern in text with
// redactedMarker, in-process, before the text goes on a span.
func redactSecrets(text string) string {
	for _, p := range secretPatterns {
		text = p.ReplaceAllString(text, redactedMarker)
	}
	return text
}
