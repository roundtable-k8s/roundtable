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

import (
	"strings"
	"testing"
)

func TestRedactSecrets(t *testing.T) {
	pat := "ghp_" + strings.Repeat("a1B2", 9)
	cases := map[string]string{
		"token " + pat + " end":              "token [REDACTED] end",
		"AKIAABCDEFGHIJKLMNOP":               "[REDACTED]",
		"api_key = abcdefghijklmnopqrstuvwx": "[REDACTED]",
		"nothing secret here":                "nothing secret here",
	}
	for in, want := range cases {
		if got := redactSecrets(in); got != want {
			t.Errorf("redactSecrets(%q) = %q, want %q", in, got, want)
		}
	}
}
