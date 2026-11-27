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
	"testing"
	"time"
)

func TestParseSleep(t *testing.T) {
	cases := []struct {
		task string
		want time.Duration
		ok   bool
	}{
		{"STUB:sleep=40", 40 * time.Second, true},
		{"STUB:sleep=40\nGather facts.", 40 * time.Second, true},
		{"  \nSTUB:sleep=5 then answer", 5 * time.Second, true},
		{"STUB:sleep=0", 0, true},
		{"STUB:sleep=", 0, false},
		{"STUB:sleep=abc", 0, false},
		{"STUB:sleep=-3", 0, false},
		{"Gather facts. STUB:sleep=40", 0, false},
		{"", 0, false},
	}
	for _, c := range cases {
		got, ok := parseSleep(c.task)
		if got != c.want || ok != c.ok {
			t.Errorf("parseSleep(%q) = %v, %t; want %v, %t", c.task, got, ok, c.want, c.ok)
		}
	}
}
