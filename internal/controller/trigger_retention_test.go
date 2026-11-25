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
	"testing"
	"time"

	"k8s.io/utils/ptr"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func TestScheduleRunTTL(t *testing.T) {
	sched := &aiv1alpha1.Trigger{Spec: aiv1alpha1.TriggerSpec{
		Source: aiv1alpha1.TriggerSource{Schedule: &aiv1alpha1.TriggerScheduleSource{Cron: "* * * * *"}}}}
	nats := &aiv1alpha1.Trigger{Spec: aiv1alpha1.TriggerSpec{
		Source: aiv1alpha1.TriggerSource{NATS: &aiv1alpha1.TriggerNATSSource{}}}}
	month := 30 * 24 * time.Hour
	for name, tc := range map[string]struct {
		trg       *aiv1alpha1.Trigger
		ttl       *int32
		retention time.Duration
		want      *int32
	}{
		"short ttl is raised":     {sched, ptr.To[int32](86400), month, ptr.To[int32](2592000)},
		"longer ttl is kept":      {sched, ptr.To[int32](7776000), month, ptr.To[int32](7776000)},
		"0 keeps forever":         {sched, ptr.To[int32](0), month, ptr.To[int32](0)},
		"unset stays unset":       {sched, nil, month, nil},
		"no retention":            {sched, ptr.To[int32](60), 0, ptr.To[int32](60)},
		"other sources untouched": {nats, ptr.To[int32](60), month, ptr.To[int32](60)},
	} {
		got := scheduleRunTTL(tc.trg, tc.ttl, tc.retention)
		if (got == nil) != (tc.want == nil) || (got != nil && *got != *tc.want) {
			t.Errorf("%s: got %v, want %v", name, got, tc.want)
		}
	}
}
