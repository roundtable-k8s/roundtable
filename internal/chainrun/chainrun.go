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

// Package chainrun starts and inspects "a run of Chain X with input Y". A run
// is a copy of a template Chain, so issue pickup, triggers and the gateway all
// start runs the same way.
package chainrun

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"maps"
	"regexp"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

// LabelRunSource records what started a run copy.
const LabelRunSource = "ai.roundtable.io/run-source"

// Run sources for LabelRunSource.
const (
	SourcePickup  = "pickup"
	SourceTrigger = "trigger"
	SourceGateway = "gateway"
)

const maxNameLen = 253

// StartOptions describes one run of a template Chain.
type StartOptions struct {
	// Name and Namespace of the new copy (ignored when Existing is set).
	Name      string
	Namespace string
	// Input becomes spec.input of the run.
	Input string
	// Trigger is the value written to the run annotation.
	Trigger string
	// Source is one of the Source* values, set as LabelRunSource.
	Source string
	// Labels and Annotations are added to a new copy, and to Existing on a
	// re-run.
	Labels      map[string]string
	Annotations map[string]string
	// Owner owns a new copy; nil leaves it unowned.
	Owner client.Object
	// Existing is a finished copy to re-run instead of creating one.
	Existing *aiv1alpha1.Chain
}

// Start creates a copy of tmpl with the run's input, or re-runs opts.Existing
// with tmpl's current spec.
func Start(ctx context.Context, c client.Client, scheme *runtime.Scheme, tmpl *aiv1alpha1.Chain, opts StartOptions) (*aiv1alpha1.Chain, error) {
	spec := tmpl.Spec.DeepCopy()
	spec.Input = opts.Input
	spec.Schedule = ""
	spec.StartingDeadlineSeconds = nil
	spec.Suspend = false

	if existing := opts.Existing; existing != nil {
		orig := existing.DeepCopy()
		existing.Spec = *spec
		if existing.Annotations == nil {
			existing.Annotations = map[string]string{}
		}
		maps.Copy(existing.Annotations, opts.Annotations)
		existing.Annotations[aiv1alpha1.AnnotationChainRun] = opts.Trigger
		existing.Annotations[aiv1alpha1.AnnotationRunTemplate] = tmpl.Name
		if opts.Source != "" || len(opts.Labels) > 0 {
			if existing.Labels == nil {
				existing.Labels = map[string]string{}
			}
			maps.Copy(existing.Labels, opts.Labels)
			if opts.Source != "" {
				existing.Labels[LabelRunSource] = opts.Source
			}
		}
		if err := c.Patch(ctx, existing, client.MergeFrom(orig)); err != nil {
			return nil, err
		}
		return existing, nil
	}

	labels := map[string]string{}
	maps.Copy(labels, opts.Labels)
	if opts.Source != "" {
		labels[LabelRunSource] = opts.Source
	}
	annotations := map[string]string{}
	maps.Copy(annotations, opts.Annotations)
	annotations[aiv1alpha1.AnnotationChainRun] = opts.Trigger
	annotations[aiv1alpha1.AnnotationRunTemplate] = tmpl.Name

	chain := &aiv1alpha1.Chain{
		ObjectMeta: metav1.ObjectMeta{
			Name:        opts.Name,
			Namespace:   opts.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: *spec,
	}
	if opts.Owner != nil {
		if err := controllerutil.SetControllerReference(opts.Owner, chain, scheme); err != nil {
			return nil, err
		}
	}
	if err := c.Create(ctx, chain); err != nil {
		return nil, err
	}
	return chain, nil
}

// Active reports whether a Chain is running or has a run pending.
func Active(c *aiv1alpha1.Chain) bool {
	return c.Status.Phase == aiv1alpha1.ChainPhaseRunning ||
		c.Annotations[aiv1alpha1.AnnotationChainRun] != c.Status.LastRunTrigger
}

// Expired reports whether a finished Chain is past ttl seconds after
// status.completedAt. A nil or non-positive ttl never expires.
func Expired(c *aiv1alpha1.Chain, ttl *int32, now time.Time) bool {
	if ttl == nil || *ttl <= 0 || c.Status.CompletedAt == nil {
		return false
	}
	return now.Sub(c.Status.CompletedAt.Time) >= time.Duration(*ttl)*time.Second
}

var nonDNSChars = regexp.MustCompile(`[^a-z0-9-]+`)

// KeyHash is the short hash of a Trigger event's dedupe key that names its
// run.
func KeyHash(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])[:12]
}

// TriggerRunName is the name of the run a Trigger starts from chain for an
// event with dedupe key key (github items of state selectors and issue
// pickup name theirs differently).
func TriggerRunName(chain, trigger, key string) string {
	return Name(chain, trigger, KeyHash(key))
}

// Name joins base and parts with "-" into a DNS-safe name of at most 253
// characters. Parts are lowercased and stripped of unsafe characters; base is
// used as is. When too long, the base and middle parts are cut and the last
// part is kept intact.
func Name(base string, parts ...string) string {
	clean := make([]string, len(parts))
	for i, p := range parts {
		clean[i] = strings.Trim(nonDNSChars.ReplaceAllString(strings.ToLower(p), "-"), "-")
	}
	suffix := ""
	if n := len(clean); n > 0 {
		suffix = "-" + clean[n-1]
		clean = clean[:n-1]
	}
	if len(suffix) > maxNameLen {
		suffix = strings.TrimRight(suffix[:maxNameLen], "-")
	}
	prefix := strings.Join(append([]string{base}, clean...), "-")
	if limit := max(maxNameLen-len(suffix), 0); len(prefix) > limit {
		prefix = strings.TrimRight(prefix[:limit], "-")
	}
	if prefix == "" {
		// Nothing of the base survived: drop the separator so the name does
		// not start with "-".
		return strings.TrimPrefix(suffix, "-")
	}
	return prefix + suffix
}
