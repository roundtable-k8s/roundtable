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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
)

// EnvPlannerDefaults is the operator env var holding the install-wide
// defaults for table planners (Helm planner.*), as JSON.
const EnvPlannerDefaults = "TABLE_PLANNER_DEFAULTS"

// plannerDomain is the domain of table-owned planner knights: they consume
// <prefix>.tasks.planner.>.
const plannerDomain = "planner"

// PlannerDefaults are the install-wide settings table-owned planner knights
// inherit where neither spec.planner nor spec.defaults sets a value.
type PlannerDefaults struct {
	Model              string                     `json:"model,omitempty"`
	Skills             []string                   `json:"skills,omitempty"`
	Arsenals           []aiv1alpha1.KnightArsenal `json:"arsenals,omitempty"`
	Prompt             *aiv1alpha1.KnightPrompt   `json:"prompt,omitempty"`
	Concurrency        int32                      `json:"concurrency,omitempty"`
	TaskTimeoutSeconds int32                      `json:"taskTimeoutSeconds,omitempty"`
}

// DefaultPlannerDefaults are used when the operator env sets none.
func DefaultPlannerDefaults() PlannerDefaults {
	return PlannerDefaults{
		Skills:             []string{"operator", "shared"},
		Concurrency:        1,
		TaskTimeoutSeconds: 600,
	}
}

// PlannerDefaultsFromEnv reads EnvPlannerDefaults over DefaultPlannerDefaults.
// Fields the JSON leaves empty keep the built-in value.
func PlannerDefaultsFromEnv() (PlannerDefaults, error) {
	d := DefaultPlannerDefaults()
	v := strings.TrimSpace(os.Getenv(EnvPlannerDefaults))
	if v == "" {
		return d, nil
	}
	var in PlannerDefaults
	if err := json.Unmarshal([]byte(v), &in); err != nil {
		return d, fmt.Errorf("parse %s: %w", EnvPlannerDefaults, err)
	}
	return d.merge(in), nil
}

// merge returns d with every field set in o replacing d's.
func (d PlannerDefaults) merge(o PlannerDefaults) PlannerDefaults {
	if o.Model != "" {
		d.Model = o.Model
	}
	if len(o.Skills) > 0 {
		d.Skills = o.Skills
	}
	if len(o.Arsenals) > 0 {
		d.Arsenals = o.Arsenals
	}
	if o.Prompt != nil {
		d.Prompt = o.Prompt
	}
	if o.Concurrency > 0 {
		d.Concurrency = o.Concurrency
	}
	if o.TaskTimeoutSeconds > 0 {
		d.TaskTimeoutSeconds = o.TaskTimeoutSeconds
	}
	return d
}

// plannerDefaults returns the reconciler's planner defaults, filling the
// built-in values for a zero struct (tests, older wiring).
func (r *RoundTableReconciler) plannerDefaults() PlannerDefaults {
	return DefaultPlannerDefaults().merge(r.PlannerDefaults)
}

// reconcilePlanner creates or updates the table-owned planner knight, or
// deletes it when the table has none (spec.planner.enabled false, a
// knightRef, or an ephemeral table). It returns the name of the knight that
// plans the table's missions.
func (r *RoundTableReconciler) reconcilePlanner(ctx context.Context, rt *aiv1alpha1.RoundTable) (string, error) {
	if !rt.OwnsPlanner() {
		if err := r.deleteOwnedPlanner(ctx, rt); err != nil {
			return "", err
		}
		return rt.PlannerKnightName(), nil
	}

	k := &aiv1alpha1.Knight{ObjectMeta: metav1.ObjectMeta{Name: rt.OwnedPlannerName(), Namespace: rt.Namespace}}
	// Never adopt a knight the table does not own (e.g. one created by hand
	// before this table had a planner).
	existing := &aiv1alpha1.Knight{}
	err := r.Get(ctx, types.NamespacedName{Name: k.Name, Namespace: k.Namespace}, existing)
	if err == nil && !metav1.IsControlledBy(existing, rt) {
		return "", fmt.Errorf("knight %s exists and is not owned by RoundTable %s; "+
			"delete it or set spec.planner.knightRef to %q", k.Name, rt.Name, k.Name)
	}
	if err != nil && !apierrors.IsNotFound(err) {
		return "", err
	}

	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, k, func() error {
		r.mutatePlanner(rt, k)
		return controllerutil.SetControllerReference(rt, k, r.Scheme)
	})
	if err != nil {
		return "", fmt.Errorf("planner knight %s: %w", k.Name, err)
	}
	if op != controllerutil.OperationResultNone {
		logf.FromContext(ctx).Info("Reconciled planner Knight", "name", k.Name, "operation", op)
	}
	return k.Name, nil
}

// plannerSpecHashAnnotation records the hash of the fields the table last
// set on its planner knight. Fields with CRD defaults (arsenals, egress) come
// back from the API server different from what was sent, so the knight is
// rewritten only when this hash changes, not on every reconcile.
const plannerSpecHashAnnotation = "ai.roundtable.io/planner-spec-hash"

// plannerSpec is what the table sets on its planner knight.
type plannerSpec struct {
	Skills             []string                     `json:"skills"`
	NATS               aiv1alpha1.KnightNATS        `json:"nats"`
	Model              string                       `json:"model,omitempty"`
	Arsenals           []aiv1alpha1.KnightArsenal   `json:"arsenals,omitempty"`
	Prompt             *aiv1alpha1.KnightPrompt     `json:"prompt,omitempty"`
	Resources          *corev1.ResourceRequirements `json:"resources,omitempty"`
	RuntimeClassName   *string                      `json:"runtimeClassName,omitempty"`
	Egress             *aiv1alpha1.KnightEgress     `json:"egress,omitempty"`
	Concurrency        int32                        `json:"concurrency"`
	TaskTimeoutSeconds int32                        `json:"taskTimeoutSeconds"`
	Description        string                       `json:"description"`
}

// desiredPlanner resolves the planner knight's fields: spec.planner, then
// spec.defaults (by leaving model and arsenals empty so the knight inherits
// the table's value as it changes), then the operator's planner defaults.
// Resources, runtimeClassName and egress not set in spec.planner are
// inherited from spec.defaults and the isolation defaults by the knight
// controller.
func (r *RoundTableReconciler) desiredPlanner(rt *aiv1alpha1.RoundTable) plannerSpec {
	d := r.plannerDefaults()
	p := rt.Spec.Planner
	if p == nil {
		p = &aiv1alpha1.RoundTablePlanner{}
	}
	td := rt.Spec.Defaults
	if td == nil {
		td = &aiv1alpha1.RoundTableDefaults{}
	}
	prefix := rt.Spec.NATS.SubjectPrefix
	out := plannerSpec{
		Skills: slices.Clone(d.Skills),
		NATS: aiv1alpha1.KnightNATS{
			URL:           rt.Spec.NATS.URL,
			Subjects:      []string{fmt.Sprintf("%s.tasks.%s.>", prefix, plannerDomain)},
			Stream:        rt.Spec.NATS.TasksStream,
			ResultsStream: rt.Spec.NATS.ResultsStream,
		},
		Concurrency:        d.Concurrency,
		TaskTimeoutSeconds: d.TaskTimeoutSeconds,
		Description:        fmt.Sprintf("Plans the meta-missions of RoundTable %s.", rt.Name),
	}
	switch {
	case p.Model != "":
		out.Model = p.Model
	case td.Model == "":
		out.Model = d.Model
	}
	switch {
	case len(p.Arsenals) > 0:
		out.Arsenals = deepCopyArsenals(p.Arsenals)
	case len(td.Arsenals) == 0:
		out.Arsenals = deepCopyArsenals(d.Arsenals)
	}
	switch {
	case p.Prompt != nil:
		out.Prompt = p.Prompt.DeepCopy()
	case d.Prompt != nil:
		out.Prompt = d.Prompt.DeepCopy()
	}
	if p.Resources != nil {
		out.Resources = p.Resources.DeepCopy()
	}
	if p.RuntimeClassName != nil {
		out.RuntimeClassName = new(string)
		*out.RuntimeClassName = *p.RuntimeClassName
	}
	if p.Egress != nil {
		out.Egress = p.Egress.DeepCopy()
	}
	return out
}

func (ps plannerSpec) hash() string {
	b, _ := json.Marshal(ps)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:8])
}

// mutatePlanner sets the labels of the table-owned planner knight and, when
// the desired fields changed since the last write, the fields the table
// controls; everything else (including CRD defaults) is left alone.
func (r *RoundTableReconciler) mutatePlanner(rt *aiv1alpha1.RoundTable, k *aiv1alpha1.Knight) {
	if k.Labels == nil {
		k.Labels = map[string]string{}
	}
	k.Labels[aiv1alpha1.LabelRoundTable] = rt.Name
	k.Labels[aiv1alpha1.LabelRole] = plannerDomain
	k.Labels["app.kubernetes.io/managed-by"] = "roundtable-operator"
	k.Labels["app.kubernetes.io/component"] = plannerDomain

	want := r.desiredPlanner(rt)
	hash := want.hash()
	if k.Annotations[plannerSpecHashAnnotation] == hash {
		return
	}
	if k.Annotations == nil {
		k.Annotations = map[string]string{}
	}
	k.Annotations[plannerSpecHashAnnotation] = hash

	s := &k.Spec
	s.Domain = plannerDomain
	s.Skills = want.Skills
	s.NATS.Subjects = want.NATS.Subjects
	s.NATS.Stream = want.NATS.Stream
	s.NATS.ResultsStream = want.NATS.ResultsStream
	if want.NATS.URL != "" {
		s.NATS.URL = want.NATS.URL
	}
	s.Model = want.Model
	s.Arsenals = want.Arsenals
	s.Prompt = want.Prompt
	s.Resources = want.Resources
	s.RuntimeClassName = want.RuntimeClassName
	s.Egress = want.Egress
	s.Concurrency = want.Concurrency
	s.TaskTimeoutSeconds = want.TaskTimeoutSeconds
	// Scale to zero when idle: an unused planner costs nothing.
	if s.Lifecycle == nil {
		s.Lifecycle = &aiv1alpha1.KnightLifecycle{}
	}
	s.Lifecycle.SuspendPolicy = knightpkg.SuspendPolicyAuto
	if s.Card == nil {
		s.Card = &aiv1alpha1.KnightCard{}
	}
	s.Card.Description = want.Description
	s.Card.Tags = []string{plannerDomain}
}

func deepCopyArsenals(in []aiv1alpha1.KnightArsenal) []aiv1alpha1.KnightArsenal {
	if len(in) == 0 {
		return nil
	}
	out := make([]aiv1alpha1.KnightArsenal, len(in))
	for i := range in {
		in[i].DeepCopyInto(&out[i])
	}
	return out
}

// deleteOwnedPlanner removes the table-owned planner knight, if any.
func (r *RoundTableReconciler) deleteOwnedPlanner(ctx context.Context, rt *aiv1alpha1.RoundTable) error {
	k := &aiv1alpha1.Knight{}
	err := r.Get(ctx, types.NamespacedName{Name: rt.OwnedPlannerName(), Namespace: rt.Namespace}, k)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !metav1.IsControlledBy(k, rt) || !k.DeletionTimestamp.IsZero() {
		return nil
	}
	if err := r.Delete(ctx, k); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete planner knight %s: %w", k.Name, err)
	}
	logf.FromContext(ctx).Info("Deleted planner Knight", "name", k.Name)
	return nil
}
