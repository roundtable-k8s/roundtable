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

package gateway

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"text/template"
	"time"

	"github.com/robfig/cron/v3"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

const (
	// Flux and Helm ownership markers.
	fluxKustomizationNS = "kustomize.toolkit.fluxcd.io/namespace"
	fluxHelmReleaseNS   = "helm.toolkit.fluxcd.io/namespace"
	helmReleaseName     = "meta.helm.sh/release-name"
	helmReleaseNS       = "meta.helm.sh/release-namespace"

	// maxTriggerNameLength is the CRD's limit: the name labels the runs.
	maxTriggerNameLength = 63
	// maxTimeZoneLength mirrors source.schedule.timeZone's MaxLength.
	maxTimeZoneLength = 64
)

// timeZonePattern mirrors source.schedule.timeZone's CRD pattern.
var timeZonePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_+\-]*(/[A-Za-z0-9_+\-]+)*$`)

// gitOwner names the Flux Kustomization or HelmRelease (or Helm release)
// that applied obj, or "" when the labels do not say.
func gitOwner(obj metav1.Object) string {
	labels, annotations := obj.GetLabels(), obj.GetAnnotations()
	get := func(key string) string {
		if v := labels[key]; v != "" {
			return v
		}
		return annotations[key]
	}
	qualify := func(ns, name string) string {
		if ns == "" {
			return name
		}
		return ns + "/" + name
	}
	switch {
	case get(fluxKustomizationName) != "":
		return "Flux Kustomization " + qualify(get(fluxKustomizationNS), get(fluxKustomizationName))
	case get(fluxHelmReleaseName) != "":
		return "Flux HelmRelease " + qualify(get(fluxHelmReleaseNS), get(fluxHelmReleaseName))
	case get(helmReleaseName) != "":
		return "Helm release " + qualify(get(helmReleaseNS), get(helmReleaseName))
	}
	return ""
}

// FieldError is a 400 that names the offending field of the request.
type FieldError struct {
	Field   string
	Message string
}

func (e *FieldError) Error() string {
	return fmt.Sprintf("%v: %s: %s", ErrInvalid, e.Field, e.Message)
}

// Is makes errors.Is(err, ErrInvalid) true.
func (e *FieldError) Is(target error) bool { return target == ErrInvalid }

func fieldErr(field, format string, args ...any) error {
	return &FieldError{Field: field, Message: fmt.Sprintf(format, args...)}
}

// GitManagedError is the 409 for a write to a Git-managed schedule.
type GitManagedError struct {
	Namespace, Name, Owner string
}

func (e *GitManagedError) Error() string {
	where := "in Git"
	if e.Owner != "" {
		where = "in Git (applied by " + e.Owner + ")"
	}
	return fmt.Sprintf("conflict: schedule %s/%s is managed by Git: change it %s, not through the gateway",
		e.Namespace, e.Name, where)
}

// Is makes errors.Is(err, ErrConflict) true.
func (e *GitManagedError) Is(target error) bool { return target == ErrConflict }

// ScheduleRequest is the body of POST /v1/schedules and PUT
// /v1/schedules/{namespace}/{name}: a Trigger's name and spec, whose source
// must be a schedule. Labels, annotations and status are not accepted.
type ScheduleRequest struct {
	// Name of the Trigger; required on POST, optional on PUT (must match
	// the path).
	Name string `json:"name,omitempty"`
	// Namespace of the Trigger; defaults to the gateway's namespace on POST,
	// must match the path on PUT.
	Namespace string `json:"namespace,omitempty"`
	// Spec is the Trigger's spec. spec.source.schedule is required and no
	// other source may be set.
	Spec aiv1alpha1.TriggerSpec `json:"spec"`
}

// ScheduleRecord is what the write routes return.
type ScheduleRecord struct {
	Name      string                 `json:"name"`
	Namespace string                 `json:"namespace"`
	ManagedBy ManagedBy              `json:"managedBy"`
	Caller    string                 `json:"caller,omitempty"`
	CreatedAt time.Time              `json:"createdAt"`
	Spec      aiv1alpha1.TriggerSpec `json:"spec"`
}

// CreateSchedule validates req and creates a schedule Trigger labelled as
// the gateway's own and annotated with the caller.
func (s *Service) CreateSchedule(ctx context.Context, req ScheduleRequest) (*ScheduleRecord, error) {
	if !s.SchedulesEnabled() {
		return nil, errSchedulesDisabled
	}
	ns, err := s.namespace(req.Namespace)
	if err != nil {
		return nil, fieldErr("namespace", "%q is not allowed on this gateway", req.Namespace)
	}
	if err := validateTriggerName(req.Name); err != nil {
		return nil, err
	}
	if err := s.validateScheduleSpec(ctx, ns, &req.Spec); err != nil {
		return nil, err
	}
	trg := &aiv1alpha1.Trigger{
		ObjectMeta: metav1.ObjectMeta{
			Name:      req.Name,
			Namespace: ns,
			Labels:    map[string]string{labelManagedBy: managedByGateway},
		},
		Spec: req.Spec,
	}
	setCaller(ctx, trg)
	if err := s.Schedules.Create(ctx, trg); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil, &ConflictError{State: "exists", Reason: fmt.Sprintf("trigger %s/%s already exists", ns, req.Name)}
		}
		return nil, scheduleAPIError("create trigger", err)
	}
	s.Log.Info("Created schedule", "trigger", trg.Name, "namespace", ns, "caller", callerName(ctx))
	return scheduleRecord(trg), nil
}

// ReplaceSchedule replaces the spec of an existing schedule Trigger. A
// Git-managed one is refused with 409.
func (s *Service) ReplaceSchedule(ctx context.Context, namespace, name string, req ScheduleRequest) (*ScheduleRecord, error) {
	if !s.SchedulesEnabled() {
		return nil, errSchedulesDisabled
	}
	ns, err := s.namespace(namespace)
	if err != nil {
		return nil, err
	}
	if req.Name != "" && req.Name != name {
		return nil, fieldErr("name", "%q does not match the path (%q)", req.Name, name)
	}
	if req.Namespace != "" && req.Namespace != ns {
		return nil, fieldErr("namespace", "%q does not match the path (%q)", req.Namespace, ns)
	}
	trg, err := s.fetchSchedule(ctx, ns, name)
	if err != nil {
		return nil, err
	}
	if err := refuseGitManaged(trg); err != nil {
		return nil, err
	}
	if err := s.validateScheduleSpec(ctx, ns, &req.Spec); err != nil {
		return nil, err
	}
	trg.Spec = req.Spec
	setCaller(ctx, trg)
	// Update carries the read resourceVersion: a concurrent change is a 409,
	// never silently overwritten. Status is a subresource and is kept.
	if err := s.Schedules.Update(ctx, trg); err != nil {
		switch {
		case apierrors.IsConflict(err):
			return nil, &ConflictError{State: "modified", Reason: fmt.Sprintf("trigger %s/%s changed meanwhile; read it and retry", ns, name)}
		case apierrors.IsNotFound(err):
			return nil, fmt.Errorf("%w: schedule %s/%s", ErrNotFound, ns, name)
		}
		return nil, scheduleAPIError("update trigger", err)
	}
	s.Log.Info("Replaced schedule", "trigger", name, "namespace", ns, "caller", callerName(ctx))
	return scheduleRecord(trg), nil
}

// DeleteSchedule deletes a schedule Trigger. A Git-managed one is refused
// with 409.
func (s *Service) DeleteSchedule(ctx context.Context, namespace, name string) error {
	if !s.SchedulesEnabled() {
		return errSchedulesDisabled
	}
	ns, err := s.namespace(namespace)
	if err != nil {
		return err
	}
	trg, err := s.fetchSchedule(ctx, ns, name)
	if err != nil {
		return err
	}
	if err := refuseGitManaged(trg); err != nil {
		return err
	}
	// Precondition on the UID so a same-named replacement isn't deleted.
	if err := s.Schedules.Delete(ctx, trg, client.Preconditions{UID: &trg.UID},
		client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("%w: schedule %s/%s", ErrNotFound, ns, name)
		}
		return k8sError("delete trigger", err)
	}
	s.Log.Info("Deleted schedule", "trigger", name, "namespace", ns, "caller", callerName(ctx))
	return nil
}

func refuseGitManaged(trg *aiv1alpha1.Trigger) error {
	if ScheduleManagedBy(trg) == ManagedByGit {
		return &GitManagedError{Namespace: trg.Namespace, Name: trg.Name, Owner: gitOwner(trg)}
	}
	return nil
}

func setCaller(ctx context.Context, trg *aiv1alpha1.Trigger) {
	caller := callerName(ctx)
	if caller == "" {
		return
	}
	if trg.Annotations == nil {
		trg.Annotations = map[string]string{}
	}
	trg.Annotations[aiv1alpha1.AnnotationCaller] = caller
}

func scheduleRecord(trg *aiv1alpha1.Trigger) *ScheduleRecord {
	return &ScheduleRecord{
		Name:      trg.Name,
		Namespace: trg.Namespace,
		ManagedBy: ScheduleManagedBy(trg),
		Caller:    trg.Annotations[aiv1alpha1.AnnotationCaller],
		CreatedAt: trg.CreationTimestamp.UTC(),
		Spec:      trg.Spec,
	}
}

func validateTriggerName(name string) error {
	switch {
	case name == "":
		return fieldErr("name", "is required")
	case len(name) > maxTriggerNameLength:
		return fieldErr("name", "must be at most %d characters", maxTriggerNameLength)
	}
	if errs := validation.IsDNS1123Subdomain(name); len(errs) > 0 {
		return fieldErr("name", "%s", strings.Join(errs, "; "))
	}
	return nil
}

// validateScheduleSpec applies the CRD's rules for a schedule Trigger (and
// the trigger controller's cron and time zone parsing), and checks the
// referenced Chain and RoundTable exist. Errors name the field.
func (s *Service) validateScheduleSpec(ctx context.Context, ns string, spec *aiv1alpha1.TriggerSpec) error {
	src := spec.Source
	var others []string
	if src.NATS != nil {
		others = append(others, "nats")
	}
	if src.Webhook != nil {
		others = append(others, "webhook")
	}
	if src.GitHub != nil {
		others = append(others, "github")
	}
	if len(others) > 0 {
		return fieldErr("spec.source", "only schedule sources are served here; remove source.%s", strings.Join(others, ", source."))
	}
	sch := src.Schedule
	if sch == nil {
		return fieldErr("spec.source.schedule", "is required")
	}
	if err := validateTimeZone(sch.TimeZone); err != nil {
		return err
	}
	if err := validateCron(sch.Cron, sch.TimeZone); err != nil {
		return err
	}
	if d := sch.StartingDeadlineSeconds; d != nil && *d < 1 {
		return fieldErr("spec.source.schedule.startingDeadlineSeconds", "must be at least 1")
	}

	if err := parseTemplate("spec.input", spec.Input); err != nil {
		return err
	}
	if err := parseTemplate("spec.dedupeKey", spec.DedupeKey); err != nil {
		return err
	}
	if err := s.validateScheduleTarget(ctx, ns, spec); err != nil {
		return err
	}
	if ref := spec.RoundTableRef; ref != nil {
		if ref.Name == "" {
			return fieldErr("spec.roundTableRef.name", "is required when roundTableRef is set")
		}
		if err := s.refExists(ctx, ns, ref.Name, "spec.roundTableRef.name", "RoundTable", &aiv1alpha1.RoundTable{}); err != nil {
			return err
		}
	}

	switch spec.Concurrency.Policy {
	case "", aiv1alpha1.TriggerConcurrencyAllow, aiv1alpha1.TriggerConcurrencyForbid, aiv1alpha1.TriggerConcurrencyReplace:
	default:
		return fieldErr("spec.concurrency.policy", "must be Allow, Forbid or Replace, not %q", spec.Concurrency.Policy)
	}
	if spec.Target.Knight != nil && spec.Concurrency.Policy == aiv1alpha1.TriggerConcurrencyReplace {
		return fieldErr("spec.concurrency.policy", "Replace is not supported with target.knight")
	}
	if spec.Concurrency.MaxActive < 0 {
		return fieldErr("spec.concurrency.maxActive", "must be at least 0")
	}
	if t := spec.TTLSecondsAfterFinished; t != nil && *t < 0 {
		return fieldErr("spec.ttlSecondsAfterFinished", "must be at least 0")
	}
	return nil
}

// validateScheduleTarget requires exactly one of target.chainRef and
// target.knight, and applies the trigger controller's rules to a knight
// target: the Knight exists and sits at roundTableRef, the task parses as a
// template, and spec.input is unset.
func (s *Service) validateScheduleTarget(ctx context.Context, ns string, spec *aiv1alpha1.TriggerSpec) error {
	tgt := spec.Target
	switch {
	case tgt.ChainRef != nil && tgt.Knight != nil:
		return fieldErr("spec.target", "set exactly one of chainRef or knight, not both")
	case tgt.Knight != nil:
		return s.validateKnightTarget(ctx, ns, spec)
	case tgt.ChainRef != nil && tgt.ChainRef.Name != "":
		return s.refExists(ctx, ns, tgt.ChainRef.Name, "spec.target.chainRef.name", "chain", &aiv1alpha1.Chain{})
	case tgt.ChainRef != nil:
		return fieldErr("spec.target.chainRef.name", "is required")
	default:
		return fieldErr("spec.target", "set exactly one of chainRef or knight")
	}
}

// triggerTemplateFuncs stand in for the trigger controller's template
// functions: templates are only parsed here, never executed.
var triggerTemplateFuncs = template.FuncMap{
	"toJson": func(any) (string, error) { return "", nil },
	"pluck":  func(string, any) []any { return nil },
}

func parseTemplate(field, text string) error {
	if _, err := template.New(field).Option("missingkey=error").Funcs(triggerTemplateFuncs).Parse(text); err != nil {
		return fieldErr(field, "%v", err)
	}
	return nil
}

func (s *Service) validateKnightTarget(ctx context.Context, ns string, spec *aiv1alpha1.TriggerSpec) error {
	kt := spec.Target.Knight
	if kt.Name == "" {
		return fieldErr("spec.target.knight.name", "is required")
	}
	if kt.Task == "" {
		return fieldErr("spec.target.knight.task", "is required")
	}
	if err := parseTemplate("spec.target.knight.task", kt.Task); err != nil {
		return err
	}
	if spec.Input != "" {
		return fieldErr("spec.input", "is not supported with target.knight")
	}
	table, err := s.Resolver.KnightTable(ctx, ns, kt.Name)
	switch {
	case errors.Is(err, ErrNotFound):
		return fieldErr("spec.target.knight.name", "knight %s/%s not found", ns, kt.Name)
	case errors.Is(err, ErrUnavailable):
		return fieldErr("spec.target.knight.name", "%v", err)
	case err != nil:
		return k8sError("get knight", err)
	}
	if want := aiv1alpha1.RefName(spec.RoundTableRef); want != "" && table != want {
		at := "no RoundTable"
		if table != "" {
			at = fmt.Sprintf("RoundTable %q", table)
		}
		return fieldErr("spec.target.knight.name", "knight %q sits at %s, not at roundTableRef %q", kt.Name, at, want)
	}
	return nil
}

func validateTimeZone(tz string) error {
	const field = "spec.source.schedule.timeZone"
	if tz == "" {
		return nil
	}
	switch {
	case tz == "Local":
		return fieldErr(field, "must be an IANA time zone name such as \"America/New_York\", not \"Local\"")
	case len(tz) > maxTimeZoneLength:
		return fieldErr(field, "must be at most %d characters", maxTimeZoneLength)
	case !timeZonePattern.MatchString(tz):
		return fieldErr(field, "%q is not a time zone name", tz)
	}
	if _, err := time.LoadLocation(tz); err != nil {
		return fieldErr(field, "%q is not an IANA time zone name such as \"America/New_York\": %v", tz, err)
	}
	return nil
}

// validateCron parses the expression the way the trigger controller does:
// a standard five-field cron (or a descriptor such as @daily) evaluated in
// the time zone, with no TZ= prefix of its own.
func validateCron(expr, tz string) error {
	const field = "spec.source.schedule.cron"
	if strings.TrimSpace(expr) == "" {
		return fieldErr(field, "is required")
	}
	if strings.Contains(expr, "TZ=") {
		return fieldErr(field, "set the time zone with spec.source.schedule.timeZone instead of a TZ= prefix")
	}
	if tz == "" {
		tz = "UTC"
	}
	if _, err := cron.ParseStandard("CRON_TZ=" + tz + " " + expr); err != nil {
		return fieldErr(field, "%q is not a valid cron expression: %v", expr, err)
	}
	return nil
}

// refExists checks a referenced object exists; a miss names the field.
func (s *Service) refExists(ctx context.Context, ns, name, field, kind string, obj client.Object) error {
	if errs := validation.IsDNS1123Subdomain(name); len(errs) > 0 {
		return fieldErr(field, "%s", strings.Join(errs, "; "))
	}
	if err := s.Schedules.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, obj); err != nil {
		if apierrors.IsNotFound(err) {
			return fieldErr(field, "%s %s/%s not found", kind, ns, name)
		}
		return k8sError("get "+kind, err)
	}
	return nil
}

// scheduleAPIError turns an API-server validation failure (OpenAPI or CEL)
// into a FieldError naming the field the API server blamed.
func scheduleAPIError(op string, err error) error {
	var status apierrors.APIStatus
	if apierrors.IsInvalid(err) && errors.As(err, &status) {
		if d := status.Status().Details; d != nil && len(d.Causes) > 0 {
			field := d.Causes[0].Field
			msgs := make([]string, 0, len(d.Causes))
			for _, c := range d.Causes {
				if c.Field != "" && c.Field != field {
					msgs = append(msgs, c.Field+": "+c.Message)
				} else {
					msgs = append(msgs, c.Message)
				}
			}
			if field == "" {
				field = "spec"
			}
			return &FieldError{Field: field, Message: strings.Join(msgs, "; ")}
		}
	}
	return k8sError(op, err)
}
