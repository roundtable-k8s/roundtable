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
	"errors"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/util"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

const (
	// defaultServiceInfoTimeout bounds the $SRV.INFO request for knight skills.
	defaultServiceInfoTimeout = time.Second
	// skillsQueryInterval is the minimum time between $SRV.INFO requests for a
	// table, so Knight status events do not each cost a request; the slow
	// resync (RequeueVerySlow) always passes it.
	skillsQueryInterval = RequeueVerySlow - 10*time.Second
)

// errNoNATSConn is returned when the NATS client does not expose a connection.
var errNoNATSConn = errors.New("NATS client has no connection")

// buildKnightCard generates a knight's agent card from its spec and status.
// runtimeSkills are the skills the running knight last reported (nil before
// it has ever reported any); spec.card overrides are merged over the result.
// UpdatedAt is left empty for the caller to set when the card changed.
func buildKnightCard(rt *aiv1alpha1.RoundTable, k *aiv1alpha1.Knight, runtimeSkills []natspkg.RuntimeSkill) *natspkg.AgentCard {
	model := k.Status.Model
	if model == "" {
		model = k.Spec.Model
	}
	runtime := k.Spec.Runtime
	if runtime == "" {
		runtime = runtimeDeployment
	}
	tools := []string{}
	if k.Spec.Tools != nil {
		tools = append(tools, k.Spec.Tools.Nix...)
	}

	card := &natspkg.AgentCard{
		SchemaVersion:      natspkg.CardSchemaVersion,
		Name:               k.Name,
		Description:        cardDescription(k),
		Version:            strconv.FormatInt(k.Generation, 10),
		DefaultInputModes:  []string{"text/plain"},
		DefaultOutputModes: []string{"text/plain"},
		RoundTable: natspkg.CardMeta{
			Table:         rt.Name,
			Knight:        k.Name,
			Domain:        k.Spec.Domain,
			Model:         model,
			Tools:         tools,
			TaskSubject:   natspkg.TaskSubject(knightSubjectPrefix(rt, k), k.Spec.Domain, k.Name),
			Runtime:       runtime,
			Phase:         string(k.Status.Phase),
			ReadyReplicas: k.Status.ReadyReplicas,
			Suspended:     k.Spec.Suspend,
			SkillsSource:  natspkg.CardSkillsFromSpec,
		},
	}
	if k.Spec.Card != nil && len(k.Spec.Card.Tags) > 0 {
		card.RoundTable.Tags = append([]string{}, k.Spec.Card.Tags...)
	}

	if runtimeSkills != nil {
		card.RoundTable.SkillsSource = natspkg.CardSkillsFromRuntime
		card.RoundTable.RuntimeSkills = runtimeSkills
		card.Skills = make([]natspkg.AgentSkill, 0, len(runtimeSkills))
		for _, s := range runtimeSkills {
			tags := []string{}
			if s.Category != "" {
				tags = append(tags, s.Category)
			}
			card.Skills = append(card.Skills, natspkg.AgentSkill{
				ID: s.Name, Name: s.Name, Description: s.Description, Tags: tags, Examples: []string{},
			})
		}
	} else {
		card.Skills = make([]natspkg.AgentSkill, 0, len(k.Spec.Skills))
		for _, category := range k.Spec.Skills {
			card.Skills = append(card.Skills, natspkg.AgentSkill{
				ID: category, Name: category, Description: category + " skills",
				Tags: []string{category}, Examples: []string{},
			})
		}
	}

	if k.Spec.Card != nil {
		card.Skills = mergeCardSkills(card.Skills, k.Spec.Card.Skills)
	}
	return card
}

// cardDescription is spec.card.description, else the first paragraph of
// spec.prompt.identity, else "<Name>, <domain> knight".
func cardDescription(k *aiv1alpha1.Knight) string {
	if k.Spec.Card != nil && strings.TrimSpace(k.Spec.Card.Description) != "" {
		return strings.TrimSpace(k.Spec.Card.Description)
	}
	if k.Spec.Prompt != nil {
		if p := firstParagraph(k.Spec.Prompt.Identity); p != "" {
			return p
		}
	}
	return util.Capitalize(k.Name) + ", " + k.Spec.Domain + " knight"
}

// firstParagraph returns the first non-empty paragraph of s with its lines
// joined by single spaces.
func firstParagraph(s string) string {
	var lines []string
	for line := range strings.SplitSeq(strings.ReplaceAll(s, "\r\n", "\n"), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			if len(lines) > 0 {
				break
			}
			continue
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, " ")
}

// mergeCardSkills applies spec.card.skills over the generated skills by id:
// set fields replace the generated ones; an unknown id appends a skill.
func mergeCardSkills(skills []natspkg.AgentSkill, overrides []aiv1alpha1.KnightCardSkill) []natspkg.AgentSkill {
	for _, o := range overrides {
		i := slices.IndexFunc(skills, func(s natspkg.AgentSkill) bool { return s.ID == o.ID })
		if i < 0 {
			skills = append(skills, natspkg.AgentSkill{ID: o.ID, Name: o.ID, Tags: []string{}, Examples: []string{}})
			i = len(skills) - 1
		}
		if o.Description != "" {
			skills[i].Description = o.Description
		}
		if o.Tags != nil {
			skills[i].Tags = append([]string{}, o.Tags...)
		}
		if o.Examples != nil {
			skills[i].Examples = append([]string{}, o.Examples...)
		}
	}
	return skills
}

// knightSubjectPrefix is the subject prefix of the knight's first task
// subject ("<prefix>.tasks..."), else the table's subjectPrefix.
func knightSubjectPrefix(rt *aiv1alpha1.RoundTable, k *aiv1alpha1.Knight) string {
	if len(k.Spec.NATS.Subjects) > 0 {
		if prefix, _, found := strings.Cut(k.Spec.NATS.Subjects[0], ".tasks."); found && prefix != "" {
			return prefix
		}
	}
	return rt.Spec.NATS.SubjectPrefix
}

// natsConn returns the shared NATS connection, or errNoNATSConn.
func natsConn(provider *natspkg.Provider) (*nats.Conn, error) {
	if provider == nil {
		return nil, errNoNATSConn
	}
	cli, err := provider.Client()
	if err != nil {
		return nil, err
	}
	cp, ok := cli.(natspkg.ConnProvider)
	if !ok || cp.Conn() == nil {
		return nil, errNoNATSConn
	}
	return cp.Conn(), nil
}

// cardsBucket returns the configured cards bucket or the default.
func (r *RoundTableReconciler) cardsBucket() string {
	if r.CardsBucket != "" {
		return r.CardsBucket
	}
	return natspkg.DefaultCardsBucket
}

// syncCards writes the card of every member knight of rt (only when its
// content changed) and deletes the table's cards whose knight is no longer a
// member. Knights being deleted lose their card.
func (r *RoundTableReconciler) syncCards(ctx context.Context, rt *aiv1alpha1.RoundTable, knights []aiv1alpha1.Knight) error {
	log := logf.FromContext(ctx)
	nc, err := natsConn(r.NATS)
	if err != nil {
		return err
	}
	store, err := natspkg.NewCardStore(nc, r.cardsBucket())
	if err != nil {
		return err
	}

	infos, queried := r.queryKnightServices(ctx, nc, rt, knights)

	members := map[string]bool{}
	var errs []error
	for i := range knights {
		k := &knights[i]
		if k.DeletionTimestamp != nil {
			continue
		}
		key := natspkg.CardKey(rt.Name, k.Name)
		members[key] = true

		existing, err := store.Get(key)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		var runtimeSkills []natspkg.RuntimeSkill
		if skills, ok := natspkg.KnightSkills(infos, rt.Name, k.Name); queried && ok {
			runtimeSkills = skills
		} else if existing != nil && existing.RoundTable.SkillsSource == natspkg.CardSkillsFromRuntime {
			// Not running, or no answer: keep what the card already has.
			runtimeSkills = existing.RoundTable.RuntimeSkills
			if runtimeSkills == nil {
				runtimeSkills = []natspkg.RuntimeSkill{}
			}
		}

		card := buildKnightCard(rt, k, runtimeSkills)
		if natspkg.SameCard(existing, card) {
			continue
		}
		card.RoundTable.UpdatedAt = r.now().UTC().Format(time.RFC3339)
		if err := store.Put(key, card); err != nil {
			errs = append(errs, err)
			continue
		}
		log.V(1).Info("Wrote agent card", "key", key)
	}

	keys, err := store.TableKeys(rt.Name)
	if err != nil {
		errs = append(errs, err)
	}
	for _, key := range keys {
		if members[key] {
			continue
		}
		if err := store.Delete(key); err != nil {
			errs = append(errs, err)
			continue
		}
		log.Info("Deleted agent card of a knight that left the table", "key", key)
	}
	return errors.Join(errs...)
}

// queryKnightServices requests $SRV.INFO from the knight micro services when
// a member knight has ready pods and the table was not queried within
// skillsQueryInterval. queried is false when no request was made (or it failed).
func (r *RoundTableReconciler) queryKnightServices(
	ctx context.Context, nc *nats.Conn, rt *aiv1alpha1.RoundTable, knights []aiv1alpha1.Knight,
) (infos []natspkg.ServiceInfo, queried bool) {
	if !slices.ContainsFunc(knights, func(k aiv1alpha1.Knight) bool { return k.Status.ReadyReplicas > 0 }) {
		return nil, false
	}
	tableKey := rt.Namespace + "/" + rt.Name
	now := r.now()
	r.skillsMu.Lock()
	if last, ok := r.skillsQueriedAt[tableKey]; ok && now.Sub(last) < skillsQueryInterval {
		r.skillsMu.Unlock()
		return nil, false
	}
	if r.skillsQueriedAt == nil {
		r.skillsQueriedAt = map[string]time.Time{}
	}
	r.skillsQueriedAt[tableKey] = now
	r.skillsMu.Unlock()

	timeout := r.ServiceInfoTimeout
	if timeout <= 0 {
		timeout = defaultServiceInfoTimeout
	}
	infos, err := natspkg.RequestServiceInfo(nc, natspkg.KnightServiceName, timeout)
	if err != nil {
		logf.FromContext(ctx).Error(err, "Knight service INFO request failed")
		return nil, false
	}
	return infos, true
}

// deleteTableCards removes every card of a table (the table is gone).
func (r *RoundTableReconciler) deleteTableCards(ctx context.Context, table string) {
	nc, err := natsConn(r.NATS)
	if err != nil {
		return
	}
	store, err := natspkg.NewCardStore(nc, r.cardsBucket())
	if err != nil {
		logf.FromContext(ctx).Error(err, "Failed to open cards bucket")
		return
	}
	keys, err := store.TableKeys(table)
	if err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list table cards")
		return
	}
	for _, key := range keys {
		if err := store.Delete(key); err != nil {
			logf.FromContext(ctx).Error(err, "Failed to delete agent card", "key", key)
		}
	}
}

// now returns the reconciler clock.
func (r *RoundTableReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// knightTables maps a Knight event to every RoundTable in its namespace, so a
// spec or status change (or the knight's deletion, or a label change moving
// it between tables) updates the cards within seconds. Membership is decided
// by the table reconcile itself, which also drops stale cards.
func (r *RoundTableReconciler) knightTables(ctx context.Context, obj client.Object) []reconcile.Request {
	tables := &aiv1alpha1.RoundTableList{}
	if err := r.List(ctx, tables, client.InNamespace(obj.GetNamespace())); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list RoundTables for knight", "knight", obj.GetName())
		return nil
	}
	reqs := make([]reconcile.Request, 0, len(tables.Items))
	for _, t := range tables.Items {
		reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&t)})
	}
	return reqs
}

// deleteKnightCards removes a deleted knight's card from every table in its
// namespace (the knight finalizer path; the table reconcile does the same).
func deleteKnightCards(ctx context.Context, c client.Reader, provider *natspkg.Provider, bucket string, k *aiv1alpha1.Knight) error {
	nc, err := natsConn(provider)
	if err != nil {
		return nil //nolint:nilerr // no NATS: nothing to clean up
	}
	tables := &aiv1alpha1.RoundTableList{}
	if err := c.List(ctx, tables, client.InNamespace(k.Namespace)); err != nil {
		return err
	}
	if len(tables.Items) == 0 {
		return nil
	}
	if bucket == "" {
		bucket = natspkg.DefaultCardsBucket
	}
	store, err := natspkg.NewCardStore(nc, bucket)
	if err != nil {
		return err
	}
	var errs []error
	for _, t := range tables.Items {
		if err := store.Delete(natspkg.CardKey(t.Name, k.Name)); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// reconcileCards syncs the table's cards; failures are logged, never fatal
// to the table reconcile. Without NATS there is nothing to do.
func (r *RoundTableReconciler) reconcileCards(ctx context.Context, rt *aiv1alpha1.RoundTable, knights []aiv1alpha1.Knight) {
	if err := r.syncCards(ctx, rt, knights); err != nil && !errors.Is(err, errNoNATSConn) {
		logf.FromContext(ctx).Error(err, "Failed to sync agent cards")
	}
}

// cleanupDeletedTableCards deletes a deleted table's cards, unless a table of
// the same name still exists in another namespace (the key space is by name).
func (r *RoundTableReconciler) cleanupDeletedTableCards(ctx context.Context, table string) {
	if r.NATS == nil {
		return
	}
	tables := &aiv1alpha1.RoundTableList{}
	if err := r.List(ctx, tables); err != nil {
		return
	}
	for _, t := range tables.Items {
		if t.Name == table {
			return
		}
	}
	r.deleteTableCards(ctx, table)
}

// knightChangedForTable filters the Knight watch: creates and deletes pass;
// updates pass only when the generation, the labels or a status field a table
// reads change (phase, ready, readyReplicas, model and the Available
// condition's status and reason). Other status-only writes are dropped, and
// so are annotation-only updates (for example the planner spec-hash): they
// change neither generation, labels nor those status fields, on purpose.
func knightChangedForTable() predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldK, ok1 := e.ObjectOld.(*aiv1alpha1.Knight)
			newK, ok2 := e.ObjectNew.(*aiv1alpha1.Knight)
			if !ok1 || !ok2 {
				return true
			}
			if oldK.Generation != newK.Generation || !maps.Equal(oldK.Labels, newK.Labels) {
				return true
			}
			o, n := &oldK.Status, &newK.Status
			if o.Phase != n.Phase || o.Ready != n.Ready || o.ReadyReplicas != n.ReadyReplicas || o.Model != n.Model {
				return true
			}
			oc := meta.FindStatusCondition(o.Conditions, aiv1alpha1.ConditionKnightAvailable)
			nc := meta.FindStatusCondition(n.Conditions, aiv1alpha1.ConditionKnightAvailable)
			if (oc == nil) != (nc == nil) {
				return true
			}
			return oc != nil && (oc.Status != nc.Status || oc.Reason != nc.Reason)
		},
	}
}
