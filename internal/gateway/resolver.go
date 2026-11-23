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
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
)

// legacyTableLabel is the older knight-to-table label.
const legacyTableLabel = "ai.roundtable.io/table"

// defaultTableName is the RoundTable used for knights without a table label
// (the same fallback mission chains use).
const defaultTableName = "default"

// target is a resolved invocation target: the knight and where its tasks and
// results live on NATS.
type target struct {
	Knight        *aiv1alpha1.Knight
	RoundTable    string
	SubjectPrefix string
	ResultsStream string
	ModelAliases  map[string]string
}

// Resolver looks knights and their RoundTables up in the Kubernetes API.
type Resolver struct {
	Client client.Reader
}

// Knight returns the named Knight.
func (r *Resolver) Knight(ctx context.Context, namespace, name string) (*aiv1alpha1.Knight, error) {
	k := &aiv1alpha1.Knight{}
	if err := r.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, k); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("%w: knight %s/%s", ErrNotFound, namespace, name)
		}
		return nil, fmt.Errorf("get knight %s/%s: %w", namespace, name, err)
	}
	return k, nil
}

// Knights lists the Knights in a namespace.
func (r *Resolver) Knights(ctx context.Context, namespace string) ([]aiv1alpha1.Knight, error) {
	var list aiv1alpha1.KnightList
	if err := r.Client.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list knights in %s: %w", namespace, err)
	}
	return list.Items, nil
}

// resolve finds the knight's RoundTable: the ai.roundtable.io/round-table
// label, then the legacy ai.roundtable.io/table label, then a RoundTable named
// "default". Without one, the subject prefix and results stream come from the
// knight's own spec.nats.
func (r *Resolver) resolve(ctx context.Context, namespace, name string) (*target, error) {
	k, err := r.Knight(ctx, namespace, name)
	if err != nil {
		return nil, err
	}
	t := &target{Knight: k}

	candidates := []string{}
	for _, label := range []string{aiv1alpha1.LabelRoundTable, legacyTableLabel} {
		if v := k.Labels[label]; v != "" {
			candidates = append(candidates, v)
		}
	}
	candidates = append(candidates, defaultTableName)
	for _, tableName := range candidates {
		rt := &aiv1alpha1.RoundTable{}
		err := r.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: tableName}, rt)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("get RoundTable %s/%s: %w", namespace, tableName, err)
		}
		t.RoundTable = rt.Name
		t.SubjectPrefix = rt.Spec.NATS.SubjectPrefix
		t.ResultsStream = rt.Spec.NATS.ResultsStream
		t.ModelAliases = rt.Spec.ModelAliases
		break
	}

	if t.SubjectPrefix == "" {
		t.SubjectPrefix = strings.TrimSuffix(knightpkg.DeriveResultsPrefix(k.Spec.NATS.Subjects), ".results")
	}
	if t.ResultsStream == "" {
		t.ResultsStream = k.Spec.NATS.ResultsStream
	}
	if t.SubjectPrefix == "" {
		return nil, fmt.Errorf("%w: knight %s/%s has no RoundTable and no spec.nats.subjects to derive a subject prefix from",
			ErrUnavailable, namespace, name)
	}
	return t, nil
}

// knightInfo summarises a Knight for list_knights.
func knightInfo(k *aiv1alpha1.Knight, defaultImage string) KnightInfo {
	table := k.Labels[aiv1alpha1.LabelRoundTable]
	if table == "" {
		table = k.Labels[legacyTableLabel]
	}
	return KnightInfo{
		Name:       k.Name,
		Namespace:  k.Namespace,
		Domain:     k.Spec.Domain,
		Model:      k.Spec.Model,
		Runtime:    string(knightpkg.AgentRuntimeForImage(knightpkg.EffectiveImage(k, defaultImage))),
		Phase:      string(k.Status.Phase),
		Ready:      k.Status.Ready,
		Suspended:  k.Spec.Suspend,
		RoundTable: table,
	}
}
