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

package v1alpha1

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// The CRD defaults for a Knight's NATS streams (kubebuilder:default on
// KnightNATS). A Knight that leaves them unset runs on these, not on its
// table's streams, so an empty value is compared as the default.
const (
	defaultKnightStream        = "fleet_a_tasks"
	defaultKnightResultsStream = "fleet_a_results"
)

// TestExampleCrossReferences checks the references between the files of one
// example directory: the quickstart and each recipe. A reference is checked
// only when its target kind is defined in the same directory, so a Chain that
// names a RoundTable kept elsewhere passes; once the directory defines that
// kind, the name must match one of its objects.
func TestExampleCrossReferences(t *testing.T) {
	dirs := []string{"../../examples/quickstart"}
	recipes, err := filepath.Glob("../../examples/recipes/*")
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recipes {
		if fi, err := os.Stat(r); err == nil && fi.IsDir() {
			dirs = append(dirs, r)
		}
	}
	for _, dir := range dirs {
		t.Run(filepath.Base(dir), func(t *testing.T) {
			files, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			var objs []exampleObject
			for _, f := range files {
				objs = append(objs, decodeExampleFile(t, f)...)
			}
			if len(objs) == 0 {
				t.Fatalf("%s: no Round Table objects", dir)
			}
			for _, msg := range checkExampleRefs(objs) {
				t.Error(msg)
			}
		})
	}
}

// checkExampleRefs returns one message per reference in objs that disagrees
// with the objects of the same kind in that set. It is pure so it can run on
// any decoded set of objects.
func checkExampleRefs(objs []exampleObject) []string {
	tables := map[string]*RoundTable{}
	knights := map[string]*Knight{}
	chains := map[string]bool{}
	for _, eo := range objs {
		switch o := eo.obj.(type) {
		case *RoundTable:
			tables[eo.name] = o
		case *Knight:
			knights[eo.name] = o
		case *Chain:
			chains[eo.name] = true
		}
	}
	tableNames := keySet(tables)
	knightNames := keySet(knights)

	var errs []string
	// name checks a reference to a kind this directory defines: the name must
	// be one of its objects. An empty name, or a kind the directory does not
	// define, is skipped.
	name := func(eo exampleObject, field, kind string, defined map[string]bool, ref string) {
		if ref == "" || len(defined) == 0 || defined[ref] {
			return
		}
		errs = append(errs, exampleErr(eo, field, "%q names no %s in this directory (have %v)",
			ref, kind, slices.Sorted(maps.Keys(defined))))
	}

	for _, eo := range objs {
		switch o := eo.obj.(type) {
		case *Chain:
			name(eo, "spec.roundTableRef.name", "RoundTable", tableNames, RefName(o.Spec.RoundTableRef))
			name(eo, "spec.outputKnightRef.name", "Knight", knightNames, RefName(o.Spec.OutputKnightRef))
			for i, s := range o.Spec.Steps {
				name(eo, fmt.Sprintf("spec.steps[%d].knightRef.name", i), "Knight", knightNames, s.KnightRef.Name)
				if s.Loop == nil {
					continue
				}
				for j, b := range s.Loop.Steps {
					name(eo, fmt.Sprintf("spec.steps[%d].loop.steps[%d].knightRef.name", i, j), "Knight", knightNames, b.KnightRef.Name)
				}
			}
		case *Trigger:
			name(eo, "spec.roundTableRef.name", "RoundTable", tableNames, RefName(o.Spec.RoundTableRef))
			name(eo, "spec.target.chainRef.name", "Chain", chains, RefName(o.Spec.Target.ChainRef))
			if k := o.Spec.Target.Knight; k != nil {
				name(eo, "spec.target.knight.name", "Knight", knightNames, k.Name)
			}
		case *Knight:
			errs = append(errs, checkKnightStreams(eo, o, tables)...)
		}
	}
	return errs
}

// checkKnightStreams checks a Knight's streams against the RoundTable whose
// subjectPrefix its task subjects sit under. It returns nothing when the
// directory defines no RoundTable.
func checkKnightStreams(eo exampleObject, k *Knight, tables map[string]*RoundTable) []string {
	if len(tables) == 0 {
		return nil
	}
	subjects := k.Spec.NATS.Subjects
	var table *RoundTable
	for _, name := range slices.Sorted(maps.Keys(tables)) {
		prefix := tables[name].Spec.NATS.SubjectPrefix + ".tasks."
		if len(subjects) > 0 && !slices.ContainsFunc(subjects, func(s string) bool { return !strings.HasPrefix(s, prefix) }) {
			table = tables[name]
			break
		}
	}
	if table == nil {
		return []string{exampleErr(eo, "spec.nats.subjects", "%v start with no RoundTable subjectPrefix in this directory (have %v)",
			subjects, subjectPrefixes(tables))}
	}

	var errs []string
	if got := orDefault(k.Spec.NATS.Stream, defaultKnightStream); got != table.Spec.NATS.TasksStream {
		errs = append(errs, exampleErr(eo, "spec.nats.stream", "%q, but RoundTable %s has tasksStream %q",
			got, table.Name, table.Spec.NATS.TasksStream))
	}
	if got := orDefault(k.Spec.NATS.ResultsStream, defaultKnightResultsStream); got != table.Spec.NATS.ResultsStream {
		errs = append(errs, exampleErr(eo, "spec.nats.resultsStream", "%q, but RoundTable %s has resultsStream %q",
			got, table.Name, table.Spec.NATS.ResultsStream))
	}
	return errs
}

// exampleErr names the file, object and field a message is about.
func exampleErr(eo exampleObject, field, format string, args ...any) string {
	return fmt.Sprintf("%s (%s %s) %s: %s", eo.file, eo.kind, eo.name, field, fmt.Sprintf(format, args...))
}

func keySet[V any](m map[string]V) map[string]bool {
	set := make(map[string]bool, len(m))
	for k := range m {
		set[k] = true
	}
	return set
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func subjectPrefixes(tables map[string]*RoundTable) []string {
	var out []string
	for _, rt := range tables {
		out = append(out, rt.Spec.NATS.SubjectPrefix)
	}
	slices.Sort(out)
	return out
}
