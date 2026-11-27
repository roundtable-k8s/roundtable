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

package e2e

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// TestE2ETiers checks that the tier constants in e2e_suite_test.go and the
// E2E_TIERS list in the Makefile name the same tiers, so that the TIER check
// and `make e2e-tiers` cannot drift from the scenarios that exist. It needs no
// cluster; `make test` runs it (the package is otherwise excluded from it).
// It parses e2e_suite_test.go as text, so it does not need the e2e build tag.
func TestE2ETiers(t *testing.T) {
	fromGo := tierConstants(t, filepath.Join(".", "e2e_suite_test.go"))
	fromMake := makeTiers(t, filepath.Join("..", "..", "Makefile"))

	for _, tier := range fromGo {
		if !slices.Contains(fromMake, tier) {
			t.Errorf("tier %q has a tier constant in e2e_suite_test.go but is missing from E2E_TIERS in the Makefile", tier)
		}
	}
	for _, tier := range fromMake {
		if !slices.Contains(fromGo, tier) {
			t.Errorf("tier %q is in E2E_TIERS in the Makefile but has no tier constant in e2e_suite_test.go", tier)
		}
	}
	if len(fromGo) == 0 {
		t.Fatal("found no tier constants in e2e_suite_test.go")
	}
}

// tierConstants returns the values of the tier* string constants, sorted.
func tierConstants(t *testing.T, path string) []string {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	var tiers []string
	for _, decl := range f.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			vs := spec.(*ast.ValueSpec)
			for i, name := range vs.Names {
				if !strings.HasPrefix(name.Name, "tier") || i >= len(vs.Values) {
					continue
				}
				lit, ok := vs.Values[i].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					continue
				}
				value, err := strconv.Unquote(lit.Value)
				if err != nil {
					t.Fatalf("unquote %s: %v", lit.Value, err)
				}
				tiers = append(tiers, value)
			}
		}
	}
	sort.Strings(tiers)
	return tiers
}

// makeTiers returns the words of the E2E_TIERS assignment in the Makefile, sorted.
func makeTiers(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		if rest, ok := strings.CutPrefix(line, "E2E_TIERS :="); ok {
			tiers := strings.Fields(rest)
			sort.Strings(tiers)
			return tiers
		}
	}
	t.Fatalf("%s has no E2E_TIERS assignment", path)
	return nil
}
