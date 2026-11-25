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
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// TestExamplesDecodeStrictly decodes every Round Table object in examples/
// into the API types, rejecting unknown fields, so the examples can't drift
// from the CRDs (the API server would silently prune a stale field).
func TestExamplesDecodeStrictly(t *testing.T) {
	files, err := filepath.Glob("../../examples/*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	nested, err := filepath.Glob("../../examples/*/*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	files = append(files, nested...)
	recipes, err := filepath.Glob("../../examples/recipes/*/*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	files = append(files, recipes...)
	if len(files) == 0 {
		t.Fatal("no examples found")
	}

	sep := regexp.MustCompile(`(?m)^---\s*$`)
	decoded := 0
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for i, doc := range sep.Split(string(data), -1) {
			if strings.TrimSpace(doc) == "" {
				continue
			}
			var meta struct {
				APIVersion string `json:"apiVersion"`
				Kind       string `json:"kind"`
			}
			if err := yaml.Unmarshal([]byte(doc), &meta); err != nil {
				t.Errorf("%s doc %d: %v", f, i, err)
				continue
			}
			if meta.APIVersion != GroupVersion.String() {
				continue
			}
			var obj any
			switch meta.Kind {
			case "Knight":
				obj = &Knight{}
			case "Chain":
				obj = &Chain{}
			case "Mission":
				obj = &Mission{}
			case "RoundTable":
				obj = &RoundTable{}
			case "Trigger":
				obj = &Trigger{}
			default:
				t.Errorf("%s doc %d: unknown kind %q", f, i, meta.Kind)
				continue
			}
			if err := yaml.UnmarshalStrict([]byte(doc), obj); err != nil {
				t.Errorf("%s doc %d (%s): %v", f, i, meta.Kind, err)
				continue
			}
			decoded++
		}
	}
	if decoded == 0 {
		t.Fatal("no Round Table objects decoded")
	}
}
