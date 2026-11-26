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
	"slices"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// acceptedExampleNATSURLs is the set of spec.nats.url values an example may
// use: the quickstart's in-cluster NATS (namespace roundtable) and the
// namespace "database" of the author's cluster used by the other examples.
// The two disagree on purpose until the owner picks one (see
// examples/README.md, "Adapting the examples"). A new author-specific value
// must not slip in: extend this set only deliberately.
var acceptedExampleNATSURLs = []string{
	"nats://nats.roundtable.svc:4222",
	"nats://nats.database.svc:4222",
}

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
			var natsURL string
			switch o := obj.(type) {
			case *Knight:
				natsURL = o.Spec.NATS.URL
			case *RoundTable:
				natsURL = o.Spec.NATS.URL
			}
			if natsURL != "" && !slices.Contains(acceptedExampleNATSURLs, natsURL) {
				t.Errorf("%s doc %d (%s): spec.nats.url %q is not one of %v", f, i, meta.Kind, natsURL, acceptedExampleNATSURLs)
			}
			decoded++
		}
	}
	if decoded == 0 {
		t.Fatal("no Round Table objects decoded")
	}
}

// TestBacklogGroomingRecipe pins the backlog-grooming recipe's policy: its
// prompts carry the three binding rules, the file step is gated on the
// survey, and nothing names a real repository.
func TestBacklogGroomingRecipe(t *testing.T) {
	dir := "../../examples/recipes/backlog-grooming"
	data, err := os.ReadFile(filepath.Join(dir, "chain.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var chain Chain
	if err := yaml.UnmarshalStrict(data, &chain); err != nil {
		t.Fatal(err)
	}
	steps := map[string]ChainStep{}
	for _, s := range chain.Spec.Steps {
		steps[s.Name] = s
	}
	for _, name := range []string{"survey", "file"} {
		step, ok := steps[name]
		if !ok {
			t.Fatalf("step %q missing", name)
		}
		for _, rule := range []string{
			"Never add the label rt:ready",
			"Never touch an issue labelled humanLabel",
			"File at most maxIssuesPerRun issues per run",
		} {
			if !strings.Contains(step.Task, rule) {
				t.Errorf("step %s: prompt lacks rule %q", name, rule)
			}
		}
	}
	if steps["file"].When == "" || !strings.Contains(steps["file"].When, "steps.survey.output") {
		t.Errorf("file step must be gated with when on survey output, got %q", steps["file"].When)
	}

	var trig Trigger
	tdata, err := os.ReadFile(filepath.Join(dir, "trigger.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.UnmarshalStrict(tdata, &trig); err != nil {
		t.Fatal(err)
	}
	if trig.Spec.Source.Schedule == nil {
		t.Error("trigger must use a schedule source")
	}
	var in map[string]any
	if err := yaml.Unmarshal([]byte(trig.Spec.Input), &in); err != nil {
		t.Fatalf("trigger input is not JSON: %v", err)
	}
	for _, k := range []string{"repo", "epicLabel", "issueLabel", "humanLabel", "maxIssuesPerRun", "maxOpenGroomed"} {
		if _, ok := in[k]; !ok {
			t.Errorf("trigger input lacks %q", k)
		}
	}
	if in["repo"] != "OWNER/REPO" {
		t.Errorf("repo must be a placeholder, got %v", in["repo"])
	}
	for _, f := range []string{"knight.yaml", "chain.yaml", "trigger.yaml", "README.md"} {
		b, err := os.ReadFile(filepath.Join(dir, f))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(b), "roundtable-k8s") || strings.Contains(string(b), "dapperdivers") {
			t.Errorf("%s names a real org", f)
		}
	}
}

// TestOllamaKnightExample pins the local-model Knight: it runs an ollama/<tag>
// model against the in-cluster endpoint from KNIGHT_MODEL_BASE_URL, reaches
// that Service through allowedServices (not an open host), and holds no
// hosted provider key.
func TestOllamaKnightExample(t *testing.T) {
	data, err := os.ReadFile("../../examples/knight-ollama.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var knight Knight
	if err := yaml.UnmarshalStrict(data, &knight); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(knight.Spec.Model, "ollama/") || len(knight.Spec.Model) == len("ollama/") {
		t.Errorf("model = %q, want ollama/<tag>", knight.Spec.Model)
	}
	var baseURL string
	for _, e := range knight.Spec.Env {
		switch e.Name {
		case "KNIGHT_MODEL_BASE_URL":
			baseURL = e.Value
		case "ANTHROPIC_API_KEY", "OPENROUTER_API_KEY", "OPENAI_API_KEY":
			t.Errorf("hosted provider key %s must not be set", e.Name)
		}
	}
	if !strings.HasPrefix(baseURL, "http://") || !strings.HasSuffix(baseURL, "/v1") {
		t.Errorf("KNIGHT_MODEL_BASE_URL = %q, want http(s) URL ending in /v1", baseURL)
	}
	if knight.Spec.Egress == nil || len(knight.Spec.Egress.AllowedServices) == 0 {
		t.Fatal("egress.allowedServices must name the model Service")
	}
	svc := knight.Spec.Egress.AllowedServices[0]
	if !strings.Contains(baseURL, svc.Name+"."+svc.Namespace+".svc") {
		t.Errorf("KNIGHT_MODEL_BASE_URL %q does not point at allowedServices[0] %s/%s", baseURL, svc.Namespace, svc.Name)
	}
	if !slices.Contains(svc.Ports, 11434) {
		t.Errorf("allowedServices[0] ports = %v, want Ollama port 11434", svc.Ports)
	}
}
