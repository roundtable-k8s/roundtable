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
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

// Every Round Table object under examples/ (at any depth) must be accepted by
// the API server, so required fields, enums, patterns, minItems and the CRDs'
// CEL rules are enforced on the examples. Complements
// TestExamplesDecodeStrictly in api/v1alpha1, which only catches unknown fields.
var _ = Describe("examples admission", func() {
	const ns = "examples-admission"

	It("accepts every Round Table example via server-side dry run", func() {
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})).To(Succeed())

		root := filepath.Join("..", "..", "examples")
		var files []string
		err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() && (strings.HasSuffix(p, ".yaml") || strings.HasSuffix(p, ".yml")) {
				files = append(files, p)
			}
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(files).NotTo(BeEmpty())

		sep := regexp.MustCompile(`(?m)^---\s*$`)
		var failures []string
		checked := 0
		for _, f := range files {
			data, err := os.ReadFile(f)
			Expect(err).NotTo(HaveOccurred())
			for i, doc := range sep.Split(string(data), -1) {
				if strings.TrimSpace(doc) == "" {
					continue
				}
				obj := &unstructured.Unstructured{}
				if err := yaml.Unmarshal([]byte(doc), &obj.Object); err != nil || obj.Object == nil {
					continue // not a Kubernetes object (values files, templates)
				}
				if obj.GroupVersionKind().Group != aiv1alpha1.GroupVersion.Group {
					continue
				}
				obj.SetNamespace(ns)
				if err := k8sClient.Create(ctx, obj, client.DryRunAll); err != nil {
					failures = append(failures, f+" doc "+strconv.Itoa(i)+" ("+obj.GetKind()+"/"+obj.GetName()+"): "+err.Error())
					continue
				}
				checked++
			}
		}
		Expect(failures).To(BeEmpty(), "examples rejected by the API server:\n%s", strings.Join(failures, "\n"))
		Expect(checked).To(BeNumerically(">", 0))
	})
})
