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

package knight

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

var _ = Describe("WithGitHubToken", func() {
	var knight *aiv1alpha1.Knight

	newTable := func(name, secret, key string) *aiv1alpha1.RoundTable {
		return &aiv1alpha1.RoundTable{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: aiv1alpha1.RoundTableSpec{
				NATS: aiv1alpha1.RoundTableNATS{SubjectPrefix: "t", TasksStream: "t", ResultsStream: "r"},
				GitHub: &aiv1alpha1.GitHubConfig{
					TokenSecretRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: secret}, Key: key},
				},
			},
		}
	}

	newReader := func(objs ...*aiv1alpha1.RoundTable) *fake.ClientBuilder {
		scheme := runtime.NewScheme()
		Expect(aiv1alpha1.AddToScheme(scheme)).To(Succeed())
		b := fake.NewClientBuilder().WithScheme(scheme)
		for _, o := range objs {
			b = b.WithObjects(o)
		}
		return b
	}

	tokenVolume := func(spec corev1.PodSpec) *corev1.Volume {
		for i := range spec.Volumes {
			if spec.Volumes[i].Name == "github-token" {
				return &spec.Volumes[i]
			}
		}
		return nil
	}

	envValue := func(c corev1.Container, name string) (string, bool) {
		for _, e := range c.Env {
			if e.Name == name {
				return e.Value, true
			}
		}
		return "", false
	}

	BeforeEach(func() {
		knight = &aiv1alpha1.Knight{
			ObjectMeta: metav1.ObjectMeta{Name: "gh-knight", Namespace: "default", Labels: map[string]string{}},
			Spec: aiv1alpha1.KnightSpec{
				Domain: "testing",
				Skills: []string{"testing"},
				NATS:   aiv1alpha1.KnightNATS{Subjects: []string{"t.tasks.testing.>"}},
			},
		}
	})

	It("does nothing when no tokenSecretRef is configured", func() {
		spec := NewPodBuilder(knight, "img").WithGitHubToken(context.Background()).Build(context.Background())
		Expect(tokenVolume(spec)).To(BeNil())
		_, ok := envValue(spec.Containers[0], "GITHUB_TOKEN_FILE")
		Expect(ok).To(BeFalse())
	})

	It("mounts the knight's own Secret read-only and sets GITHUB_TOKEN_FILE", func() {
		knight.Spec.GitHub = &aiv1alpha1.GitHubConfig{
			TokenSecretRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "gh-app-token"}},
		}
		spec := NewPodBuilder(knight, "img").WithGitHubToken(context.Background()).Build(context.Background())

		vol := tokenVolume(spec)
		Expect(vol).NotTo(BeNil())
		Expect(vol.Secret).NotTo(BeNil())
		Expect(vol.Secret.SecretName).To(Equal("gh-app-token"))
		Expect(vol.Secret.Items).To(ConsistOf(corev1.KeyToPath{Key: "token", Path: "token"}))

		app := spec.Containers[0]
		var mount *corev1.VolumeMount
		for i := range app.VolumeMounts {
			if app.VolumeMounts[i].Name == "github-token" {
				mount = &app.VolumeMounts[i]
			}
		}
		Expect(mount).NotTo(BeNil())
		Expect(mount.MountPath).To(Equal("/var/run/secrets/roundtable/github"))
		Expect(mount.ReadOnly).To(BeTrue())
		// subPath mounts are never refreshed by the kubelet.
		Expect(mount.SubPath).To(BeEmpty())

		v, ok := envValue(app, "GITHUB_TOKEN_FILE")
		Expect(ok).To(BeTrue())
		Expect(v).To(Equal("/var/run/secrets/roundtable/github/token"))

		// The token itself is never injected into the environment.
		for _, e := range app.Env {
			if e.ValueFrom != nil {
				Expect(e.ValueFrom.SecretKeyRef).To(BeNil(), "env %s must not reference a Secret", e.Name)
			}
		}

		// Sidecars do not get the token.
		for _, c := range spec.Containers[1:] {
			for _, m := range c.VolumeMounts {
				Expect(m.Name).NotTo(Equal("github-token"))
			}
		}
	})

	It("passes optional through to the Secret volume", func() {
		knight.Spec.GitHub = &aiv1alpha1.GitHubConfig{
			TokenSecretRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "gh-app-token"},
				Key:                  "token",
				Optional:             new(true),
			},
		}
		spec := NewPodBuilder(knight, "img").WithGitHubToken(context.Background()).Build(context.Background())
		vol := tokenVolume(spec)
		Expect(vol).NotTo(BeNil())
		Expect(vol.Secret.Optional).To(HaveValue(BeTrue()))
	})

	It("inherits the RoundTable's Secret via the round-table label", func() {
		knight.Labels[aiv1alpha1.LabelRoundTable] = "fleet"
		reader := newReader(newTable("fleet", "fleet-token", "")).Build()

		spec := NewPodBuilder(knight, "img").WithReader(reader).WithGitHubToken(context.Background()).Build(context.Background())
		vol := tokenVolume(spec)
		Expect(vol).NotTo(BeNil())
		Expect(vol.Secret.SecretName).To(Equal("fleet-token"))
		Expect(vol.Secret.Items).To(ConsistOf(corev1.KeyToPath{Key: "token", Path: "token"}))
	})

	It("inherits the RoundTable's Secret via the legacy table label and honors a custom key", func() {
		knight.Labels["ai.roundtable.io/table"] = "legacy"
		reader := newReader(newTable("legacy", "legacy-token", "gh")).Build()

		spec := NewPodBuilder(knight, "img").WithReader(reader).WithGitHubToken(context.Background()).Build(context.Background())
		vol := tokenVolume(spec)
		Expect(vol).NotTo(BeNil())
		Expect(vol.Secret.SecretName).To(Equal("legacy-token"))
		Expect(vol.Secret.Items).To(ConsistOf(corev1.KeyToPath{Key: "gh", Path: "token"}))
	})

	It("prefers the knight's override over the RoundTable", func() {
		knight.Labels[aiv1alpha1.LabelRoundTable] = "fleet"
		knight.Spec.GitHub = &aiv1alpha1.GitHubConfig{
			TokenSecretRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "own-token"}, Key: "pat"},
		}
		reader := newReader(newTable("fleet", "fleet-token", "")).Build()

		ref := ResolveGitHubTokenSecretRef(context.Background(), reader, knight)
		Expect(ref).To(Equal(&corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "own-token"}, Key: "pat"}))
	})

	It("ignores a missing RoundTable", func() {
		knight.Labels[aiv1alpha1.LabelRoundTable] = "gone"
		reader := newReader().Build()
		Expect(ResolveGitHubTokenSecretRef(context.Background(), reader, knight)).To(BeNil())
	})
})

var _ = Describe("WithTableSecrets", func() {
	build := func(labels map[string]string, envFrom []corev1.EnvFromSource) corev1.PodSpec {
		scheme := runtime.NewScheme()
		Expect(aiv1alpha1.AddToScheme(scheme)).To(Succeed())
		rt := &aiv1alpha1.RoundTable{
			ObjectMeta: metav1.ObjectMeta{Name: "fleet", Namespace: "default"},
			Spec: aiv1alpha1.RoundTableSpec{
				NATS:    aiv1alpha1.RoundTableNATS{SubjectPrefix: "t", TasksStream: "t", ResultsStream: "r"},
				Secrets: []corev1.LocalObjectReference{{Name: "api-key"}, {Name: "shared"}},
			},
		}
		reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rt).Build()
		k := &aiv1alpha1.Knight{
			ObjectMeta: metav1.ObjectMeta{Name: "k", Namespace: "default", Labels: labels},
			Spec: aiv1alpha1.KnightSpec{
				Domain: "testing", Skills: []string{"testing"}, EnvFrom: envFrom,
				NATS: aiv1alpha1.KnightNATS{Subjects: []string{"t.tasks.testing.>"}},
			},
		}
		return NewPodBuilder(k, "img").WithReader(reader).WithTableSecrets(context.Background()).Build(context.Background())
	}
	names := func(spec corev1.PodSpec) []string {
		out := make([]string, 0, len(spec.Containers[0].EnvFrom))
		for _, e := range spec.Containers[0].EnvFrom {
			out = append(out, e.SecretRef.Name)
		}
		return out
	}

	It("adds table secrets for knights with the round-table label, without duplicates", func() {
		own := []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "shared"}}}}
		spec := build(map[string]string{aiv1alpha1.LabelRoundTable: "fleet"}, own)
		Expect(names(spec)).To(Equal([]string{"shared", "api-key"}))
	})

	It("supports the legacy table label", func() {
		spec := build(map[string]string{"ai.roundtable.io/table": "fleet"}, nil)
		Expect(names(spec)).To(Equal([]string{"api-key", "shared"}))
	})

	It("adds nothing for knights without a table label", func() {
		spec := build(map[string]string{}, nil)
		Expect(spec.Containers[0].EnvFrom).To(BeEmpty())
	})
})
