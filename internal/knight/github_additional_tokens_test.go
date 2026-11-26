/*
Copyright 2026.

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
	"encoding/json"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

var _ = Describe("WithGitHubAdditionalTokens", func() {
	var knight *aiv1alpha1.Knight

	secretRef := func(name string) corev1.SecretKeySelector {
		return corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: name}, Key: "token"}
	}

	build := func(tokens ...aiv1alpha1.GitHubAdditionalToken) corev1.PodSpec {
		scheme := runtime.NewScheme()
		Expect(aiv1alpha1.AddToScheme(scheme)).To(Succeed())
		rt := &aiv1alpha1.RoundTable{
			ObjectMeta: metav1.ObjectMeta{Name: "fleet", Namespace: "default"},
			Spec: aiv1alpha1.RoundTableSpec{
				NATS: aiv1alpha1.RoundTableNATS{SubjectPrefix: "t", TasksStream: "t", ResultsStream: "r"},
			},
		}
		if tokens != nil {
			rt.Spec.GitHub = &aiv1alpha1.RoundTableGitHubConfig{AdditionalTokens: tokens}
		}
		reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rt).Build()
		return NewPodBuilder(knight, "img").WithReader(reader).
			WithGitHubToken(context.Background()).
			WithGitHubAdditionalTokens(context.Background()).
			Build(context.Background())
	}

	additionalVolumes := func(spec corev1.PodSpec) []corev1.Volume {
		var out []corev1.Volume
		for _, v := range spec.Volumes {
			if strings.HasPrefix(v.Name, "github-additional-") {
				out = append(out, v)
			}
		}
		return out
	}

	additionalMounts := func(c corev1.Container) []corev1.VolumeMount {
		var out []corev1.VolumeMount
		for _, m := range c.VolumeMounts {
			if strings.HasPrefix(m.Name, "github-additional-") {
				out = append(out, m)
			}
		}
		return out
	}

	envJSON := func(spec corev1.PodSpec) ([]GitHubAdditionalTokenFile, bool) {
		for _, e := range spec.Containers[0].Env {
			if e.Name == GitHubAdditionalTokensEnv {
				var files []GitHubAdditionalTokenFile
				Expect(json.Unmarshal([]byte(e.Value), &files)).To(Succeed())
				return files, true
			}
		}
		return nil, false
	}

	expectNone := func(spec corev1.PodSpec) {
		Expect(additionalVolumes(spec)).To(BeEmpty())
		Expect(additionalMounts(spec.Containers[0])).To(BeEmpty())
		_, ok := envJSON(spec)
		Expect(ok).To(BeFalse())
	}

	BeforeEach(func() {
		knight = &aiv1alpha1.Knight{
			ObjectMeta: metav1.ObjectMeta{
				Name: "rt-operator", Namespace: "default",
				Labels: map[string]string{aiv1alpha1.LabelRoundTable: "fleet"},
			},
			Spec: aiv1alpha1.KnightSpec{
				Domain: "testing",
				Skills: []string{"testing"},
				NATS:   aiv1alpha1.KnightNATS{Subjects: []string{"t.tasks.testing.>"}},
			},
		}
	})

	It("adds no volume and no env when the table has no additionalTokens", func() {
		expectNone(build())
	})

	It("adds no volume and no env for a knight without a table", func() {
		knight.Labels = map[string]string{}
		expectNone(build(aiv1alpha1.GitHubAdditionalToken{Match: "acme", TokenSecretRef: secretRef("acme-token")}))
	})

	It("mounts a token for every knight when knights is empty, and the env JSON matches the mounts", func() {
		spec := build(
			aiv1alpha1.GitHubAdditionalToken{Match: "dapperdivers/dapper-cluster", TokenSecretRef: secretRef("dc-token")},
			aiv1alpha1.GitHubAdditionalToken{Match: "acme", TokenSecretRef: corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "acme-token"}, Key: "pat",
			}},
		)

		vols := additionalVolumes(spec)
		Expect(vols).To(HaveLen(2))
		Expect(vols[0].Name).To(Equal("github-additional-0"))
		Expect(vols[0].Secret.SecretName).To(Equal("dc-token"))
		Expect(vols[0].Secret.Items).To(ConsistOf(corev1.KeyToPath{Key: "token", Path: "token"}))
		Expect(vols[1].Name).To(Equal("github-additional-1"))
		Expect(vols[1].Secret.SecretName).To(Equal("acme-token"))
		Expect(vols[1].Secret.Items).To(ConsistOf(corev1.KeyToPath{Key: "pat", Path: "token"}))

		mounts := additionalMounts(spec.Containers[0])
		Expect(mounts).To(Equal([]corev1.VolumeMount{
			{Name: "github-additional-0", MountPath: "/var/run/secrets/roundtable/github-additional/0", ReadOnly: true},
			{Name: "github-additional-1", MountPath: "/var/run/secrets/roundtable/github-additional/1", ReadOnly: true},
		}))
		// Not nested under the default token mount, no subPath.
		for _, m := range mounts {
			Expect(m.MountPath).NotTo(HavePrefix(GitHubTokenDir + "/"))
			Expect(m.SubPath).To(BeEmpty())
		}

		files, ok := envJSON(spec)
		Expect(ok).To(BeTrue())
		Expect(files).To(Equal([]GitHubAdditionalTokenFile{
			{Match: "dapperdivers/dapper-cluster", File: "/var/run/secrets/roundtable/github-additional/0/token"},
			{Match: "acme", File: "/var/run/secrets/roundtable/github-additional/1/token"},
		}))
		for i, f := range files {
			Expect(f.File).To(Equal(mounts[i].MountPath + "/token"))
		}

		// No env var carries a token value or a Secret reference.
		for _, e := range spec.Containers[0].Env {
			if e.ValueFrom != nil {
				Expect(e.ValueFrom.SecretKeyRef).To(BeNil(), "env %s must not reference a Secret", e.Name)
			}
		}
		// Sidecars do not get the tokens.
		for _, c := range spec.Containers[1:] {
			Expect(additionalMounts(c)).To(BeEmpty())
		}
	})

	It("limits a token to the named knights, keeping the item index in the path", func() {
		spec := build(
			aiv1alpha1.GitHubAdditionalToken{Match: "other", TokenSecretRef: secretRef("other-token"), Knights: []string{"rt-lead"}},
			aiv1alpha1.GitHubAdditionalToken{Match: "dapperdivers/dapper-cluster", TokenSecretRef: secretRef("dc-token"), Knights: []string{"rt-lead", "rt-operator"}},
		)
		vols := additionalVolumes(spec)
		Expect(vols).To(HaveLen(1))
		Expect(vols[0].Name).To(Equal("github-additional-1"))
		Expect(vols[0].Secret.SecretName).To(Equal("dc-token"))
		files, ok := envJSON(spec)
		Expect(ok).To(BeTrue())
		Expect(files).To(Equal([]GitHubAdditionalTokenFile{
			{Match: "dapperdivers/dapper-cluster", File: "/var/run/secrets/roundtable/github-additional/1/token"},
		}))
	})

	It("excludes a knight not in the knights list", func() {
		expectNone(build(aiv1alpha1.GitHubAdditionalToken{
			Match: "dapperdivers/dapper-cluster", TokenSecretRef: secretRef("dc-token"), Knights: []string{"rt-lead"},
		}))
	})

	It("gives mission knights only the tokens with an empty knights list", func() {
		knight.Labels[aiv1alpha1.LabelMission] = "m1"
		knight.Name = "rt-lead" // a coincidental name match must not count
		spec := build(
			aiv1alpha1.GitHubAdditionalToken{Match: "limited", TokenSecretRef: secretRef("limited-token"), Knights: []string{"rt-lead"}},
			aiv1alpha1.GitHubAdditionalToken{Match: "everyone", TokenSecretRef: secretRef("everyone-token")},
		)
		files, ok := envJSON(spec)
		Expect(ok).To(BeTrue())
		Expect(files).To(Equal([]GitHubAdditionalTokenFile{
			{Match: "everyone", File: "/var/run/secrets/roundtable/github-additional/1/token"},
		}))
	})

	It("honours the Knight opt-out", func() {
		knight.Spec.GitHub = &aiv1alpha1.GitHubConfig{DisableAdditionalTokens: true}
		expectNone(build(aiv1alpha1.GitHubAdditionalToken{Match: "acme", TokenSecretRef: secretRef("acme-token")}))
	})

	It("passes optional through to the Secret volume", func() {
		ref := secretRef("acme-token")
		ref.Optional = new(true)
		spec := build(aiv1alpha1.GitHubAdditionalToken{Match: "acme", TokenSecretRef: ref})
		vols := additionalVolumes(spec)
		Expect(vols).To(HaveLen(1))
		Expect(vols[0].Secret.Optional).To(HaveValue(BeTrue()))
	})
})
