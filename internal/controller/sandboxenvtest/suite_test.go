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

// Package sandboxenvtest tests sandbox-runtime knights against the real
// agent-sandbox v1beta1 CRD schemas (test/crds/agent-sandbox, see
// `make update-agent-sandbox-crds`). It is a separate package from
// internal/controller because that suite relies on the Sandbox API being
// absent (#186). The agent-sandbox controller itself is not run: tests write
// Sandbox status by hand.
package sandboxenvtest

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/controller"
	rtruntime "github.com/roundtable-k8s/roundtable/pkg/runtime"
)

var (
	ctx       context.Context
	cancel    context.CancelFunc
	testEnv   *envtest.Environment
	cfg       *rest.Config
	k8sClient client.Client
	mgrDone   chan error
)

func TestSandboxEnvtest(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Sandbox Envtest Suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))
	ctx, cancel = context.WithCancel(context.TODO())

	scheme := k8sruntime.NewScheme()
	Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
	Expect(aiv1alpha1.AddToScheme(scheme)).To(Succeed())

	By("bootstrapping test environment with the agent-sandbox CRDs")
	testEnv = &envtest.Environment{
		CRDDirectoryPaths: []string{
			filepath.Join("..", "..", "..", "config", "crd", "bases"),
			filepath.Join("..", "..", "..", "test", "crds", "agent-sandbox"),
		},
		ErrorIfCRDPathMissing: true,
	}
	if dir := firstFoundEnvTestBinaryDir(); dir != "" {
		testEnv.BinaryAssetsDirectory = dir
	}

	var err error
	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
	Expect(err).NotTo(HaveOccurred())

	By("starting the Knight controller with the sandbox backend enabled")
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Controller:             config.Controller{SkipNameValidation: new(true)},
	})
	Expect(err).NotTo(HaveOccurred())

	r := &controller.KnightReconciler{
		Client:         mgr.GetClient(),
		Scheme:         mgr.GetScheme(),
		Recorder:       record.NewFakeRecorder(1000),
		DefaultImage:   "test:latest",
		SandboxEnabled: true,
	}
	deploymentBackend := r.NewDeploymentBackend()
	r.RuntimeBackend = deploymentBackend
	r.RuntimeBackends = map[string]rtruntime.RuntimeBackend{
		"deployment": deploymentBackend,
		"sandbox": rtruntime.NewSandboxBackend(
			mgr.GetClient(), mgr.GetScheme(), r.DefaultImage, r.BuildDrainablePodSpec),
	}
	Expect(r.SetupWithManager(mgr)).To(Succeed())

	mgrDone = make(chan error, 1)
	go func() {
		defer GinkgoRecover()
		mgrDone <- mgr.Start(ctx)
	}()
})

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	cancel()
	Eventually(mgrDone, time.Minute).Should(Receive(BeNil()))
	Eventually(func() error { return testEnv.Stop() }, time.Minute, time.Second).Should(Succeed())
})

// firstFoundEnvTestBinaryDir finds bin/k8s/<version> so the suite also runs
// from an IDE; `make test` sets KUBEBUILDER_ASSETS instead.
func firstFoundEnvTestBinaryDir() string {
	basePath := filepath.Join("..", "..", "..", "bin", "k8s")
	entries, err := os.ReadDir(basePath)
	if err != nil {
		return ""
	}
	for _, entry := range entries {
		if entry.IsDir() {
			return filepath.Join(basePath, entry.Name())
		}
	}
	return ""
}
