package controller

import (
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ManagerClientOptions returns the client options for the manager. Secrets are
// read straight from the API server: a cached Secret read would start a
// cluster-wide Secret informer (and require list/watch RBAC on Secrets).
func ManagerClientOptions() client.Options {
	return client.Options{Cache: &client.CacheOptions{
		DisableFor: []client.Object{&corev1.Secret{}},
	}}
}
