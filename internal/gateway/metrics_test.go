package gateway

import "testing"

func TestIsPollRoute(t *testing.T) {
	for route, want := range map[string]bool{
		"GET /v1/tasks/{taskId}":                          true,
		"GET /v1/schedules/{namespace}/{name}/runs/{run}": true,
		"GET /v1/schedules/{namespace}/{name}/runs":       true,
		"GET /v1/schedules/{namespace}/{name}":            false,
		"POST /v1/missions":                               false,
	} {
		if got := isPollRoute(route); got != want {
			t.Errorf("isPollRoute(%q) = %v, want %v", route, got, want)
		}
	}
}
