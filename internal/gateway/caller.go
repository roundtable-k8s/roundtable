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

package gateway

import "context"

// CallerStaticToken is the caller recorded for requests authenticated with
// one of the gateway's static bearer tokens.
const CallerStaticToken = "token"

type callerKey struct{}

// WithCaller returns ctx carrying the authenticated caller.
func WithCaller(ctx context.Context, caller string) context.Context {
	return context.WithValue(ctx, callerKey{}, caller)
}

// CallerFrom returns the authenticated caller of a request, or "".
func CallerFrom(ctx context.Context) string {
	c, _ := ctx.Value(callerKey{}).(string)
	return c
}
