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
	"fmt"
	"net/url"
	"slices"
	"strings"
)

// ValidateFlakeRefDir rejects a flake reference whose dir query parameter
// escapes the repository: an absolute path or a ".." segment, in plain or
// URL-encoded form. References without a dir parameter are accepted. It
// mirrors the pattern on KnightToolsFlake.Ref and does not check anything else
// about the reference.
func ValidateFlakeRefDir(ref string) error {
	_, query, found := strings.Cut(ref, "?")
	if !found {
		return nil
	}
	for kv := range strings.SplitSeq(query, "&") {
		key, val, _ := strings.Cut(kv, "=")
		if k, err := url.QueryUnescape(key); err != nil || k != "dir" {
			continue
		}
		dir, err := url.QueryUnescape(val)
		if err != nil {
			return fmt.Errorf("flake ref dir %q is not valid URL encoding", val)
		}
		dir = strings.ReplaceAll(dir, `\`, "/")
		if strings.HasPrefix(dir, "/") {
			return fmt.Errorf("flake ref dir %q must be relative to the repository root", dir)
		}
		if slices.Contains(strings.Split(dir, "/"), "..") {
			return fmt.Errorf("flake ref dir %q must not contain parent-directory (..) segments", dir)
		}
	}
	return nil
}
