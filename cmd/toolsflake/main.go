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

// Command toolsflake prints the flake the tools builder image is warmed up
// with (see Dockerfile.tools-builder). It is the operator's own tools image
// flake for a one-package tool list, so the image build realises exactly the
// nix2container tooling a real build Job needs, at the operator's pins.
//
// With -tag it prints the tag that image must be published under instead.
package main

import (
	"flag"
	"fmt"

	"github.com/roundtable-k8s/roundtable/internal/knight"
)

func main() {
	tag := flag.Bool("tag", false, "print the tools builder image tag instead of the flake")
	flag.Parse()
	if *tag {
		fmt.Println(knight.ToolsBuilderTag())
		return
	}
	fmt.Print(knight.ToolsBuilderWarmupFlake())
}
