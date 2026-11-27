package controller

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"io/fs"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// nonWarningReasons are Reason constants assigned to a same-named variable in a
// marked function but never emitted by the marked Warning call (the healthy
// branch, or a later condition that reuses the variable name).
var nonWarningReasons = map[string]bool{"ScaledObjectReady": true, "PlanCapApplied": true}

// TestEventReasonMarkers ties each reasons marker comment (read by
// scripts/check-events-doc.sh) to the code it annotates. For the Warning
// Recorder.Event call on the line below a marker, it works out the reasons the
// call can emit from the AST — a constant, the assignments to the reason
// variable in the enclosing function, the arguments of every caller when the
// reason is a parameter, or the composite literals of a local struct's reason
// field — and requires them to equal the marker's set. A new reason on a
// marked path therefore fails here until the marker (and docs/events.md) is
// updated.
func TestEventReasonMarkers(t *testing.T) {
	fset := token.NewFileSet()
	consts := map[string]string{}
	var files []*ast.File
	for _, dir := range []string{".", "../../api/v1alpha1"} {
		pkgs, err := parser.ParseDir(fset, dir, func(fi fs.FileInfo) bool {
			return !strings.HasSuffix(fi.Name(), "_test.go")
		}, parser.ParseComments)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range pkgs {
			for _, f := range p.Files {
				if dir == "." {
					files = append(files, f)
				}
				for _, d := range f.Decls {
					g, ok := d.(*ast.GenDecl)
					if !ok {
						continue
					}
					for _, s := range g.Specs {
						vs, ok := s.(*ast.ValueSpec)
						if !ok {
							continue
						}
						for i, n := range vs.Names {
							if i < len(vs.Values) {
								if bl, ok := vs.Values[i].(*ast.BasicLit); ok && bl.Kind == token.STRING {
									consts[n.Name], _ = strconv.Unquote(bl.Value)
								}
							}
						}
					}
				}
			}
		}
	}
	if len(consts) == 0 {
		t.Fatal("no string constants parsed")
	}

	// resolve returns the reason strings an expression denotes: a quoted
	// string, or a (possibly package-qualified) string constant.
	resolve := func(e ast.Expr) (string, bool) {
		switch x := e.(type) {
		case *ast.BasicLit:
			s, err := strconv.Unquote(x.Value)
			return s, err == nil
		case *ast.Ident:
			s, ok := consts[x.Name]
			return s, ok
		case *ast.SelectorExpr:
			s, ok := consts[x.Sel.Name]
			return s, ok
		}
		return "", false
	}
	text := func(e ast.Expr) string { return types2str(fset, e) }

	// callers maps a function name to every call of it in the package.
	callers := map[string][]*ast.CallExpr{}
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			if c, ok := n.(*ast.CallExpr); ok {
				switch fn := c.Fun.(type) {
				case *ast.Ident:
					callers[fn.Name] = append(callers[fn.Name], c)
				case *ast.SelectorExpr:
					callers[fn.Sel.Name] = append(callers[fn.Sel.Name], c)
				}
			}
			return true
		})
	}

	// reasons collects what expression e can hold inside function fn.
	var reasons func(fn *ast.FuncDecl, e ast.Expr) []string
	reasons = func(fn *ast.FuncDecl, e ast.Expr) []string {
		if s, ok := resolve(e); ok {
			return []string{s}
		}
		var out []string
		want := text(e)
		// Parameter: take the matching argument of every caller.
		if id, ok := e.(*ast.Ident); ok {
			idx := 0
			for _, fld := range fn.Type.Params.List {
				for _, n := range fld.Names {
					if n.Name == id.Name {
						for _, c := range callers[fn.Name.Name] {
							if idx < len(c.Args) {
								s, ok := resolve(c.Args[idx])
								if !ok {
									t.Errorf("%s: cannot resolve argument %d of %s", fset.Position(c.Pos()), idx, fn.Name.Name)
								}
								out = append(out, s)
							}
						}
						return out
					}
					idx++
				}
			}
		}
		// Local struct field: first element/keyed value of its literals.
		field := ""
		if se, ok := e.(*ast.SelectorExpr); ok {
			field = se.Sel.Name
		}
		structs := map[string]int{}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			ts, ok := n.(*ast.TypeSpec)
			if !ok {
				return true
			}
			if st, ok := ts.Type.(*ast.StructType); ok && field != "" {
				i := 0
				for _, f := range st.Fields.List {
					for _, nm := range f.Names {
						if nm.Name == field {
							structs[ts.Name.Name] = i
						}
						i++
					}
				}
			}
			return true
		})
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.AssignStmt:
				for i, l := range x.Lhs {
					if text(l) == want && i < len(x.Rhs) {
						out = append(out, reasons(fn, x.Rhs[i])...)
					}
				}
			case *ast.CompositeLit:
				if id, ok := x.Type.(*ast.Ident); ok {
					if i, ok := structs[id.Name]; ok {
						for j, el := range x.Elts {
							if kv, ok := el.(*ast.KeyValueExpr); ok {
								if k, ok := kv.Key.(*ast.Ident); ok && k.Name == field {
									out = append(out, reasons(fn, kv.Value)...)
								}
							} else if j == i {
								out = append(out, reasons(fn, el)...)
							}
						}
					}
				}
			}
			return true
		})
		return out
	}

	// The marker text is split so the guard script does not read this file as one.
	re := regexp.MustCompile(`// event-` + `reasons:(.*)`)
	markers := 0
	for _, f := range files {
		for _, cg := range f.Comments {
			for _, c := range cg.List {
				m := re.FindStringSubmatch(c.Text)
				if m == nil {
					continue
				}
				markers++
				pos := fset.Position(c.Pos())
				marked := strings.Fields(m[1])
				sort.Strings(marked)

				var call *ast.CallExpr
				var fn *ast.FuncDecl
				for _, d := range f.Decls {
					fd, ok := d.(*ast.FuncDecl)
					if !ok || fd.Body == nil {
						continue
					}
					ast.Inspect(fd.Body, func(n ast.Node) bool {
						ce, ok := n.(*ast.CallExpr)
						if ok && fset.Position(ce.Pos()).Line == pos.Line+1 && fset.Position(ce.Pos()).Filename == pos.Filename {
							call, fn = ce, fd
						}
						return true
					})
				}
				if call == nil || len(call.Args) < 3 || !strings.HasSuffix(text(call.Args[1]), "EventTypeWarning") {
					t.Errorf("%s: marker is not directly above a Recorder.Event(..., EventTypeWarning, reason, ...) call", pos)
					continue
				}
				seen := map[string]bool{}
				var got []string
				for _, r := range reasons(fn, call.Args[2]) {
					if r != "" && !seen[r] && !nonWarningReasons[r] {
						seen[r] = true
						got = append(got, r)
					}
				}
				sort.Strings(got)
				if strings.Join(got, " ") != strings.Join(marked, " ") {
					t.Errorf("%s: the call can emit %v but the marker names %v", pos, got, marked)
				}
			}
		}
	}
	if markers == 0 {
		t.Fatal("no event-reasons markers found")
	}
}

func types2str(fset *token.FileSet, e ast.Expr) string {
	var b bytes.Buffer
	_ = printer.Fprint(&b, fset, e)
	return b.String()
}
