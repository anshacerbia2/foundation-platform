// Command archcheck asserts the boundaries STD-GLB-BE-001 declares.
//
// It reads the package graph rather than matching text, so an import introduced
// through an alias, a blank identifier, or a test file is detected on the same basis
// as a direct one. Rules live in arch.json beside the module root, so the ruleset is
// reviewable in the same diff as the code it constrains.
//
// It uses the standard library only, so it runs in any environment that can build the
// module it checks.
package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strings"
)

// rules is the reviewable ruleset. A rule absent here is not enforced, which is why
// the file lives beside the code rather than inside this program.
type rules struct {
	// Module is the module path, used to separate internal imports from external ones.
	Module string `json:"module"`

	// InternalEdges declares, per package, the internal packages it may import. A
	// package absent from this map may import no internal package. An edge not
	// declared is an error, so accidental coupling fails the build rather than
	// accumulating.
	InternalEdges map[string][]string `json:"internalEdges"`

	// PureSegments names directory segments whose packages perform no input or
	// output. A package whose final path segment matches carries the purity rules of
	// STD-GLB-BE-001 rule 4.
	PureSegments []string `json:"pureSegments"`

	// NoGoroutineSegments names directory segments forbidden from starting a
	// goroutine, per rule 7. A loop belongs to a driving adapter.
	NoGoroutineSegments []string `json:"noGoroutineSegments"`

	// DeniedImports names import paths that are denied everywhere except to the
	// packages listed in Except. It is how a driver stays named in one place: the
	// exception list is short, reviewable, and fails the build when it grows.
	DeniedImports map[string]deniedImport `json:"deniedImports"`

	// ReservedDirNames are directory names rejected by rule 12.
	ReservedDirNames []string `json:"reservedDirNames"`
}

// deniedImport is a denial with a stated reason and an explicit exception list. A
// denial without a reason becomes folklore, and a reason recorded here is read at the
// moment someone tries to add the import.
type deniedImport struct {
	Reason string   `json:"reason"`
	Except []string `json:"except"`
}

// pkg mirrors the subset of `go list -json` this program reads.
//
// XTestImports is read alongside TestImports. The two are separate lists: an internal
// test file belongs to the package, an external one belongs to package foo_test, and go
// list reports their imports under different keys. Reading only the first left a hole
// through which an external test could import anything at all, which is exactly where a
// boundary erodes first because a test is where a shortcut feels harmless.
type pkg struct {
	ImportPath   string
	Dir          string
	Name         string
	GoFiles      []string
	TestGoFiles  []string
	XTestGoFiles []string
	Imports      []string
	TestImports  []string
	XTestImports []string
}

// allImports returns every import the package pulls in, from production, internal test,
// and external test files alike.
func (p pkg) allImports() []string {
	return slices.Concat(p.Imports, p.TestImports, p.XTestImports)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "archcheck: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	r, err := loadRules("arch.json")
	if err != nil {
		return err
	}

	packages, err := listPackages()
	if err != nil {
		return err
	}

	var findings []string
	for _, p := range packages {
		rel := relative(r.Module, p.ImportPath)
		if rel == "" || strings.HasPrefix(rel, "tools/") {
			continue
		}
		findings = append(findings, checkEdges(r, p, rel)...)
		findings = append(findings, checkDeniedImports(r, p, rel)...)
		findings = append(findings, checkNaming(r, p, rel)...)
		f, err := checkSource(r, p, rel)
		if err != nil {
			return err
		}
		findings = append(findings, f...)
	}

	if len(findings) > 0 {
		sort.Strings(findings)
		fmt.Fprintf(os.Stderr, "archcheck: %d boundary violation(s)\n\n", len(findings))
		for _, f := range findings {
			fmt.Fprintf(os.Stderr, "  %s\n", f)
		}
		fmt.Fprintln(os.Stderr)
		return fmt.Errorf("boundaries declared in arch.json are not satisfied")
	}

	fmt.Printf("archcheck: %d packages, all declared boundaries satisfied\n", len(packages))
	return nil
}

func loadRules(name string) (rules, error) {
	b, err := os.ReadFile(name)
	if err != nil {
		return rules{}, fmt.Errorf("reading %s: %w", name, err)
	}
	var r rules
	if err := json.Unmarshal(b, &r); err != nil {
		return rules{}, fmt.Errorf("parsing %s: %w", name, err)
	}
	if r.Module == "" {
		return rules{}, fmt.Errorf("%s declares no module path", name)
	}
	return r, nil
}

// listPackages reads the package graph from the toolchain rather than from the file
// system, so build tags, generated files, and test files are resolved the same way the
// compiler resolves them.
func listPackages() ([]pkg, error) {
	out, err := exec.Command("go", "list", "-json", "./...").Output()
	if err != nil {
		return nil, fmt.Errorf("go list: %w", err)
	}

	var packages []pkg
	dec := json.NewDecoder(strings.NewReader(string(out)))
	for dec.More() {
		var p pkg
		if err := dec.Decode(&p); err != nil {
			return nil, fmt.Errorf("decoding go list output: %w", err)
		}
		packages = append(packages, p)
	}
	return packages, nil
}

func relative(module, importPath string) string {
	if importPath == module {
		return "."
	}
	return strings.TrimPrefix(strings.TrimPrefix(importPath, module), "/")
}

// checkEdges asserts rule 3. Test imports are included deliberately: a test that
// reaches across a boundary the production code may not cross has established the
// coupling the rule exists to prevent, and it will be copied.
func checkEdges(r rules, p pkg, rel string) []string {
	allowed, declared := r.InternalEdges[rel]

	var findings []string
	seen := map[string]bool{}

	for _, imp := range p.allImports() {
		if !strings.HasPrefix(imp, r.Module) {
			continue
		}
		target := relative(r.Module, imp)
		if target == rel || seen[target] {
			continue
		}
		seen[target] = true

		if !declared {
			findings = append(findings, fmt.Sprintf(
				"%s imports %s, but %s declares no internal edges in arch.json", rel, target, rel))
			continue
		}
		if !slices.Contains(allowed, target) {
			findings = append(findings, fmt.Sprintf(
				"%s imports %s, which is not a declared edge of %s", rel, target, rel))
		}
	}
	return findings
}

func checkDeniedImports(r rules, p pkg, rel string) []string {
	var findings []string
	for _, imp := range p.allImports() {
		for denied, rule := range r.DeniedImports {
			if imp != denied && !strings.HasPrefix(imp, denied+"/") {
				continue
			}
			if slices.Contains(rule.Except, rel) {
				continue
			}
			findings = append(findings, fmt.Sprintf("%s imports %s: %s", rel, imp, rule.Reason))
		}
	}
	return findings
}

func checkNaming(r rules, p pkg, rel string) []string {
	var findings []string
	for _, seg := range strings.Split(rel, "/") {
		if slices.Contains(r.ReservedDirNames, seg) {
			findings = append(findings, fmt.Sprintf(
				"%s uses reserved directory name %q; rule 12 prohibits it", rel, seg))
		}
	}
	if strings.Contains(p.Name, "_") {
		findings = append(findings, fmt.Sprintf(
			"package %s in %s contains an underscore; rule 12 prohibits it", p.Name, rel))
	}
	return findings
}

// checkSource asserts the rules that read syntax rather than imports: no
// context.Context in a pure package, and no goroutine started outside a driving
// adapter.
func checkSource(r rules, p pkg, rel string) ([]string, error) {
	pure := hasFinalSegment(rel, r.PureSegments)
	noGoroutine := hasFinalSegment(rel, r.NoGoroutineSegments)
	if !pure && !noGoroutine {
		return nil, nil
	}

	var findings []string
	fset := token.NewFileSet()

	for _, name := range p.GoFiles {
		full := filepath.Join(p.Dir, name)
		file, err := parser.ParseFile(fset, full, nil, 0)
		if err != nil {
			return nil, fmt.Errorf("parsing %s: %w", full, err)
		}

		ast.Inspect(file, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.FuncType:
				if pure && signatureTakesContext(node) {
					findings = append(findings, fmt.Sprintf(
						"%s: a function accepts context.Context; rule 4 reserves that for app/, because a context signals input or output",
						positionOf(fset, node.Pos(), rel, name)))
				}
			case *ast.GoStmt:
				if noGoroutine {
					findings = append(findings, fmt.Sprintf(
						"%s: starts a goroutine; rule 7 places worker loops in a driving adapter",
						positionOf(fset, node.Pos(), rel, name)))
				}
			}
			return true
		})
	}
	return findings, nil
}

func signatureTakesContext(ft *ast.FuncType) bool {
	if ft.Params == nil {
		return false
	}
	for _, field := range ft.Params.List {
		sel, ok := field.Type.(*ast.SelectorExpr)
		if !ok {
			continue
		}
		ident, ok := sel.X.(*ast.Ident)
		if !ok {
			continue
		}
		if ident.Name == "context" && sel.Sel.Name == "Context" {
			return true
		}
	}
	return false
}

func hasFinalSegment(rel string, segments []string) bool {
	return slices.Contains(segments, path.Base(rel))
}

func positionOf(fset *token.FileSet, pos token.Pos, rel, file string) string {
	p := fset.Position(pos)
	return fmt.Sprintf("%s/%s:%d", rel, file, p.Line)
}
