package main

import (
	"path/filepath"
	"strings"
	"testing"
)

// A check that never fails is worth nothing, and the only way to know a check fails is
// to make it fail. These tests hold that proof so it survives the next change to this
// tool rather than depending on someone having run a manual probe once.

const module = "example.com/m"

func testRules() rules {
	return rules{
		Module: module,
		InternalEdges: map[string][]string{
			"id":    {},
			"event": {"id"},
			"db":    {},
		},
		PureSegments:        []string{"domain", "impure", "pure"},
		NoGoroutineSegments: []string{"domain", "app", "impure", "pure"},
		DeniedImports: map[string]deniedImport{
			"github.com/jackc/pgx": {Reason: "only db names the driver", Except: []string{"db"}},
		},
		ReservedDirNames: []string{"interface", "type"},
	}
}

func contains(findings []string, substr string) bool {
	for _, f := range findings {
		if strings.Contains(f, substr) {
			return true
		}
	}
	return false
}

func TestEdgeDeclaredIsAccepted(t *testing.T) {
	p := pkg{ImportPath: module + "/event", Imports: []string{module + "/id", "encoding/json"}}

	if findings := checkEdges(testRules(), p, "event"); len(findings) != 0 {
		t.Errorf("declared edge rejected: %v", findings)
	}
}

func TestUndeclaredEdgeIsRejected(t *testing.T) {
	p := pkg{ImportPath: module + "/id", Imports: []string{module + "/event"}}

	findings := checkEdges(testRules(), p, "id")
	if len(findings) != 1 {
		t.Fatalf("findings = %v, want exactly one", findings)
	}
	if !contains(findings, "not a declared edge") {
		t.Errorf("finding does not name the rule: %v", findings)
	}
}

// A package absent from the map may import no internal package. Defaulting to
// permissive would mean a new package silently starts unconstrained.
func TestPackageAbsentFromRulesMayNotImportInternally(t *testing.T) {
	p := pkg{ImportPath: module + "/rogue", Imports: []string{module + "/id"}}

	findings := checkEdges(testRules(), p, "rogue")
	if !contains(findings, "declares no internal edges") {
		t.Errorf("findings = %v, want a missing-declaration finding", findings)
	}
}

// A test that reaches across a boundary the production code may not cross has
// established the coupling the rule exists to prevent, and it will be copied.
func TestTestImportsAreCheckedToo(t *testing.T) {
	p := pkg{ImportPath: module + "/id", TestImports: []string{module + "/db"}}

	if findings := checkEdges(testRules(), p, "id"); len(findings) != 1 {
		t.Errorf("findings = %v, want the test import to be caught", findings)
	}
}

// An external test file lives in package foo_test, and go list reports its imports under
// XTestImports rather than TestImports. Reading only the latter left a hole an external
// test could drive anything through, and moving a file into package foo_test is a
// one-word change nobody would flag in review.
func TestExternalTestImportsAreCheckedToo(t *testing.T) {
	p := pkg{ImportPath: module + "/id", XTestImports: []string{module + "/db"}}

	if findings := checkEdges(testRules(), p, "id"); len(findings) != 1 {
		t.Errorf("findings = %v, want the external test import to be caught", findings)
	}
}

func TestExternalTestImportsAreSubjectToDenial(t *testing.T) {
	p := pkg{ImportPath: module + "/outbox", XTestImports: []string{"github.com/jackc/pgx/v5"}}

	if findings := checkDeniedImports(testRules(), p, "outbox"); len(findings) != 1 {
		t.Errorf("findings = %v, want the denied import to be caught in an external test", findings)
	}
}

func TestExternalAndSelfImportsAreIgnored(t *testing.T) {
	p := pkg{
		ImportPath: module + "/id",
		Imports:    []string{"crypto/rand", "github.com/other/lib", module + "/id"},
	}

	if findings := checkEdges(testRules(), p, "id"); len(findings) != 0 {
		t.Errorf("findings = %v, want none", findings)
	}
}

func TestDeniedImportIsRejected(t *testing.T) {
	p := pkg{ImportPath: module + "/outbox", Imports: []string{"github.com/jackc/pgx/v5"}}

	findings := checkDeniedImports(testRules(), p, "outbox")
	if len(findings) != 1 {
		t.Fatalf("findings = %v, want exactly one", findings)
	}
	if !contains(findings, "only db names the driver") {
		t.Errorf("finding omits the stated reason: %v", findings)
	}
}

// The exception list is what lets one package own the driver. Its shortness is the
// signal; a long list means the driver has leaked.
func TestDeniedImportHonoursTheExceptionList(t *testing.T) {
	p := pkg{ImportPath: module + "/db", Imports: []string{"github.com/jackc/pgx/v5/pgxpool"}}

	if findings := checkDeniedImports(testRules(), p, "db"); len(findings) != 0 {
		t.Errorf("findings = %v, want none for the excepted package", findings)
	}
}

func TestDeniedImportDoesNotMatchAPrefixOfAnUnrelatedPath(t *testing.T) {
	p := pkg{ImportPath: module + "/outbox", Imports: []string{"github.com/jackc/pgxsomething"}}

	if findings := checkDeniedImports(testRules(), p, "outbox"); len(findings) != 0 {
		t.Errorf("findings = %v, want none; the path only shares a prefix", findings)
	}
}

func TestReservedDirectoryNameIsRejected(t *testing.T) {
	p := pkg{ImportPath: module + "/identity/interface/http", Name: "http"}

	findings := checkNaming(testRules(), p, "identity/interface/http")
	if !contains(findings, "reserved directory name") {
		t.Errorf("findings = %v, want a reserved-name finding", findings)
	}
}

func TestUnderscorePackageNameIsRejected(t *testing.T) {
	p := pkg{ImportPath: module + "/membership", Name: "membership_service"}

	findings := checkNaming(testRules(), p, "membership")
	if !contains(findings, "underscore") {
		t.Errorf("findings = %v, want an underscore finding", findings)
	}
}

func TestConformingNamesProduceNoFinding(t *testing.T) {
	p := pkg{ImportPath: module + "/membership/adapter", Name: "adapter"}

	if findings := checkNaming(testRules(), p, "membership/adapter"); len(findings) != 0 {
		t.Errorf("findings = %v, want none", findings)
	}
}

// checkSource reads syntax rather than imports, so it is exercised against fixtures on
// disk. They live under testdata, which the go tool ignores, so they are parsed and
// never built.
func TestSourceRulesCatchContextAndGoroutine(t *testing.T) {
	dir := filepath.Join("testdata", "impure")
	p := pkg{ImportPath: module + "/impure", Dir: dir, GoFiles: []string{"impure.go"}}

	findings, err := checkSource(testRules(), p, "impure")
	if err != nil {
		t.Fatalf("checkSource: %v", err)
	}

	if !contains(findings, "accepts context.Context") {
		t.Errorf("findings = %v, want a context finding", findings)
	}
	if !contains(findings, "starts a goroutine") {
		t.Errorf("findings = %v, want a goroutine finding", findings)
	}

	// A function signature and a method signature are the same violation.
	var contexts int
	for _, f := range findings {
		if strings.Contains(f, "accepts context.Context") {
			contexts++
		}
	}
	if contexts != 2 {
		t.Errorf("context findings = %d, want 2 (one function, one method)", contexts)
	}

	// Every finding names a file and a line, or it cannot be acted on.
	for _, f := range findings {
		if !strings.Contains(f, "impure.go:") {
			t.Errorf("finding carries no file and line: %q", f)
		}
	}
}

func TestSourceRulesAcceptAConformingPackage(t *testing.T) {
	dir := filepath.Join("testdata", "pure")
	p := pkg{ImportPath: module + "/pure", Dir: dir, GoFiles: []string{"pure.go"}}

	findings, err := checkSource(testRules(), p, "pure")
	if err != nil {
		t.Fatalf("checkSource: %v", err)
	}
	if len(findings) != 0 {
		t.Errorf("findings = %v, want none for a conforming package", findings)
	}
}

// A package outside the configured segments is not subject to the syntax rules, so an
// adapter may take a context and may start a worker loop.
func TestSourceRulesSkipPackagesOutsideTheConfiguredSegments(t *testing.T) {
	dir := filepath.Join("testdata", "impure")
	p := pkg{ImportPath: module + "/adapter", Dir: dir, GoFiles: []string{"impure.go"}}

	findings, err := checkSource(testRules(), p, "adapter")
	if err != nil {
		t.Fatalf("checkSource: %v", err)
	}
	if len(findings) != 0 {
		t.Errorf("findings = %v, want none; adapter carries neither rule", findings)
	}
}

func TestRelativeStripsTheModulePath(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{module, "."},
		{module + "/event", "event"},
		{module + "/a/b/c", "a/b/c"},
	} {
		if got := relative(module, tc.in); got != tc.want {
			t.Errorf("relative(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestLoadRulesRejectsAFileWithoutAModule(t *testing.T) {
	if _, err := loadRules(filepath.Join("testdata", "does-not-exist.json")); err == nil {
		t.Error("loadRules accepted a missing file")
	}
}
