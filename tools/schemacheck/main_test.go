package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func write(t *testing.T, root, name, content string) {
	t.Helper()
	full := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func registryFor(versions ...string) string {
	quoted := make([]string, len(versions))
	for i, version := range versions {
		quoted[i] = `"` + version + `"`
	}
	return `{"schemas":[{"event_type":"com.scnehaux.test.record.lifecycle.created","major":1,"versions":[` + strings.Join(quoted, ",") + `]}]}`
}

func objectSchema(required, properties string) string {
	return `{"$schema":"https://json-schema.org/draft/2020-12/schema","$id":"https://schemas.example/test","type":"object","properties":{` + properties + `},"required":[` + required + `]}`
}

func TestCompatibleEvolutionPasses(t *testing.T) {
	root := t.TempDir()
	write(t, root, "registry.json", registryFor("v1/1.0.json", "v1/1.1.json"))
	write(t, root, "v1/1.0.json", objectSchema(`"id"`, `"id":{"type":"string"}`))
	write(t, root, "v1/1.1.json", objectSchema(`"id"`, `"id":{"type":"string"},"note":{"type":"string"}`))
	if err := check(root); err != nil {
		t.Fatalf("check: %v", err)
	}
}

func TestRemovedPropertyFails(t *testing.T) {
	root := t.TempDir()
	write(t, root, "registry.json", registryFor("v1/1.0.json", "v1/1.1.json"))
	write(t, root, "v1/1.0.json", objectSchema(``, `"id":{"type":"string"}`))
	write(t, root, "v1/1.1.json", objectSchema(``, `"note":{"type":"string"}`))
	if err := check(root); err == nil || !strings.Contains(err.Error(), "removed") {
		t.Fatalf("check error = %v", err)
	}
}

func TestChangedTypeFails(t *testing.T) {
	root := t.TempDir()
	write(t, root, "registry.json", registryFor("v1/1.0.json", "v1/1.1.json"))
	write(t, root, "v1/1.0.json", objectSchema(``, `"id":{"type":"string"}`))
	write(t, root, "v1/1.1.json", objectSchema(``, `"id":{"type":"integer"}`))
	if err := check(root); err == nil || !strings.Contains(err.Error(), "changed type") {
		t.Fatalf("check error = %v", err)
	}
}

func TestNewRequiredPropertyFails(t *testing.T) {
	root := t.TempDir()
	write(t, root, "registry.json", registryFor("v1/1.0.json", "v1/1.1.json"))
	write(t, root, "v1/1.0.json", objectSchema(``, `"id":{"type":"string"}`))
	write(t, root, "v1/1.1.json", objectSchema(`"id"`, `"id":{"type":"string"}`))
	if err := check(root); err == nil || !strings.Contains(err.Error(), "became required") {
		t.Fatalf("check error = %v", err)
	}
}

func TestSemanticVersionOrderComesFromTheRegistry(t *testing.T) {
	root := t.TempDir()
	write(t, root, "registry.json", registryFor("v1/1.9.json", "v1/1.10.json"))
	write(t, root, "v1/1.9.json", objectSchema(``, `"id":{"type":"string"}`))
	write(t, root, "v1/1.10.json", objectSchema(``, `"id":{"type":"string"},"note":{"type":"string"}`))
	if err := check(root); err != nil {
		t.Fatalf("check semantic order: %v", err)
	}
}

func TestDuplicateSchemaVersionFails(t *testing.T) {
	root := t.TempDir()
	write(t, root, "registry.json", registryFor("v1/1.0.json", "v1/1.0.json"))
	write(t, root, "v1/1.0.json", objectSchema(``, `"id":{"type":"string"}`))
	if err := check(root); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("check error = %v", err)
	}
}
