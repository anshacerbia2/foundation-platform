// Command schemacheck validates the temporary event registry and rejects incompatible
// schema evolution.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

type registry struct {
	Schemas []entry `json:"schemas"`
}

type entry struct {
	EventType string   `json:"event_type"`
	Major     int      `json:"major"`
	Versions  []string `json:"versions"`
}

type schema struct {
	Schema     string                     `json:"$schema"`
	ID         string                     `json:"$id"`
	Type       string                     `json:"type"`
	Properties map[string]json.RawMessage `json:"properties"`
	Required   []string                   `json:"required"`
}

func main() {
	root := flag.String("root", "contracts/events", "event registry directory")
	flag.Parse()
	if err := check(*root); err != nil {
		fmt.Fprintf(os.Stderr, "schemacheck: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("schemacheck: event registry is compatible")
}

func check(root string) error {
	registered, err := readRegistry(filepath.Join(root, "registry.json"))
	if err != nil {
		return err
	}
	seen := make(map[string]bool)
	for _, contract := range registered.Schemas {
		key := fmt.Sprintf("%s@v%d", contract.EventType, contract.Major)
		if strings.TrimSpace(contract.EventType) == "" || contract.Major < 1 {
			return fmt.Errorf("registry entry %q has no event type or positive major", key)
		}
		if seen[key] {
			return fmt.Errorf("registry contains duplicate %s", key)
		}
		seen[key] = true
		if len(contract.Versions) == 0 {
			return fmt.Errorf("%s has no schema versions", key)
		}
		var previous schema
		seenVersions := make(map[string]bool)
		for i, name := range contract.Versions {
			if seenVersions[name] {
				return fmt.Errorf("%s contains duplicate schema version %q", key, name)
			}
			seenVersions[name] = true
			if filepath.IsAbs(name) || strings.Contains(filepath.Clean(name), "..") {
				return fmt.Errorf("%s contains unsafe schema path %q", key, name)
			}
			current, err := readSchema(filepath.Join(root, name))
			if err != nil {
				return fmt.Errorf("%s version %q: %w", key, name, err)
			}
			if i > 0 {
				if err := compatible(previous, current); err != nil {
					return fmt.Errorf("%s version %q is incompatible: %w", key, name, err)
				}
			}
			previous = current
		}
	}
	return nil
}

func readRegistry(name string) (registry, error) {
	var value registry
	if err := decodeStrict(name, &value); err != nil {
		return registry{}, err
	}
	return value, nil
}

func readSchema(name string) (schema, error) {
	var value schema
	if err := decode(name, &value, false); err != nil {
		return schema{}, err
	}
	if value.Schema == "" || value.ID == "" {
		return schema{}, errors.New("$schema and $id are required")
	}
	if value.Type != "object" || value.Properties == nil {
		return schema{}, errors.New("root must be an object with properties")
	}
	for _, required := range value.Required {
		if _, ok := value.Properties[required]; !ok {
			return schema{}, fmt.Errorf("required property %q is not declared", required)
		}
	}
	return value, nil
}

func decodeStrict(name string, target any) error {
	return decode(name, target, true)
}

func decode(name string, target any, strict bool) error {
	content, err := os.ReadFile(name)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(content))
	if strict {
		decoder.DisallowUnknownFields()
	}
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decoding %s: %w", name, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%s contains trailing JSON data", name)
	}
	return nil
}

func compatible(previous, current schema) error {
	for name, oldProperty := range previous.Properties {
		newProperty, ok := current.Properties[name]
		if !ok {
			return fmt.Errorf("property %q was removed", name)
		}
		if err := compatibleProperty(name, oldProperty, newProperty); err != nil {
			return err
		}
	}
	for _, required := range current.Required {
		if !slices.Contains(previous.Required, required) {
			return fmt.Errorf("property %q became required", required)
		}
	}
	return nil
}

type property struct {
	Type       string                     `json:"type"`
	Ref        string                     `json:"$ref"`
	Properties map[string]json.RawMessage `json:"properties"`
	Required   []string                   `json:"required"`
	Items      json.RawMessage            `json:"items"`
}

func compatibleProperty(path string, oldRaw, newRaw json.RawMessage) error {
	var oldProperty, newProperty property
	if err := json.Unmarshal(oldRaw, &oldProperty); err != nil {
		return fmt.Errorf("old property %q: %w", path, err)
	}
	if err := json.Unmarshal(newRaw, &newProperty); err != nil {
		return fmt.Errorf("new property %q: %w", path, err)
	}
	oldType := oldProperty.Type
	newType := newProperty.Type
	if oldProperty.Ref != "" {
		oldType = "$ref:" + oldProperty.Ref
	}
	if newProperty.Ref != "" {
		newType = "$ref:" + newProperty.Ref
	}
	if oldType == "" || newType == "" {
		return fmt.Errorf("property %q requires type or $ref", path)
	}
	if oldType != newType {
		return fmt.Errorf("property %q changed type from %s to %s", path, oldType, newType)
	}

	if oldProperty.Type == "object" {
		for name, oldChild := range oldProperty.Properties {
			newChild, ok := newProperty.Properties[name]
			if !ok {
				return fmt.Errorf("property %q was removed", path+"."+name)
			}
			if err := compatibleProperty(path+"."+name, oldChild, newChild); err != nil {
				return err
			}
		}
		for _, required := range newProperty.Required {
			if !slices.Contains(oldProperty.Required, required) {
				return fmt.Errorf("property %q became required", path+"."+required)
			}
		}
	}
	if oldProperty.Type == "array" && len(oldProperty.Items) > 0 {
		if len(newProperty.Items) == 0 {
			return fmt.Errorf("property %q removed its item contract", path)
		}
		return compatibleProperty(path+"[]", oldProperty.Items, newProperty.Items)
	}
	return nil
}
