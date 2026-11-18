package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
)

func TestBuildLLMReportSchemaAndCheckedInArtifact(t *testing.T) {
	schema := buildLLMReportSchema()

	if got, ok := schema["additionalProperties"].(bool); !ok || got {
		t.Fatalf("expected top-level additionalProperties=false, got %#v", schema["additionalProperties"])
	}

	req, _ := schema["required"].([]string)
	if !slices.Contains(req, "summary") || !slices.Contains(req, "findings") || slices.Contains(req, "provenance") {
		t.Fatalf("unexpected required keys: %#v", req)
	}

	props := schema["properties"].(JSONSchema)
	findingsSchema := props["findings"].(JSONSchema)
	findingProps := findingsSchema["items"].(JSONSchema)["properties"].(JSONSchema)

	conf := findingProps["confidence"].(JSONSchema)
	if !slices.Equal(conf["type"].([]string), []string{"number", "null"}) {
		t.Fatalf("unexpected confidence type: %#v", conf["type"])
	}
	if conf["minimum"] != 0.0 || conf["maximum"] != 1.0 {
		t.Fatalf("unexpected confidence bounds: %#v", conf)
	}

	line := findingProps["line"].(JSONSchema)
	if _, hasMin := line["minimum"]; hasMin {
		t.Fatalf("line should not carry float bounds: %#v", line)
	}

	sevChoices := findingProps["severity"].(JSONSchema)["enum"].([]string)
	if slices.Contains(sevChoices, "SEVERITY_UNSPECIFIED") || !slices.Contains(sevChoices, "SEVERITY_HIGH") {
		t.Fatalf("unexpected severity enum list: %#v", sevChoices)
	}

	artifactPath := filepath.Join("..", "..", "gen", "schema", "llm_review_report.schema.json")
	onDisk, err := os.ReadFile(artifactPath)
	if err != nil {
		t.Fatalf("read checked-in schema: %v", err)
	}
	generated, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		t.Fatalf("marshal schema: %v", err)
	}
	generated = append(generated, '\n')
	if string(onDisk) != string(generated) {
		t.Fatalf("checked-in schema %s is out of date; run ./scripts/gen-proto.sh", artifactPath)
	}
}

func TestSchemaRejectsInt64Field(t *testing.T) {
	md := (&reviewv1.TaskBudget{}).ProtoReflect().Descriptor()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on Int64Kind field in TaskBudget schema")
		}
	}()
	objectSchemaFromMessage(md, nil)
}
