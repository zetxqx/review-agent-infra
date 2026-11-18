package main

import (
	"encoding/json"
	"fmt"
	"os"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// key: JSON Schema keyword or object property name; value: schema value
type JSONSchema map[string]any

func buildLLMReportSchema() JSONSchema {
	md := (&reviewv1.ReviewReport{}).ProtoReflect().Descriptor()
	// key: proto field name excluded from LLM output schema; value: true when excluded
	skip := map[string]bool{"provenance": true}
	return objectSchemaFromMessage(md, skip)
}

// skip key: proto field name excluded from generated schema; value: true when excluded.
func objectSchemaFromMessage(md protoreflect.MessageDescriptor, skip map[string]bool) JSONSchema {
	// key: JSON property name; value: JSON Schema definition for that field
	props := JSONSchema{}
	req := make([]string, 0, md.Fields().Len())
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if skip[string(fd.Name())] {
			continue
		}
		name := fd.JSONName()
		req = append(req, name)
		props[name] = schemaForField(fd)
	}
	return JSONSchema{
		"type":                 "object",
		"additionalProperties": false,
		"required":             req,
		"properties":           props,
	}
}

func schemaForField(fd protoreflect.FieldDescriptor) JSONSchema {
	if fd.IsList() {
		itemFD := schemaForSingularField(fd)
		s := JSONSchema{"type": "array", "items": itemFD}
		if fd.Name() == "findings" {
			s["maxItems"] = 100
		}
		return s
	}
	s := schemaForSingularField(fd)
	if fd.HasOptionalKeyword() {
		if t, ok := s["type"].(string); ok {
			s["type"] = []string{t, "null"}
		}
	}
	return s
}

func schemaForSingularField(fd protoreflect.FieldDescriptor) JSONSchema {
	switch fd.Kind() {
	case protoreflect.StringKind:
		return JSONSchema{"type": "string"}
	case protoreflect.Int32Kind:
		return JSONSchema{"type": "integer"}
	case protoreflect.Int64Kind:
		panic(fmt.Errorf("int64 field %q is incompatible with JSON Schema integer (protojson encodes int64 as string)", fd.FullName()))
	case protoreflect.DoubleKind, protoreflect.FloatKind:
		s := JSONSchema{"type": "number"}
		if fd.Name() == "confidence" {
			s["minimum"] = 0.0
			s["maximum"] = 1.0
		}
		return s
	case protoreflect.BoolKind:
		return JSONSchema{"type": "boolean"}
	case protoreflect.EnumKind:
		vals := fd.Enum().Values()
		choices := make([]string, 0, vals.Len())
		for i := 0; i < vals.Len(); i++ {
			if vals.Get(i).Number() != 0 {
				choices = append(choices, string(vals.Get(i).Name()))
			}
		}
		return JSONSchema{"type": "string", "enum": choices}
	case protoreflect.MessageKind:
		return objectSchemaFromMessage(fd.Message(), nil)
	default:
		return JSONSchema{}
	}
}

func main() {
	out, err := json.MarshalIndent(buildLLMReportSchema(), "", "  ")
	if err != nil {
		panic(err)
	}
	if len(os.Args) > 1 {
		if err := os.WriteFile(os.Args[1], append(out, '\n'), 0o644); err != nil {
			panic(err)
		}
		return
	}
	fmt.Println(string(out))
}
