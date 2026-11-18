package sandbox

import (
	"archive/tar"
	"bytes"
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
	"github.com/zetxqx/review-agent-infra/pkg/contract"
	"google.golang.org/protobuf/encoding/protojson"
)

const maxReportReadBytes = 2 << 20

// inputs key: file path relative to /workspace/.review/in; value: raw file bytes.
func buildInputsTar(inputs map[string][]byte) ([]byte, error) {
	keys := make([]string, 0, len(inputs))
	for k := range inputs {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	now := time.Unix(0, 0)
	for _, k := range keys {
		cleaned := path.Clean(strings.TrimLeft(k, "/"))
		if cleaned == "" || cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
			return nil, fmt.Errorf("unsafe input tar path %q", k)
		}
		data := inputs[k]
		hdr := &tar.Header{
			Name:     cleaned,
			Mode:     0o644,
			Size:     int64(len(data)),
			ModTime:  now,
			Typeflag: tar.TypeReg,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return nil, err
		}
		if _, err := tw.Write(data); err != nil {
			return nil, err
		}
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// inputs key: file path relative to /workspace/.review/in; value: raw file bytes.
// Return map key: file path relative to /workspace/.review/in; value: raw file bytes with updated task paths.
func rewriteTaskPaths(inputs map[string][]byte, srcDir, inDir, outDir string) (map[string][]byte, error) {
	// key: file path relative to /workspace/.review/in; value: raw file bytes
	cloned := make(map[string][]byte, len(inputs))
	for k, v := range inputs {
		cloned[k] = v
	}
	rawTask, ok := cloned["task.json"]
	if !ok {
		return nil, errors.New("SandboxSpec.Inputs missing task.json")
	}
	var task reviewv1.AgentTask
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(rawTask, &task); err != nil {
		return nil, fmt.Errorf("decode task.json: %w", err)
	}
	task.SrcDir = srcDir
	task.DiffFile = filepath.Join(inDir, "diff.patch")
	if _, hasRulesDir := lookupRulesPrefix(cloned); hasRulesDir {
		task.RulesDir = filepath.Join(inDir, "rules")
	} else if _, hasRulesMD := cloned["rules.md"]; hasRulesMD {
		task.RulesDir = filepath.Join(inDir, "rules.md")
	} else {
		task.RulesDir = filepath.Join(inDir, "rules")
	}
	task.OutDir = outDir

	encoded, err := (protojson.MarshalOptions{Indent: "  "}).Marshal(&task)
	if err != nil {
		return nil, fmt.Errorf("encode task.json: %w", err)
	}
	cloned["task.json"] = append(encoded, '\n')
	return cloned, nil
}

// inputs key: file path relative to /workspace/.review/in; value: raw file bytes.
func lookupRulesPrefix(inputs map[string][]byte) (string, bool) {
	for k := range inputs {
		cleaned := path.Clean(strings.TrimLeft(k, "/"))
		if strings.HasPrefix(cleaned, "rules/") {
			return cleaned, true
		}
	}
	return "", false
}

type cappedBuffer struct {
	buf bytes.Buffer
	max int
}

func (c *cappedBuffer) Write(p []byte) (int, error) {
	if c.buf.Len()+len(p) > c.max {
		return 0, fmt.Errorf("sandbox report output exceeds %d byte cap", c.max)
	}
	return c.buf.Write(p)
}

func (c *cappedBuffer) Bytes() []byte {
	return c.buf.Bytes()
}

func decodeAndValidateReport(raw []byte) (*reviewv1.ReviewReport, error) {
	var report reviewv1.ReviewReport
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, &report); err != nil {
		return nil, fmt.Errorf("decode ReviewReport protojson: %w", err)
	}
	contract.ValidateReport(&report)
	return &report, nil
}
