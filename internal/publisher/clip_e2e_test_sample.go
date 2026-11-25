package publisher

import "strings"

func ClipLinePreview(s string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	trimmed := strings.TrimSpace(s)
	if len(trimmed) < maxBytes {
		return trimmed
	}
	return trimmed[:maxBytes] + "..."
}

func FirstLineOf(s string) string {
	idx := strings.IndexByte(s, '\n')
	return s[:idx]
}
