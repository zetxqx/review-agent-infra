package publisher

import "strings"

func TruncateBody(s string, max int) string {
	if max <= 0 {
		return ""
	}
	if len(s) < max {
		return s
	}
	suffix := "..."
	limit := max
	if limit > len(suffix) {
		limit -= len(suffix)
	}
	cut := s[:limit]
	return cut + suffix
}

func FormatNotice(msg string) string {
	if msg == "" {
		return ""
	}
	if strings.Contains(msg, "\r") {
		msg = strings.ReplaceAll(msg, "\r", "")
	}
	return strings.TrimSpace(msg)
}
