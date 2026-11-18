package publisher

import (
	"regexp"
	"strings"
)

const RedactedPlaceholder = "[redacted]"

var (
	reCodeFence    = regexp.MustCompile("(?s)(```.*?```|~~~.*?~~~)")
	reJWTToken     = regexp.MustCompile(`eyJ[A-Za-z0-9_-]{6,}(?:\.[A-Za-z0-9_-]+)*`)
	reHTML         = regexp.MustCompile(`(?s)<!--.*?-->|</?[A-Za-z][^>\n]*>`)
	reImage        = regexp.MustCompile(`!\[([^\]]*)\]\([^)]*\)`)
	reRefImage     = regexp.MustCompile(`!\[([^\]]*)\]\[[^\]]*\]`)
	reExternalLink = regexp.MustCompile(`(?i)\[([^\]]+)\]\(\s*(?:https?:|javascript:|data:|//)[^)]*\)`)
	reSlashCmd     = regexp.MustCompile(`(?m)^([ \t>]*)/([A-Za-z])`)
	reMention      = regexp.MustCompile(`@([A-Za-z0-9_-])`)
)

func SanitizeMarkdown(text string, mintedTokens ...string) string {
	for _, tok := range mintedTokens {
		if tok != "" {
			text = strings.ReplaceAll(text, tok, RedactedPlaceholder)
		}
	}
	text = reJWTToken.ReplaceAllString(text, RedactedPlaceholder)

	spans := reCodeFence.FindAllStringIndex(text, -1)
	if len(spans) == 0 {
		return scrubProse(text)
	}

	var b strings.Builder
	pos := 0
	for _, span := range spans {
		b.WriteString(scrubProse(text[pos:span[0]]))
		fenced := text[span[0]:span[1]]
		b.WriteString(reSlashCmd.ReplaceAllString(fenced, "$1\u200b/$2"))
		pos = span[1]
	}
	b.WriteString(scrubProse(text[pos:]))
	return b.String()
}

func scrubProse(s string) string {
	s = reHTML.ReplaceAllString(s, "")
	s = reImage.ReplaceAllString(s, "$1")
	s = reRefImage.ReplaceAllString(s, "$1")
	s = reExternalLink.ReplaceAllString(s, "$1")
	s = reSlashCmd.ReplaceAllString(s, `$1\/$2`)
	s = reMention.ReplaceAllString(s, "@\u200b$1")
	return s
}
