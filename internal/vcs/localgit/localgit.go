package localgit

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strings"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
	"github.com/zetxqx/review-agent-infra/pkg/contract"
)

const (
	emptyTreeSHA     = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"
	maxRuleFileBytes = 64 << 10
)

var _ contract.VCSPlugin = (*Plugin)(nil)

type CommitSpec struct {
	RepoDir string
	RepoID  string
	BaseSHA string
	HeadSHA string
	Title   string
	Body    string
}

type Plugin struct {
	DefaultRepoDir string
	Published      []contract.PublishRequest
}

func New(defaultRepoDir string) *Plugin {
	return &Plugin{DefaultRepoDir: defaultRepoDir}
}

func ResolveRefs(ctx context.Context, repoDir, commitRef, baseRef, headRef string) (CommitSpec, error) {
	absRepo, err := filepath.Abs(repoDir)
	if err != nil {
		return CommitSpec{}, err
	}
	topDir, err := gitOutput(ctx, absRepo, "rev-parse", "--show-toplevel")
	if err != nil {
		return CommitSpec{}, fmt.Errorf("resolve git repository %q: %w", absRepo, err)
	}

	var baseSHA, headSHA string
	if commitRef != "" {
		headSHA, err = gitOutput(ctx, topDir, "rev-parse", "--verify", commitRef+"^{commit}")
		if err != nil {
			return CommitSpec{}, fmt.Errorf("resolve commit %q: %w", commitRef, err)
		}
		parentSHA, pErr := gitOutput(ctx, topDir, "rev-parse", "--verify", headSHA+"^1^{commit}")
		if pErr != nil {
			baseSHA = emptyTreeSHA
		} else {
			baseSHA = parentSHA
		}
	} else {
		if baseRef == "" {
			baseRef = "HEAD~1"
		}
		if headRef == "" {
			headRef = "HEAD"
		}
		headSHA, err = gitOutput(ctx, topDir, "rev-parse", "--verify", headRef+"^{commit}")
		if err != nil {
			return CommitSpec{}, fmt.Errorf("resolve head ref %q: %w", headRef, err)
		}
		rawBase, bErr := gitOutput(ctx, topDir, "rev-parse", "--verify", baseRef+"^{commit}")
		if bErr != nil {
			return CommitSpec{}, fmt.Errorf("resolve base ref %q: %w", baseRef, err)
		}
		if mb, mbErr := gitOutput(ctx, topDir, "merge-base", rawBase, headSHA); mbErr == nil && mb != "" {
			baseSHA = mb
		} else {
			baseSHA = rawBase
		}
	}

	title, _ := gitOutput(ctx, topDir, "log", "-1", "--format=%s", headSHA)
	body, _ := gitOutput(ctx, topDir, "log", "-1", "--format=%b", headSHA)

	return CommitSpec{
		RepoDir: topDir,
		RepoID:  filepath.Base(topDir),
		BaseSHA: baseSHA,
		HeadSHA: headSHA,
		Title:   title,
		Body:    body,
	}, nil
}

func (p *Plugin) VerifyAndParseWebhook(_ context.Context, _ http.Header, _ []byte) (*reviewv1.ReviewEvent, error) {
	return nil, errors.New("webhook parsing is not supported by localgit VCSPlugin")
}

func (p *Plugin) GetHeadSHA(ctx context.Context, repo string, _ int) (string, error) {
	dir, err := p.resolveRepoDir(repo)
	if err != nil {
		return "", err
	}
	return gitOutput(ctx, dir, "rev-parse", "--verify", "HEAD^{commit}")
}

func (p *Plugin) FetchDiffAndTarball(ctx context.Context, repo string, _ int, baseSHA, headSHA string) ([]byte, io.ReadCloser, error) {
	dir, err := p.resolveRepoDir(repo)
	if err != nil {
		return nil, nil, err
	}
	if headSHA == "" {
		return nil, nil, errors.New("headSHA is required")
	}
	if baseSHA == "" {
		baseSHA = emptyTreeSHA
	}

	var rawDiff []byte
	if baseSHA == emptyTreeSHA {
		rawDiff, err = gitBytes(ctx, dir, "diff", "--no-renames", "--no-ext-diff", "--no-color", emptyTreeSHA, headSHA)
	} else {
		rawDiff, err = gitBytes(ctx, dir, "diff", "--no-renames", "--no-ext-diff", "--no-color", baseSHA+"..."+headSHA)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("git diff %s..%s: %w", baseSHA, headSHA, err)
	}
	cleanDiff := stripBinaryDiffSections(rawDiff)

	archiveBytes, err := gitBytes(ctx, dir, "archive", "--format=tar.gz", "--prefix=repo/", headSHA)
	if err != nil {
		return nil, nil, fmt.Errorf("git archive %s: %w", headSHA, err)
	}
	return cleanDiff, io.NopCloser(bytes.NewReader(archiveBytes)), nil
}

// Return map key: repository-relative rule file path; value: raw UTF-8 rule bytes.
func (p *Plugin) FetchBaseRules(ctx context.Context, repo, baseSHA string, changedFiles []string) (map[string][]byte, error) {
	dir, err := p.resolveRepoDir(repo)
	if err != nil {
		return nil, err
	}
	// key: repository-relative rule file path; value: raw UTF-8 rule bytes
	rules := make(map[string][]byte)
	if baseSHA == "" || baseSHA == emptyTreeSHA {
		return rules, nil
	}

	treeOut, err := gitOutput(ctx, dir, "ls-tree", "-r", "--name-only", baseSHA)
	if err != nil || strings.TrimSpace(treeOut) == "" {
		return rules, nil
	}

	// key: ancestor directory path of changed files; value: true when touched by diff
	touchedDirs := make(map[string]bool)
	touchedDirs["."] = true
	for _, cf := range changedFiles {
		cleaned := path.Clean(strings.TrimLeft(cf, "/"))
		for d := path.Dir(cleaned); d != "." && d != "/"; d = path.Dir(d) {
			touchedDirs[d] = true
		}
	}

	entries := strings.Split(treeOut, "\n")
	slices.Sort(entries)
	for _, entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		if !isBaseRuleCandidate(entry, touchedDirs) {
			continue
		}
		content, readErr := gitBytes(ctx, dir, "show", baseSHA+":"+entry)
		if readErr != nil {
			continue
		}
		if len(content) > maxRuleFileBytes {
			content = content[:maxRuleFileBytes]
		}
		rules[entry] = content
	}
	return rules, nil
}

func (p *Plugin) PublishReview(_ context.Context, req contract.PublishRequest) (*contract.PublishResult, error) {
	p.Published = append(p.Published, req)
	comments := make([]contract.PublishedComment, 0, len(req.Inlines)+len(req.FileComments))
	var nextID int64 = 1
	for _, c := range req.Inlines {
		comments = append(comments, contract.PublishedComment{
			Fingerprint: c.Fingerprint,
			Category:    c.Category,
			CommentID:   nextID,
		})
		nextID++
	}
	for _, c := range req.FileComments {
		comments = append(comments, contract.PublishedComment{
			Fingerprint: c.Fingerprint,
			Category:    c.Category,
			CommentID:   nextID,
		})
		nextID++
	}
	return &contract.PublishResult{
		ReviewID: int64(len(p.Published)),
		Comments: comments,
	}, nil
}

func (p *Plugin) PollFeedback(_ context.Context, _ string, _ int, _ []int64) ([]contract.FeedbackSample, error) {
	return nil, nil
}

func (p *Plugin) resolveRepoDir(repo string) (string, error) {
	if repo != "" {
		if info, err := os.Stat(repo); err == nil && info.IsDir() {
			return filepath.Abs(repo)
		}
	}
	if p.DefaultRepoDir != "" {
		return filepath.Abs(p.DefaultRepoDir)
	}
	if repo != "" {
		return filepath.Abs(repo)
	}
	return "", errors.New("repository directory is required")
}

// touchedDirs key: ancestor directory path of changed files; value: true when touched by diff.
func isBaseRuleCandidate(entry string, touchedDirs map[string]bool) bool {
	base := path.Base(entry)
	dir := path.Dir(entry)
	if base == "AGENTS.md" && touchedDirs[dir] {
		return true
	}
	if strings.HasPrefix(entry, "docs/review/") && strings.HasSuffix(entry, ".md") {
		return true
	}
	return false
}

func stripBinaryDiffSections(raw []byte) []byte {
	text := string(raw)
	if !strings.Contains(text, "Binary files ") && !strings.Contains(text, "GIT binary patch") {
		return raw
	}
	lines := strings.Split(text, "\n")
	var out []string
	var section []string

	flushSection := func() {
		if len(section) == 0 {
			return
		}
		joined := strings.Join(section, "\n")
		if !strings.Contains(joined, "\nBinary files ") && !strings.Contains(joined, "\nGIT binary patch") {
			out = append(out, section...)
		}
		section = nil
	}

	for _, line := range lines {
		if strings.HasPrefix(line, "diff --git ") {
			flushSection()
		}
		section = append(section, line)
	}
	flushSection()
	return []byte(strings.Join(out, "\n"))
}

func gitOutput(ctx context.Context, dir string, args ...string) (string, error) {
	b, err := gitBytes(ctx, dir, args...)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}

func gitBytes(ctx context.Context, dir string, args ...string) ([]byte, error) {
	fullArgs := append([]string{"-c", "core.hooksPath=/dev/null", "-c", "core.fsmonitor=false"}, args...)
	cmd := exec.CommandContext(ctx, "git", fullArgs...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_OPTIONAL_LOCKS=0", "GIT_TERMINAL_PROMPT=0")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git %v: %w (%s)", args, err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}
