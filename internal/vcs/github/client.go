package github

import (
	"bytes"
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/zetxqx/review-agent-infra/pkg/contract"
)

const (
	defaultGitHubAPIBase = "https://api.github.com"
	defaultMaxDiffBytes  = 16 << 20
	defaultMaxTarBytes   = 256 << 20
	maxRuleBytes         = 64 << 10
	maxListPages         = 10
	listPageSize         = 100
)

type ghAppMeta struct {
	ID int64 `json:"id"`
}

type ghReviewRecord struct {
	ID                    int64      `json:"id"`
	Body                  string     `json:"body"`
	User                  ghUser     `json:"user"`
	PerformedViaGitHubApp *ghAppMeta `json:"performed_via_github_app"`
}

type ghCommentRecord struct {
	ID                    int64      `json:"id"`
	Body                  string     `json:"body"`
	User                  ghUser     `json:"user"`
	PerformedViaGitHubApp *ghAppMeta `json:"performed_via_github_app"`
}

var (
	_ contract.VCSPlugin       = (*Plugin)(nil)
	_ contract.CheckLogFetcher = (*Plugin)(nil)

	reFPMarker        = regexp.MustCompile(`<!--\s*fp:([A-Za-z0-9_-]+)\s*-->`)
	reReviewRunMarker = regexp.MustCompile(`<!--\s*review-run:\s*([^\s>]+)(?:\s+sha:\s*([^\s>]+))?(?:\s+fps:\s*([A-Za-z0-9_,-]*))?\s*-->`)
)

type Config struct {
	AppID          int64
	InstallationID int64
	PrivateKey     *rsa.PrivateKey
	StaticToken    string
	WebhookSecret  []byte
	APIBaseURL     string
	HTTPClient     *http.Client
	MaxDiffBytes   int64
	MaxTarBytes    int64
	Now            func() time.Time
}

type Plugin struct {
	webhookSecret []byte
	apiBaseURL    string
	httpClient    *http.Client
	auth          *AppAuthenticator
	maxDiffBytes  int64
	maxTarBytes   int64
}

func NewPlugin(cfg Config) *Plugin {
	baseURL := strings.TrimRight(cfg.APIBaseURL, "/")
	if baseURL == "" {
		baseURL = defaultGitHubAPIBase
	}
	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: 60 * time.Second}
	}
	maxDiff := cfg.MaxDiffBytes
	if maxDiff <= 0 {
		maxDiff = defaultMaxDiffBytes
	}
	maxTar := cfg.MaxTarBytes
	if maxTar <= 0 {
		maxTar = defaultMaxTarBytes
	}
	cfg.APIBaseURL = baseURL
	cfg.HTTPClient = hc
	return &Plugin{
		webhookSecret: slices.Clone(cfg.WebhookSecret),
		apiBaseURL:    baseURL,
		httpClient:    hc,
		auth:          newAppAuthenticator(cfg),
		maxDiffBytes:  maxDiff,
		maxTarBytes:   maxTar,
	}
}

func (p *Plugin) CheckCredentials(ctx context.Context) error {
	return p.auth.Check(ctx)
}

func (p *Plugin) canFetchLivePR() bool {
	return p.auth.HasCredentials() || p.apiBaseURL != defaultGitHubAPIBase
}

func (p *Plugin) GetHeadSHA(ctx context.Context, repo string, prNumber int) (string, error) {
	pr, err := p.fetchPullRequest(ctx, repo, prNumber)
	if err != nil {
		return "", err
	}
	if pr.Head.SHA == "" {
		return "", fmt.Errorf("empty head SHA on %s#%d", repo, prNumber)
	}
	return pr.Head.SHA, nil
}

func (p *Plugin) FetchDiffAndTarball(ctx context.Context, repo string, prNumber int, baseSHA, headSHA string) ([]byte, io.ReadCloser, error) {
	owner, name, err := splitOwnerRepo(repo)
	if err != nil {
		return nil, nil, err
	}
	if headSHA == "" {
		return nil, nil, errors.New("headSHA is required")
	}

	var diffPath string
	if baseSHA != "" {
		diffPath = fmt.Sprintf("/repos/%s/%s/compare/%s...%s", owner, name, url.PathEscape(baseSHA), url.PathEscape(headSHA))
	} else {
		diffPath = fmt.Sprintf("/repos/%s/%s/pulls/%d", owner, name, prNumber)
	}

	diffBytes, err := p.doBytes(ctx, repo, http.MethodGet, diffPath, "application/vnd.github.v3.diff", nil, p.maxDiffBytes)
	if err != nil {
		return nil, nil, fmt.Errorf("fetch GitHub diff: %w", err)
	}
	cleanDiff := stripBinaryDiffSections(diffBytes)

	tarPath := fmt.Sprintf("/repos/%s/%s/tarball/%s", owner, name, url.PathEscape(headSHA))
	resp, err := p.doRaw(ctx, repo, http.MethodGet, tarPath, "application/vnd.github+json", nil)
	if err != nil {
		return nil, nil, fmt.Errorf("fetch GitHub tarball: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		_ = resp.Body.Close()
		return nil, nil, fmt.Errorf("fetch GitHub tarball status %d: %s", resp.StatusCode, strings.TrimSpace(string(errBody)))
	}

	return cleanDiff, &sizeCappedReadCloser{
		rc:  resp.Body,
		max: p.maxTarBytes,
	}, nil
}

// Return map key: repository-relative rule file path; value: raw UTF-8 rule bytes.
func (p *Plugin) FetchBaseRules(ctx context.Context, repo, baseSHA string, changedFiles []string) (map[string][]byte, error) {
	owner, name, err := splitOwnerRepo(repo)
	if err != nil {
		return nil, err
	}
	// key: repository-relative rule file path; value: raw UTF-8 rule bytes
	rules := make(map[string][]byte)
	if baseSHA == "" {
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

	treePath := fmt.Sprintf("/repos/%s/%s/git/trees/%s?recursive=1", owner, name, url.PathEscape(baseSHA))
	treeBody, treeErr := p.doBytes(ctx, repo, http.MethodGet, treePath, "application/vnd.github+json", nil, 8<<20)
	var candidatePaths []string
	if treeErr == nil {
		var parsedTree struct {
			Tree []struct {
				Path string `json:"path"`
				Type string `json:"type"`
			} `json:"tree"`
		}
		if json.Unmarshal(treeBody, &parsedTree) == nil {
			for _, item := range parsedTree.Tree {
				if item.Type != "blob" {
					continue
				}
				if isBaseRulePath(item.Path, touchedDirs) {
					candidatePaths = append(candidatePaths, item.Path)
				}
			}
		}
	} else {
		candidatePaths = append(candidatePaths, "AGENTS.md")
	}

	slices.Sort(candidatePaths)
	for _, relPath := range candidatePaths {
		content, fetchErr := p.fetchFileAtRef(ctx, repo, owner, name, relPath, baseSHA)
		if fetchErr != nil || len(content) == 0 {
			continue
		}
		if len(content) > maxRuleBytes {
			content = content[:maxRuleBytes]
		}
		rules[relPath] = content
	}
	return rules, nil
}

func (p *Plugin) FetchCheckLogs(ctx context.Context, repo string, _ int, headSHA string, maxBytes int) ([]byte, error) {
	owner, name, err := splitOwnerRepo(repo)
	if err != nil {
		return nil, err
	}
	if headSHA == "" || maxBytes <= 0 {
		return nil, nil
	}
	checksPath := fmt.Sprintf("/repos/%s/%s/commits/%s/check-runs?per_page=100", owner, name, url.PathEscape(headSHA))
	raw, err := p.doBytes(ctx, repo, http.MethodGet, checksPath, "application/vnd.github+json", nil, 4<<20)
	if err != nil {
		return nil, err
	}

	var parsed struct {
		CheckRuns []struct {
			ID         int64  `json:"id"`
			Name       string `json:"name"`
			Status     string `json:"status"`
			Conclusion string `json:"conclusion"`
			Output     struct {
				Title   string `json:"title"`
				Summary string `json:"summary"`
				Text    string `json:"text"`
			} `json:"output"`
		} `json:"check_runs"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, err
	}

	var b strings.Builder
	for _, cr := range parsed.CheckRuns {
		switch strings.ToLower(cr.Conclusion) {
		case "failure", "timed_out", "action_required", "cancelled":
		default:
			continue
		}
		fmt.Fprintf(&b, "=== Check: %s (%s) ===\n", cr.Name, cr.Conclusion)
		if cr.Output.Title != "" {
			b.WriteString(cr.Output.Title + "\n")
		}
		if cr.Output.Summary != "" {
			b.WriteString(cr.Output.Summary + "\n")
		}
		if cr.Output.Text != "" {
			b.WriteString(cr.Output.Text + "\n")
		}
		b.WriteByte('\n')
		if b.Len() >= maxBytes {
			break
		}
	}

	out := []byte(b.String())
	if len(out) > maxBytes {
		out = out[:maxBytes]
	}
	return out, nil
}

func formatReviewRunMarker(runID, commitID string, fps []string) string {
	// key: finding fingerprint; value: true when already included
	seen := make(map[string]bool, len(fps))
	uniq := make([]string, 0, len(fps))
	for _, fp := range fps {
		if fp == "" || seen[fp] {
			continue
		}
		seen[fp] = true
		uniq = append(uniq, fp)
	}
	slices.Sort(uniq)
	return fmt.Sprintf("<!-- review-run: %s sha: %s fps: %s -->", runID, commitID, strings.Join(uniq, ","))
}

func (p *Plugin) isTrustedMarkerAuthor(app *ghAppMeta, user ghUser) bool {
	if p.auth.AppID() <= 0 {
		return true
	}
	if app != nil && app.ID > 0 {
		return app.ID == p.auth.AppID()
	}
	if botLogin := p.auth.BotLogin(); botLogin != "" {
		return strings.EqualFold(strings.TrimSpace(user.Login), botLogin)
	}
	return isBotUser(user)
}

func (p *Plugin) listPRReviews(ctx context.Context, repo, owner, name string, prNumber int) ([]ghReviewRecord, error) {
	var all []ghReviewRecord
	for page := 1; page <= maxListPages; page++ {
		reviewsPath := fmt.Sprintf("/repos/%s/%s/pulls/%d/reviews?per_page=%d&page=%d", owner, name, prNumber, listPageSize, page)
		raw, err := p.doBytes(ctx, repo, http.MethodGet, reviewsPath, "application/vnd.github+json", nil, 4<<20)
		if err != nil {
			return all, err
		}
		var batch []ghReviewRecord
		if err := json.Unmarshal(raw, &batch); err != nil {
			return all, err
		}
		all = append(all, batch...)
		if len(batch) < listPageSize {
			break
		}
	}
	return all, nil
}

func (p *Plugin) ListPublishedFingerprints(ctx context.Context, repo string, prNumber int) ([]string, error) {
	owner, name, err := splitOwnerRepo(repo)
	if err != nil {
		return nil, err
	}
	reviews, err := p.listPRReviews(ctx, repo, owner, name, prNumber)
	if err != nil {
		return nil, err
	}
	// key: published finding fingerprint from PR review markers; value: true
	seen := make(map[string]bool)
	var out []string
	for _, rv := range reviews {
		if !p.isTrustedMarkerAuthor(rv.PerformedViaGitHubApp, rv.User) {
			continue
		}
		for _, m := range reReviewRunMarker.FindAllStringSubmatch(rv.Body, -1) {
			if len(m) < 4 || strings.TrimSpace(m[3]) == "" {
				continue
			}
			for _, fp := range strings.Split(m[3], ",") {
				fp = strings.TrimSpace(fp)
				if fp == "" || seen[fp] {
					continue
				}
				seen[fp] = true
				out = append(out, fp)
			}
		}
	}
	slices.Sort(out)
	return out, nil
}

func (p *Plugin) PublishReview(ctx context.Context, req contract.PublishRequest) (*contract.PublishResult, error) {
	owner, name, err := splitOwnerRepo(req.Repo)
	if err != nil {
		return nil, err
	}

	// key: finding fingerprint; value: finding category name from request
	categoryByFP := make(map[string]string, len(req.Inlines)+len(req.FileComments))
	var markerFPs []string
	markerFPs = append(markerFPs, req.Fingerprints...)
	for _, ic := range req.Inlines {
		if ic.Fingerprint != "" {
			categoryByFP[ic.Fingerprint] = ic.Category
			markerFPs = append(markerFPs, ic.Fingerprint)
		}
	}
	for _, fc := range req.FileComments {
		if fc.Fingerprint != "" {
			categoryByFP[fc.Fingerprint] = fc.Category
			markerFPs = append(markerFPs, fc.Fingerprint)
		}
	}

	if req.RunID != "" {
		if existingReviewID, found, findErr := p.findExistingReviewByRunID(ctx, req.Repo, owner, name, req.PRNumber, req.RunID); findErr == nil && found {
			comments, _ := p.collectPostedCommentsForReview(ctx, req.Repo, owner, name, req.PRNumber, existingReviewID, categoryByFP)
			return &contract.PublishResult{
				ReviewID: existingReviewID,
				Comments: comments,
			}, nil
		}
	}

	existingFPs, _ := p.listExistingPRCommentFingerprints(ctx, req.Repo, owner, name, req.PRNumber)
	if markerExisting, listErr := p.ListPublishedFingerprints(ctx, req.Repo, req.PRNumber); listErr == nil {
		for _, fp := range markerExisting {
			existingFPs[fp] = true
		}
	}

	type ghDraftComment struct {
		Path        string `json:"path"`
		Line        int    `json:"line,omitempty"`
		Side        string `json:"side,omitempty"`
		SubjectType string `json:"subject_type,omitempty"`
		Body        string `json:"body"`
	}

	var draftComments []ghDraftComment
	for _, ic := range req.Inlines {
		if ic.Fingerprint != "" && existingFPs[ic.Fingerprint] {
			continue
		}
		side := ic.Side
		if side == "" {
			side = "RIGHT"
		}
		body := ic.Body
		if ic.Fingerprint != "" {
			body = strings.TrimSpace(body) + "\n<!-- fp:" + ic.Fingerprint + " -->"
		}
		draftComments = append(draftComments, ghDraftComment{
			Path: ic.File,
			Line: int(ic.Line),
			Side: side,
			Body: body,
		})
	}
	for _, fc := range req.FileComments {
		if fc.Fingerprint != "" && existingFPs[fc.Fingerprint] {
			continue
		}
		body := fc.Body
		if fc.Fingerprint != "" {
			body = strings.TrimSpace(body) + "\n<!-- fp:" + fc.Fingerprint + " -->"
		}
		draftComments = append(draftComments, ghDraftComment{
			Path:        fc.File,
			SubjectType: "file",
			Body:        body,
		})
	}

	summaryBody := strings.TrimSpace(req.Summary)
	if (len(req.Inlines) > 0 || len(req.FileComments) > 0) && len(draftComments) == 0 && summaryBody == "" {
		return &contract.PublishResult{}, nil
	}
	if req.RunID != "" {
		runMarker := formatReviewRunMarker(req.RunID, req.CommitID, markerFPs)
		if summaryBody == "" {
			summaryBody = runMarker
		} else {
			summaryBody = summaryBody + "\n\n" + runMarker
		}
	}

	postPayload := struct {
		CommitID string           `json:"commit_id,omitempty"`
		Body     string           `json:"body"`
		Event    string           `json:"event"`
		Comments []ghDraftComment `json:"comments,omitempty"`
	}{
		CommitID: req.CommitID,
		Body:     summaryBody,
		Event:    "COMMENT",
		Comments: draftComments,
	}
	encodedPayload, err := json.Marshal(postPayload)
	if err != nil {
		return nil, err
	}

	reviewsPath := fmt.Sprintf("/repos/%s/%s/pulls/%d/reviews", owner, name, req.PRNumber)
	respBytes, err := p.doBytes(ctx, req.Repo, http.MethodPost, reviewsPath, "application/vnd.github+json", encodedPayload, 4<<20)
	if err != nil {
		return nil, fmt.Errorf("post GitHub review: %w", err)
	}

	var createdReview struct {
		ID int64 `json:"id"`
	}
	if err := json.Unmarshal(respBytes, &createdReview); err != nil {
		return nil, fmt.Errorf("decode created GitHub review: %w", err)
	}

	postedComments, _ := p.collectPostedCommentsForReview(ctx, req.Repo, owner, name, req.PRNumber, createdReview.ID, categoryByFP)
	return &contract.PublishResult{
		ReviewID: createdReview.ID,
		Comments: postedComments,
	}, nil
}

func (p *Plugin) PollFeedback(ctx context.Context, repo string, _ int, commentIDs []int64) ([]contract.FeedbackSample, error) {
	owner, name, err := splitOwnerRepo(repo)
	if err != nil {
		return nil, err
	}
	var samples []contract.FeedbackSample
	for _, cid := range commentIDs {
		if cid <= 0 {
			continue
		}
		reactPath := fmt.Sprintf("/repos/%s/%s/pulls/comments/%d/reactions?per_page=100", owner, name, cid)
		raw, err := p.doBytes(ctx, repo, http.MethodGet, reactPath, "application/vnd.github+json", nil, 2<<20)
		if err != nil {
			return nil, err
		}
		var reactions []struct {
			Content string `json:"content"`
		}
		if err := json.Unmarshal(raw, &reactions); err != nil {
			return nil, err
		}
		up, down := 0, 0
		for _, r := range reactions {
			switch r.Content {
			case "+1":
				up++
			case "-1":
				down++
			}
		}
		samples = append(samples, contract.FeedbackSample{
			CommentID:  cid,
			ThumbsUp:   up,
			ThumbsDown: down,
		})
	}
	return samples, nil
}

func (p *Plugin) fetchPullRequest(ctx context.Context, repo string, prNumber int) (*ghPullRequest, error) {
	owner, name, err := splitOwnerRepo(repo)
	if err != nil {
		return nil, err
	}
	prPath := fmt.Sprintf("/repos/%s/%s/pulls/%d", owner, name, prNumber)
	raw, err := p.doBytes(ctx, repo, http.MethodGet, prPath, "application/vnd.github+json", nil, 4<<20)
	if err != nil {
		return nil, err
	}
	var pr ghPullRequest
	if err := json.Unmarshal(raw, &pr); err != nil {
		return nil, err
	}
	return &pr, nil
}

func (p *Plugin) fetchFileAtRef(ctx context.Context, repo, owner, name, relPath, ref string) ([]byte, error) {
	segments := strings.Split(relPath, "/")
	for i, s := range segments {
		segments[i] = url.PathEscape(s)
	}
	escapedPath := strings.Join(segments, "/")
	contentPath := fmt.Sprintf("/repos/%s/%s/contents/%s?ref=%s", owner, name, escapedPath, url.QueryEscape(ref))

	resp, err := p.doRaw(ctx, repo, http.MethodGet, contentPath, "application/vnd.github.raw", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch %s at %s: status %d", relPath, ref, resp.StatusCode)
	}

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxRuleBytes+4096))
	if err != nil {
		return nil, err
	}
	if strings.Contains(resp.Header.Get("Content-Type"), "application/json") {
		var envelope struct {
			Content  string `json:"content"`
			Encoding string `json:"encoding"`
		}
		if json.Unmarshal(raw, &envelope) == nil && envelope.Encoding == "base64" {
			cleaned := strings.ReplaceAll(envelope.Content, "\n", "")
			if decoded, decErr := base64.StdEncoding.DecodeString(cleaned); decErr == nil {
				return decoded, nil
			}
		}
	}
	return raw, nil
}

func (p *Plugin) findExistingReviewByRunID(ctx context.Context, repo, owner, name string, prNumber int, runID string) (int64, bool, error) {
	reviews, err := p.listPRReviews(ctx, repo, owner, name, prNumber)
	if err != nil {
		return 0, false, err
	}
	for _, rv := range reviews {
		if !p.isTrustedMarkerAuthor(rv.PerformedViaGitHubApp, rv.User) {
			continue
		}
		for _, m := range reReviewRunMarker.FindAllStringSubmatch(rv.Body, -1) {
			if len(m) >= 2 && m[1] == runID {
				return rv.ID, true, nil
			}
		}
	}
	return 0, false, nil
}

// Return map key: finding fingerprint; value: true when already present on PR review comments.
func (p *Plugin) listExistingPRCommentFingerprints(ctx context.Context, repo, owner, name string, prNumber int) (map[string]bool, error) {
	// key: finding fingerprint; value: true when already present on PR review comments
	out := make(map[string]bool)
	for page := 1; page <= maxListPages; page++ {
		commentsPath := fmt.Sprintf("/repos/%s/%s/pulls/%d/comments?per_page=%d&page=%d", owner, name, prNumber, listPageSize, page)
		raw, err := p.doBytes(ctx, repo, http.MethodGet, commentsPath, "application/vnd.github+json", nil, 4<<20)
		if err != nil {
			return out, err
		}
		var comments []ghCommentRecord
		if err := json.Unmarshal(raw, &comments); err != nil {
			return out, err
		}
		for _, c := range comments {
			if !p.isTrustedMarkerAuthor(c.PerformedViaGitHubApp, c.User) {
				continue
			}
			if m := reFPMarker.FindStringSubmatch(c.Body); len(m) == 2 {
				out[m[1]] = true
			}
		}
		if len(comments) < listPageSize {
			break
		}
	}
	return out, nil
}

// categoryByFP key: finding fingerprint; value: finding category name from PublishRequest.
func (p *Plugin) collectPostedCommentsForReview(ctx context.Context, repo, owner, name string, prNumber int, reviewID int64, categoryByFP map[string]string) ([]contract.PublishedComment, error) {
	var raw []byte
	var err error
	if reviewID > 0 {
		revCommentsPath := fmt.Sprintf("/repos/%s/%s/pulls/%d/reviews/%d/comments?per_page=100", owner, name, prNumber, reviewID)
		raw, err = p.doBytes(ctx, repo, http.MethodGet, revCommentsPath, "application/vnd.github+json", nil, 4<<20)
	}
	if err != nil || len(raw) == 0 {
		prCommentsPath := fmt.Sprintf("/repos/%s/%s/pulls/%d/comments?per_page=100", owner, name, prNumber)
		raw, err = p.doBytes(ctx, repo, http.MethodGet, prCommentsPath, "application/vnd.github+json", nil, 4<<20)
		if err != nil {
			return nil, err
		}
	}

	var ghComments []struct {
		ID   int64  `json:"id"`
		Body string `json:"body"`
	}
	if err := json.Unmarshal(raw, &ghComments); err != nil {
		return nil, err
	}

	var out []contract.PublishedComment
	for _, c := range ghComments {
		m := reFPMarker.FindStringSubmatch(c.Body)
		if len(m) != 2 {
			continue
		}
		fp := m[1]
		cat, wanted := categoryByFP[fp]
		if len(categoryByFP) > 0 && !wanted {
			continue
		}
		out = append(out, contract.PublishedComment{
			Fingerprint: fp,
			Category:    cat,
			CommentID:   c.ID,
		})
	}
	return out, nil
}

func (p *Plugin) doBytes(ctx context.Context, repo, method, urlPath, accept string, reqBody []byte, maxBytes int64) ([]byte, error) {
	resp, err := p.doRaw(ctx, repo, method, urlPath, accept, reqBody)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, readErr := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
	if readErr != nil {
		return nil, readErr
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("GitHub response for %s exceeds %d byte cap", urlPath, maxBytes)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("GitHub %s %s returned status %d: %s", method, urlPath, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	return data, nil
}

func (p *Plugin) doRaw(ctx context.Context, repo, method, urlPath, accept string, reqBody []byte) (*http.Response, error) {
	tok, err := p.auth.TokenForRepo(ctx, repo)
	if err != nil {
		return nil, err
	}
	var bodyReader io.Reader
	if reqBody != nil {
		bodyReader = bytes.NewReader(reqBody)
	}
	req, err := http.NewRequestWithContext(ctx, method, p.apiBaseURL+urlPath, bodyReader)
	if err != nil {
		return nil, err
	}
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	return p.httpClient.Do(req)
}

type sizeCappedReadCloser struct {
	rc   io.ReadCloser
	read int64
	max  int64
}

func (s *sizeCappedReadCloser) Read(p []byte) (int, error) {
	n, err := s.rc.Read(p)
	s.read += int64(n)
	if s.read > s.max {
		_ = s.rc.Close()
		return 0, fmt.Errorf("GitHub tarball stream exceeds %d byte cap", s.max)
	}
	return n, err
}

func (s *sizeCappedReadCloser) Close() error {
	return s.rc.Close()
}

// touchedDirs key: ancestor directory path of changed files; value: true when touched by diff.
func isBaseRulePath(entry string, touchedDirs map[string]bool) bool {
	if path.Base(entry) == "AGENTS.md" && touchedDirs[path.Dir(entry)] {
		return true
	}
	return strings.HasPrefix(entry, "docs/review/") && strings.HasSuffix(entry, ".md")
}

func stripBinaryDiffSections(raw []byte) []byte {
	text := string(raw)
	if !strings.Contains(text, "Binary files ") && !strings.Contains(text, "GIT binary patch") {
		return raw
	}
	lines := strings.Split(text, "\n")
	var out []string
	var section []string

	flush := func() {
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
			flush()
		}
		section = append(section, line)
	}
	flush()
	return []byte(strings.Join(out, "\n"))
}

func splitOwnerRepo(repo string) (string, string, error) {
	trimmed := strings.Trim(strings.TrimSpace(repo), "/")
	owner, name, ok := strings.Cut(trimmed, "/")
	if !ok || owner == "" || name == "" || strings.Contains(name, "/") {
		return "", "", fmt.Errorf("invalid GitHub repository slug %q (expected owner/repo)", repo)
	}
	return owner, name, nil
}
