package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"text/tabwriter"
	"time"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
	"github.com/zetxqx/review-agent-infra/internal/inference"
	"github.com/zetxqx/review-agent-infra/internal/publisher"
	"github.com/zetxqx/review-agent-infra/internal/sandbox"
	"github.com/zetxqx/review-agent-infra/internal/token"
	"github.com/zetxqx/review-agent-infra/internal/vcs/github"
	"github.com/zetxqx/review-agent-infra/internal/vcs/localgit"
	"github.com/zetxqx/review-agent-infra/pkg/contract"
	"google.golang.org/protobuf/encoding/protojson"
)

const fallbackGHScript = "/usr/local/google/home/bobzetian/.gemini/config/plugins/github/skills/github/scripts/gh"

type commonFlags struct {
	repoDir              string
	commitRef            string
	commitsCSV           string
	baseRef              string
	headRef              string
	runnerKind           string
	dockerImage          string
	dockerImageSet       bool
	dockerNetwork        string
	loaderBin            string
	upstreamKind         string
	upstreamURL          string
	upstreamURLSet       bool
	upstreamKey          string
	upstreamModel        string
	modelAlias           string
	mockUpstream         bool
	mockScript           string
	minConfidence        float64
	topN                 int
	format               string
	outFile              string
	concurrency          int
	runID                string
	supportsFileComments bool
}

type PublishedReviewInfo struct {
	Repo         string `json:"repo"`
	PRNumber     int    `json:"pr_number"`
	ReviewID     int64  `json:"review_id"`
	CommentCount int    `json:"comment_count"`
}

type ExecutionMetrics struct {
	RunID             string                   `json:"run_id,omitempty"`
	Commit            string                   `json:"commit"`
	Profile           string                   `json:"profile"`
	Adapter           string                   `json:"adapter"`
	Status            string                   `json:"status"`
	NormalizerPath    string                   `json:"normalizer_path"`
	FindingsRaw       int                      `json:"findings_raw"`
	FindingsPublished int                      `json:"findings_published"`
	InlineAligned     int                      `json:"inline_aligned"`
	FallbackCount     int                      `json:"fallback_count"`
	DurationMs        int64                    `json:"duration_ms"`
	Requests          int32                    `json:"requests"`
	NormalizeRequests int32                    `json:"normalize_requests"`
	PromptTokens      int64                    `json:"prompt_tokens"`
	CachedTokens      int64                    `json:"cached_tokens"`
	CompletionTokens  int64                    `json:"completion_tokens"`
	CacheHitRatio     float64                  `json:"cache_hit_ratio"`
	Pipeline          publisher.PipelineResult `json:"-"`
	Report            *reviewv1.ReviewReport   `json:"-"`
}

type ScorecardRow struct {
	Profile           string  `json:"profile"`
	Adapter           string  `json:"adapter"`
	Runs              int     `json:"runs"`
	FindingsRaw       int     `json:"findings_raw"`
	FindingsPublished int     `json:"findings_published"`
	InlineAligned     int     `json:"inline_aligned"`
	AvgLatencyMs      int64   `json:"avg_latency_ms"`
	Requests          int32   `json:"requests"`
	NormalizeRequests int32   `json:"normalize_requests"`
	PromptTokens      int64   `json:"prompt_tokens"`
	CachedTokens      int64   `json:"cached_tokens"`
	CompletionTokens  int64   `json:"completion_tokens"`
	CacheHitRatio     float64 `json:"cache_hit_ratio"`
	NormalizerPath    string  `json:"normalizer_path"`
}

type liveAdapterSession struct {
	sandboxURL   string
	internalURL  string
	tokenKey     []byte
	proxy        *inference.ProxyHandler
	mockUpstream *diffAwareMockUpstream
	closeFn      func()
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	if err := runCLI(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func runCLI(ctx context.Context, args []string, stdout io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: reviewctl <review|compare> [flags]")
	}
	switch args[0] {
	case "review":
		return runReviewCmd(ctx, args[1:], stdout)
	case "compare":
		return runCompareCmd(ctx, args[1:], stdout)
	default:
		return fmt.Errorf("unknown subcommand %q (expected review or compare)", args[0])
	}
}

func bindCommonFlags(fs *flag.FlagSet, c *commonFlags) {
	fs.StringVar(&c.repoDir, "repo", ".", "")
	fs.StringVar(&c.commitRef, "commit", "", "")
	fs.StringVar(&c.commitsCSV, "commits", "", "")
	fs.StringVar(&c.baseRef, "base", "", "")
	fs.StringVar(&c.headRef, "head", "", "")
	fs.StringVar(&c.runnerKind, "runner", envOr("REVIEWCTL_RUNNER", "local"), "")
	fs.StringVar(&c.dockerImage, "docker-image", envOr("REVIEWCTL_DOCKER_IMAGE", sandbox.DefaultSandboxImage), "")
	fs.StringVar(&c.dockerNetwork, "docker-network", envOr("REVIEWCTL_DOCKER_NETWORK", "host"), "")
	fs.StringVar(&c.loaderBin, "loader-bin", os.Getenv("REVIEWCTL_LOADER_BIN"), "")
	fs.StringVar(&c.upstreamKind, "upstream-kind", envOr("REVIEWCTL_UPSTREAM_KIND", "llmd-gateway"), "")
	fs.StringVar(&c.upstreamURL, "upstream-url", envOr("REVIEWCTL_UPSTREAM_URL", "http://127.0.0.1:8000"), "")
	fs.StringVar(&c.upstreamKey, "upstream-api-key", os.Getenv("REVIEWCTL_UPSTREAM_API_KEY"), "")
	fs.StringVar(&c.upstreamModel, "upstream-model", envOr("REVIEWCTL_UPSTREAM_MODEL", "Qwen/Qwen3-Coder-30B-A3B-Instruct"), "")
	fs.StringVar(&c.modelAlias, "model-alias", envOr("REVIEWCTL_MODEL_ALIAS", "review-coder"), "")
	fs.BoolVar(&c.mockUpstream, "mock-upstream", false, "")
	fs.StringVar(&c.mockScript, "mock-script", envOr("REVIEWCTL_MOCK_SCRIPT", ""), "")
	fs.Float64Var(&c.minConfidence, "min-confidence", publisher.DefaultMinConfidence, "")
	fs.IntVar(&c.topN, "top-n", publisher.DefaultTopN, "")
	fs.StringVar(&c.format, "format", "markdown", "")
	fs.StringVar(&c.outFile, "out", "", "")
	fs.IntVar(&c.concurrency, "concurrency", 2, "")
}

func runReviewCmd(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("reviewctl review", flag.ContinueOnError)
	var cfg commonFlags
	var harnessName string
	var publishPR int
	var githubRepo string
	var skipHeadCheck bool
	bindCommonFlags(fs, &cfg)
	fs.StringVar(&harnessName, "harness", "single-shot", "")
	fs.IntVar(&publishPR, "publish-pr", 0, "")
	fs.StringVar(&githubRepo, "github-repo", "", "")
	fs.StringVar(&cfg.runID, "run-id", "", "")
	fs.BoolVar(&cfg.supportsFileComments, "supports-file-comments", false, "")
	fs.BoolVar(&skipHeadCheck, "skip-head-check", false, "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "upstream-url" {
			cfg.upstreamURLSet = true
		}
		if f.Name == "docker-image" {
			cfg.dockerImageSet = true
		}
	})

	profile, err := resolveProfile(harnessName)
	if err != nil {
		return err
	}
	commits, err := resolveTargetCommits(ctx, cfg)
	if err != nil {
		return err
	}

	var vcs *github.Plugin
	var ghRepo string
	if publishPR > 0 {
		ghRepo, err = resolveGitHubRepo(ctx, githubRepo, commits[0].RepoDir)
		if err != nil {
			return err
		}
		ghToken, tokErr := resolveGitHubToken(ctx)
		if tokErr != nil {
			return tokErr
		}
		apiBase := envOr("GITHUB_API_URL", "")
		if ghToken == "" && apiBase == "" {
			return errors.New("GitHub token is required for --publish-pr (set GITHUB_TOKEN or authenticate with gh)")
		}
		vcs = github.NewPlugin(github.Config{
			StaticToken: ghToken,
			APIBaseURL:  apiBase,
		})
	}

	adapterSess, err := startEphemeralAdapter(cfg)
	if err != nil {
		return err
	}
	defer adapterSess.closeFn()

	runner, cleanupRunner, err := buildSandboxRunner(cfg)
	if err != nil {
		return err
	}
	defer cleanupRunner()

	execRes, err := executeProfileOnCommit(ctx, cfg, adapterSess, runner, commits[0], profile, 0)
	if err != nil {
		return err
	}

	var pubInfo *PublishedReviewInfo
	if vcs != nil {
		if !skipHeadCheck {
			remoteHead, headErr := vcs.GetHeadSHA(ctx, ghRepo, publishPR)
			if headErr != nil {
				return fmt.Errorf("verify PR #%d head SHA on %s: %w", publishPR, ghRepo, headErr)
			}
			if remoteHead != commits[0].HeadSHA {
				return fmt.Errorf("PR #%d remote head SHA %s does not match reviewed commit %s (pass --skip-head-check to override)", publishPR, remoteHead, commits[0].HeadSHA)
			}
		}
		pubRes, pubErr := vcs.PublishReview(ctx, contract.PublishRequest{
			Repo:         ghRepo,
			PRNumber:     publishPR,
			CommitID:     commits[0].HeadSHA,
			RunID:        execRes.RunID,
			Event:        "COMMENT",
			Summary:      execRes.Pipeline.Summary,
			Inlines:      execRes.Pipeline.Inlines,
			FileComments: execRes.Pipeline.FileComments,
		})
		if pubErr != nil {
			return pubErr
		}
		pubInfo = &PublishedReviewInfo{
			Repo:         ghRepo,
			PRNumber:     publishPR,
			ReviewID:     pubRes.ReviewID,
			CommentCount: len(pubRes.Comments),
		}
	}

	var rendered []byte
	if cfg.format == "json" {
		rendered, err = json.MarshalIndent(struct {
			Metrics   ExecutionMetrics         `json:"metrics"`
			Summary   string                   `json:"summary"`
			Inlines   []contract.InlineComment `json:"inlines"`
			Published *PublishedReviewInfo     `json:"published,omitempty"`
		}{
			Metrics:   execRes,
			Summary:   execRes.Pipeline.Summary,
			Inlines:   execRes.Pipeline.Inlines,
			Published: pubInfo,
		}, "", "  ")
		if err != nil {
			return err
		}
		rendered = append(rendered, '\n')
	} else {
		rendered = []byte(renderSingleReviewMarkdown(commits[0], execRes, pubInfo))
	}

	if cfg.outFile != "" {
		if err := os.WriteFile(cfg.outFile, rendered, 0o644); err != nil {
			return err
		}
	}
	_, err = stdout.Write(rendered)
	return err
}

func runCompareCmd(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("reviewctl compare", flag.ContinueOnError)
	var cfg commonFlags
	var harnessesCSV string
	var harnessAlias string
	bindCommonFlags(fs, &cfg)
	cfg.format = "table"
	fs.StringVar(&harnessesCSV, "harnesses", "single-shot,mock-llm", "")
	fs.StringVar(&harnessAlias, "harness", "", "")
	if err := fs.Parse(args); err != nil {
		return err
	}
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "upstream-url" {
			cfg.upstreamURLSet = true
		}
		if f.Name == "docker-image" {
			cfg.dockerImageSet = true
		}
	})
	if harnessAlias != "" {
		harnessesCSV = harnessAlias
	}

	var profiles []ProfileSpec
	for _, rawName := range strings.Split(harnessesCSV, ",") {
		if strings.TrimSpace(rawName) == "" {
			continue
		}
		prof, err := resolveProfile(rawName)
		if err != nil {
			return err
		}
		profiles = append(profiles, prof)
	}
	if len(profiles) == 0 {
		return errors.New("at least one harness profile is required")
	}

	commits, err := resolveTargetCommits(ctx, cfg)
	if err != nil {
		return err
	}

	adapterSess, err := startEphemeralAdapter(cfg)
	if err != nil {
		return err
	}
	defer adapterSess.closeFn()

	runner, cleanupRunner, err := buildSandboxRunner(cfg)
	if err != nil {
		return err
	}
	defer cleanupRunner()

	type jobSpec struct {
		idx     int
		commit  localgit.CommitSpec
		profile ProfileSpec
	}
	var jobs []jobSpec
	for _, c := range commits {
		for _, p := range profiles {
			jobs = append(jobs, jobSpec{idx: len(jobs), commit: c, profile: p})
		}
	}

	results := make([]ExecutionMetrics, len(jobs))
	limit := cfg.concurrency
	if limit <= 0 {
		limit = 1
	}

	runCtx, cancelRuns := context.WithCancel(ctx)
	defer cancelRuns()

	sem := make(chan struct{}, limit)
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error

	for _, jb := range jobs {
		job := jb
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-runCtx.Done():
				return
			}
			defer func() { <-sem }()

			res, runErr := executeProfileOnCommit(runCtx, cfg, adapterSess, runner, job.commit, job.profile, job.idx)
			if runErr != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("profile %s on %s: %w", job.profile.Name, shortSHA(job.commit.HeadSHA), runErr)
					cancelRuns()
				}
				errMu.Unlock()
				return
			}
			results[job.idx] = res
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}

	rows := aggregateScorecard(profiles, results)

	var outBytes []byte
	if cfg.format == "json" {
		outBytes, err = json.MarshalIndent(struct {
			Scorecard []ScorecardRow     `json:"scorecard"`
			Runs      []ExecutionMetrics `json:"runs"`
		}{
			Scorecard: rows,
			Runs:      results,
		}, "", "  ")
		if err != nil {
			return err
		}
		outBytes = append(outBytes, '\n')
	} else {
		outBytes = []byte(renderScorecardTable(commits, rows, results))
	}

	if cfg.outFile != "" {
		if err := os.WriteFile(cfg.outFile, outBytes, 0o644); err != nil {
			return err
		}
	}
	_, err = stdout.Write(outBytes)
	return err
}

func resolveTargetCommits(ctx context.Context, cfg commonFlags) ([]localgit.CommitSpec, error) {
	if cfg.commitsCSV != "" {
		var specs []localgit.CommitSpec
		for _, ref := range strings.Split(cfg.commitsCSV, ",") {
			ref = strings.TrimSpace(ref)
			if ref == "" {
				continue
			}
			sp, err := localgit.ResolveRefs(ctx, cfg.repoDir, ref, "", "")
			if err != nil {
				return nil, err
			}
			specs = append(specs, sp)
		}
		if len(specs) == 0 {
			return nil, errors.New("no valid commits in --commits")
		}
		return specs, nil
	}
	if cfg.commitRef == "" && cfg.baseRef == "" && cfg.headRef == "" {
		cfg.commitRef = "HEAD"
	}
	sp, err := localgit.ResolveRefs(ctx, cfg.repoDir, cfg.commitRef, cfg.baseRef, cfg.headRef)
	if err != nil {
		return nil, err
	}
	return []localgit.CommitSpec{sp}, nil
}

func startEphemeralAdapter(cfg commonFlags) (*liveAdapterSession, error) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}

	var script *mockUpstreamScript
	if cfg.mockScript != "" {
		loaded, scriptErr := loadMockUpstreamScript(cfg.mockScript)
		if scriptErr != nil {
			return nil, scriptErr
		}
		script = loaded
	}

	var closers []func()
	var mockUpstream *diffAwareMockUpstream
	upstreamURL := cfg.upstreamURL
	useMock := cfg.mockUpstream || cfg.mockScript != "" || upstreamURL == "mock"
	if !useMock && !cfg.upstreamURLSet && os.Getenv("REVIEWCTL_UPSTREAM_URL") == "" {
		if conn, dialErr := net.DialTimeout("tcp", "127.0.0.1:8000", 50*time.Millisecond); dialErr == nil {
			_ = conn.Close()
		} else {
			useMock = true
			fmt.Fprintln(os.Stderr, "reviewctl: no live upstream on 127.0.0.1:8000; using built-in mock llm-d SSE upstream")
		}
	}
	if useMock {
		mockSrv, mockHandler := startMockUpstreamServerWithScript(script)
		mockUpstream = mockHandler
		upstreamURL = mockSrv.URL
		closers = append(closers, mockSrv.Close)
	}

	var up contract.UpstreamProvider
	switch cfg.upstreamKind {
	case "llmd-gateway":
		up = &inference.LlmdGatewayUpstream{
			BaseURL:   upstreamURL,
			APIKey:    cfg.upstreamKey,
			Objective: "code-review-batch",
		}
	case "vllm-direct", "ollama":
		up = &inference.VllmDirectUpstream{
			BaseURL: upstreamURL,
			APIKey:  cfg.upstreamKey,
		}
	default:
		for _, fn := range closers {
			fn()
		}
		return nil, fmt.Errorf("unknown --upstream-kind %q", cfg.upstreamKind)
	}

	proxy := inference.NewProxyHandler(inference.Config{
		TokenKey:      key,
		Upstream:      up,
		UpstreamModel: cfg.upstreamModel,
		ModelAlias:    cfg.modelAlias,
	})

	sbxLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		for _, fn := range closers {
			fn()
		}
		return nil, err
	}
	intLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = sbxLn.Close()
		for _, fn := range closers {
			fn()
		}
		return nil, err
	}

	sbxSrv := &http.Server{Handler: proxy.SandboxMux(), ReadHeaderTimeout: 10 * time.Second}
	intSrv := &http.Server{Handler: proxy.InternalMux(), ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = sbxSrv.Serve(sbxLn) }()
	go func() { _ = intSrv.Serve(intLn) }()

	closers = append(closers, func() {
		_ = sbxSrv.Close()
		_ = intSrv.Close()
	})

	return &liveAdapterSession{
		sandboxURL:   "http://" + sbxLn.Addr().String(),
		internalURL:  "http://" + intLn.Addr().String(),
		tokenKey:     key,
		proxy:        proxy,
		mockUpstream: mockUpstream,
		closeFn: func() {
			for i := len(closers) - 1; i >= 0; i-- {
				closers[i]()
			}
		},
	}, nil
}

func buildSandboxRunner(cfg commonFlags) (contract.SandboxRunner, func(), error) {
	switch cfg.runnerKind {
	case "local":
		r := sandbox.NewLocalProcessRunner("")
		r.LoaderBin = cfg.loaderBin
		return r, func() { _ = r.Close() }, nil
	case "docker":
		r := sandbox.NewDockerRunner(cfg.dockerImage, cfg.dockerNetwork)
		return r, func() {}, nil
	default:
		return nil, nil, fmt.Errorf("unknown --runner %q (expected local or docker)", cfg.runnerKind)
	}
}

func executeProfileOnCommit(
	ctx context.Context,
	cfg commonFlags,
	adapterSess *liveAdapterSession,
	runner contract.SandboxRunner,
	commit localgit.CommitSpec,
	profile ProfileSpec,
	seq int,
) (ExecutionMetrics, error) {
	vcs := localgit.New(commit.RepoDir)
	diffPatch, tarStream, err := vcs.FetchDiffAndTarball(ctx, commit.RepoDir, 0, commit.BaseSHA, commit.HeadSHA)
	if err != nil {
		return ExecutionMetrics{}, err
	}
	tmpTarDir, err := os.MkdirTemp("", "reviewctl-snap-*")
	if err != nil {
		_ = tarStream.Close()
		return ExecutionMetrics{}, err
	}
	defer os.RemoveAll(tmpTarDir)

	tarPath := filepath.Join(tmpTarDir, "snapshot.tar.gz")
	tarFile, err := os.Create(tarPath)
	if err != nil {
		_ = tarStream.Close()
		return ExecutionMetrics{}, err
	}
	_, copyErr := io.Copy(tarFile, tarStream)
	_ = tarStream.Close()
	_ = tarFile.Close()
	if copyErr != nil {
		return ExecutionMetrics{}, copyErr
	}

	parsedDiff := publisher.ParseUnifiedDiff(diffPatch)
	rulesMap, err := vcs.FetchBaseRules(ctx, commit.RepoDir, commit.BaseSHA, parsedDiff.Files())
	if err != nil {
		return ExecutionMetrics{}, err
	}

	runID := cfg.runID
	if runID == "" {
		runID = fmt.Sprintf("cli-%s-%s-%s-%d", shortSHA(commit.HeadSHA), randomHexID(4), profile.Name, seq)
	}
	jti := randomHexID(12)
	claims := &reviewv1.TaskClaims{
		RunId:                   runID,
		SessionId:               fmt.Sprintf("%s-s0-%s", runID, profile.Name),
		Repo:                    commit.RepoID,
		HarnessProfile:          profile.Name,
		MaxRequests:             profile.Budget.MaxRequests,
		MaxTotalTokens:          profile.Budget.MaxTotalTokens,
		NormalizerReserveTokens: profile.Budget.NormalizerReserveTokens,
		MaxOutputTokens:         4096,
		Exp:                     time.Now().Add(time.Duration(profile.Budget.WallClockSeconds+300) * time.Second).Unix(),
		Jti:                     jti,
	}
	rawToken, err := token.Mint(adapterSess.tokenKey, claims)
	if err != nil {
		return ExecutionMetrics{}, err
	}

	task := &reviewv1.AgentTask{
		RunId:      runID,
		ShardIndex: 0,
		Harness: &reviewv1.HarnessRef{
			Profile: profile.Name,
			Adapter: profile.Adapter,
			Options: profile.Options,
		},
		ModelAlias: cfg.modelAlias,
		PrTitle:    commit.Title,
		PrBody:     commit.Body,
		BaseBranch: shortSHA(commit.BaseSHA),
		Budget:     profile.Budget,
	}
	taskBytes, err := (protojson.MarshalOptions{Indent: "  "}).Marshal(task)
	if err != nil {
		return ExecutionMetrics{}, err
	}

	// key: file path relative to /workspace/.review/in; value: raw file bytes
	inputs := map[string][]byte{
		"task.json":  append(taskBytes, '\n'),
		"diff.patch": diffPatch,
	}
	for relRule, content := range rulesMap {
		inputs["rules/"+relRule] = content
	}

	harnessImage := profile.Image
	if cfg.dockerImageSet || os.Getenv("REVIEWCTL_DOCKER_IMAGE") != "" {
		harnessImage = cfg.dockerImage
	}

	started := time.Now()
	report, err := runner.Run(ctx, contract.SandboxSpec{
		ShortRunID:     shortSHA(commit.HeadSHA),
		ShardIndex:     0,
		Attempt:        0,
		Role:           "primary",
		HarnessProfile: profile.Name,
		HarnessImage:   harnessImage,
		TarballPath:    tarPath,
		Inputs:         inputs,
		AdapterURL:     adapterSess.sandboxURL,
		TaskToken:      rawToken,
		TimeoutSeconds: int(profile.Budget.WallClockSeconds),
	})
	elapsed := time.Since(started)
	if err != nil {
		return ExecutionMetrics{}, err
	}

	usage := fetchSandboxUsage(ctx, adapterSess.internalURL, jti)

	pipeOpts := publisher.DefaultPipelineOptions()
	pipeOpts.DefaultConfidence = profile.DefaultConfidence
	pipeOpts.MinConfidence = cfg.minConfidence
	pipeOpts.SupportsFileComments = cfg.supportsFileComments
	pipeOpts.TopN = cfg.topN
	pipeOpts.MintedTokens = []string{rawToken}

	rawCount := len(report.GetFindings())
	pipeRes := publisher.RunPipeline(report, diffPatch, pipeOpts)

	var promptToks, cachedToks, compToks int64
	var reqCount, normReqCount int32
	if usage != nil {
		reqCount = usage.Requests
		normReqCount = usage.NormalizeRequests
		promptToks = usage.PromptTokens
		cachedToks = usage.CachedTokens
		compToks = usage.CompletionTokens
	}
	var ratio float64
	if promptToks > 0 {
		ratio = float64(cachedToks) / float64(promptToks)
	}

	return ExecutionMetrics{
		RunID:             runID,
		Commit:            shortSHA(commit.HeadSHA),
		Profile:           profile.Name,
		Adapter:           profile.Adapter,
		Status:            formatReportStatus(report.GetProvenance().GetStatus()),
		NormalizerPath:    formatNormalizerPath(report.GetProvenance().GetNormalizer()),
		FindingsRaw:       rawCount,
		FindingsPublished: len(pipeRes.Aligned),
		InlineAligned:     pipeRes.InlineCount,
		FallbackCount:     pipeRes.FallbackCount,
		DurationMs:        elapsed.Milliseconds(),
		Requests:          reqCount,
		NormalizeRequests: normReqCount,
		PromptTokens:      promptToks,
		CachedTokens:      cachedToks,
		CompletionTokens:  compToks,
		CacheHitRatio:     ratio,
		Pipeline:          pipeRes,
		Report:            report,
	}, nil
}

func fetchSandboxUsage(ctx context.Context, internalURL, jti string) *reviewv1.SandboxUsage {
	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, internalURL+"/internal/usage/"+jti, nil)
	if err != nil {
		return nil
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil
	}
	var usage reviewv1.SandboxUsage
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(body, &usage); err != nil {
		return nil
	}
	return &usage
}

func aggregateScorecard(profiles []ProfileSpec, runs []ExecutionMetrics) []ScorecardRow {
	rows := make([]ScorecardRow, 0, len(profiles))
	for _, p := range profiles {
		var row ScorecardRow
		row.Profile = p.Name
		row.Adapter = p.Adapter
		var totalLatency int64
		// key: normalizer path label; value: true when observed across runs for this profile
		normPaths := make(map[string]bool)
		for _, r := range runs {
			if r.Profile != p.Name {
				continue
			}
			row.Runs++
			row.FindingsRaw += r.FindingsRaw
			row.FindingsPublished += r.FindingsPublished
			row.InlineAligned += r.InlineAligned
			totalLatency += r.DurationMs
			row.Requests += r.Requests
			row.NormalizeRequests += r.NormalizeRequests
			row.PromptTokens += r.PromptTokens
			row.CachedTokens += r.CachedTokens
			row.CompletionTokens += r.CompletionTokens
			normPaths[r.NormalizerPath] = true
		}
		if row.Runs > 0 {
			row.AvgLatencyMs = totalLatency / int64(row.Runs)
		}
		if row.PromptTokens > 0 {
			row.CacheHitRatio = float64(row.CachedTokens) / float64(row.PromptTokens)
		}
		var paths []string
		for k := range normPaths {
			paths = append(paths, k)
		}
		slices.Sort(paths)
		row.NormalizerPath = strings.Join(paths, ",")
		rows = append(rows, row)
	}
	return rows
}

func renderSingleReviewMarkdown(commit localgit.CommitSpec, res ExecutionMetrics, pub *PublishedReviewInfo) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Code Review: `%s` (%s)\n\n", shortSHA(commit.HeadSHA), commit.Title)
	fmt.Fprintf(
		&b,
		"- **Profile:** `%s` (`%s`, normalizer: `%s`)\n"+
			"- **Findings:** %d published (%d inline aligned, %d fallback, %d raw)\n"+
			"- **Tokens:** prompt=%d, cached=%d (%.1f%%), completion=%d, requests=%d+%d normalize\n"+
			"- **Latency:** %dms\n",
		res.Profile, res.Adapter, res.NormalizerPath,
		res.FindingsPublished, res.InlineAligned, res.FallbackCount, res.FindingsRaw,
		res.PromptTokens, res.CachedTokens, res.CacheHitRatio*100, res.CompletionTokens,
		res.Requests, res.NormalizeRequests, res.DurationMs,
	)
	if pub != nil {
		fmt.Fprintf(&b, "- **Published Review:** repo=%s pr=#%d review_id=%d comments=%d\n", pub.Repo, pub.PRNumber, pub.ReviewID, pub.CommentCount)
	}
	b.WriteByte('\n')
	if strings.TrimSpace(res.Pipeline.Summary) != "" {
		b.WriteString("## Summary\n\n")
		b.WriteString(strings.TrimSpace(res.Pipeline.Summary))
		b.WriteString("\n\n")
	}
	if len(res.Pipeline.Inlines) > 0 {
		b.WriteString("## Aligned Inline Comments\n\n")
		for _, ic := range res.Pipeline.Inlines {
			fmt.Fprintf(&b, "### `%s:%d` (%s, fp `%s`)\n\n%s\n\n", ic.File, ic.Line, ic.Side, ic.Fingerprint[:12], ic.Body)
		}
	}
	return b.String()
}

func renderScorecardTable(commits []localgit.CommitSpec, rows []ScorecardRow, runs []ExecutionMetrics) string {
	var shas []string
	for _, c := range commits {
		shas = append(shas, shortSHA(c.HeadSHA))
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Harness Comparison Scorecard (repo=%s, commits=%s)\n\n", commits[0].RepoID, strings.Join(shas, ","))

	tw := tabwriter.NewWriter(&b, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "PROFILE\tRUNS\tFINDINGS\tALIGNED_INLINE\tLATENCY_MS\tPROMPT_TOK\tCOMP_TOK\tCACHED_TOK\tCACHE_RATIO\tNORMALIZER")
	for _, r := range rows {
		fmt.Fprintf(
			tw,
			"%s\t%d\t%d (raw %d)\t%d\t%d\t%d\t%d\t%d\t%.1f%%\t%s\n",
			r.Profile,
			r.Runs,
			r.FindingsPublished,
			r.FindingsRaw,
			r.InlineAligned,
			r.AvgLatencyMs,
			r.PromptTokens,
			r.CompletionTokens,
			r.CachedTokens,
			r.CacheHitRatio*100,
			r.NormalizerPath,
		)
	}
	_ = tw.Flush()

	b.WriteString("\nAligned Findings Preview:\n")
	for _, run := range runs {
		if len(run.Pipeline.Inlines) == 0 {
			fmt.Fprintf(&b, "  [%s @ %s] (0 inline findings)\n", run.Profile, run.Commit)
			continue
		}
		for _, ic := range run.Pipeline.Inlines {
			firstLine, _, _ := strings.Cut(ic.Body, "\n")
			fmt.Fprintf(&b, "  [%s @ %s] %s:%d -> %s\n", run.Profile, run.Commit, ic.File, ic.Line, firstLine)
		}
	}
	return b.String()
}

func formatNormalizerPath(p reviewv1.NormalizerPath) string {
	switch p {
	case reviewv1.NormalizerPath_NORMALIZER_PATH_DETERMINISTIC:
		return "deterministic"
	case reviewv1.NormalizerPath_NORMALIZER_PATH_LLM:
		return "llm"
	default:
		return "none"
	}
}

func formatReportStatus(s reviewv1.ReportStatus) string {
	if s == reviewv1.ReportStatus_REPORT_STATUS_OK {
		return "ok"
	}
	return "harness_failed"
}

func shortSHA(sha string) string {
	if len(sha) > 8 {
		return sha[:8]
	}
	return sha
}

func randomHexID(nBytes int) string {
	buf := make([]byte, nBytes)
	_, _ = rand.Read(buf)
	return hex.EncodeToString(buf)
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func resolveGitHubRepo(ctx context.Context, explicit, repoDir string) (string, error) {
	if strings.TrimSpace(explicit) != "" {
		return validateOwnerRepo(explicit)
	}
	cmd := exec.CommandContext(ctx, "git", "-C", repoDir, "remote", "get-url", "origin")
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0", "GIT_OPTIONAL_LOCKS=0")
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("resolve origin remote for %q (or pass --github-repo): %w", repoDir, err)
	}
	return parseGitHubRemoteURL(strings.TrimSpace(string(out)))
}

func parseGitHubRemoteURL(rawURL string) (string, error) {
	trimmed := strings.TrimSpace(rawURL)
	if trimmed == "" {
		return "", errors.New("empty git origin remote URL")
	}
	if strings.Contains(trimmed, "://") {
		u, err := url.Parse(trimmed)
		if err != nil {
			return "", fmt.Errorf("parse remote URL %q: %w", rawURL, err)
		}
		return validateOwnerRepo(u.Path)
	}
	if _, after, ok := strings.Cut(trimmed, ":"); ok {
		return validateOwnerRepo(after)
	}
	return validateOwnerRepo(trimmed)
}

func validateOwnerRepo(raw string) (string, error) {
	cleaned := strings.Trim(strings.TrimSpace(raw), "/")
	cleaned = strings.TrimSuffix(cleaned, ".git")
	owner, name, ok := strings.Cut(cleaned, "/")
	if !ok || owner == "" || name == "" || strings.Contains(name, "/") {
		return "", fmt.Errorf("invalid GitHub repository slug %q (expected owner/repo)", raw)
	}
	return owner + "/" + name, nil
}

func resolveGitHubToken(ctx context.Context) (string, error) {
	if tok := strings.TrimSpace(os.Getenv("GITHUB_TOKEN")); tok != "" {
		return tok, nil
	}
	ghBin, err := exec.LookPath("gh")
	if err != nil {
		if info, statErr := os.Stat(fallbackGHScript); statErr == nil && !info.IsDir() {
			ghBin = fallbackGHScript
		}
	}
	if ghBin == "" {
		return "", nil
	}
	cmd := exec.CommandContext(ctx, ghBin, "auth", "token")
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	out, err := cmd.Output()
	if err != nil {
		return "", nil
	}
	return strings.TrimSpace(string(out)), nil
}
