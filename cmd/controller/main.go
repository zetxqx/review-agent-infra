package main

import (
	"context"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/zetxqx/review-agent-infra/internal/inference"
	"github.com/zetxqx/review-agent-infra/internal/orchestrator"
	"github.com/zetxqx/review-agent-infra/internal/sandbox"
	"github.com/zetxqx/review-agent-infra/internal/store"
	"github.com/zetxqx/review-agent-infra/internal/telemetry"
	ghvcs "github.com/zetxqx/review-agent-infra/internal/vcs/github"
	"github.com/zetxqx/review-agent-infra/pkg/contract"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const maxWebhookBodyBytes = 10 << 20

type controllerFlags struct {
	listenAddr           string
	sandboxListenAddr    string
	controllerSandboxURL string
	configPath           string
	workerID             string
	maxConcurrentRuns    int
	debounceSeconds      int
	leaseSeconds         int
	sandboxRunner        string
	sandboxNamespace     string
	runtimeClass         string
	kubeconfig           string
	dockerImage          string
	dockerNetwork        string
	upstreamKind         string
	upstreamURL          string
	upstreamKey          string
	upstreamModel        string
	objective            string
	modelAlias           string
	githubAppID          int64
	githubInstallID      int64
	githubKeyFile        string
	githubKeyPEM         string
	githubToken          string
	githubWebhookSec     string
	githubAPIURL         string
	reapIntervalSec      int
	feedbackPollSec      int
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	if err := runController(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func parseControllerFlags(args []string) (controllerFlags, error) {
	fs := flag.NewFlagSet("controller", flag.ContinueOnError)
	var f controllerFlags
	appIDDefault, err := envInt64("GITHUB_APP_ID")
	if err != nil {
		return f, err
	}
	installIDDefault, err := envInt64("GITHUB_INSTALLATION_ID")
	if err != nil {
		return f, err
	}
	fs.StringVar(&f.listenAddr, "listen-addr", envOrDefault("CONTROLLER_LISTEN_ADDR", ":8080"), "")
	fs.StringVar(&f.sandboxListenAddr, "sandbox-listen-addr", envOrDefault("CONTROLLER_SANDBOX_LISTEN_ADDR", ":8081"), "")
	fs.StringVar(&f.controllerSandboxURL, "controller-sandbox-url", envOrDefault("CONTROLLER_SANDBOX_URL", "http://review-controller-sandbox.review-system.svc.cluster.local:8081"), "")
	fs.StringVar(&f.configPath, "config", os.Getenv("CONTROLLER_CONFIG_PATH"), "")
	fs.StringVar(&f.workerID, "worker-id", envOrDefault("CONTROLLER_WORKER_ID", defaultWorkerID()), "")
	fs.IntVar(&f.maxConcurrentRuns, "max-concurrent-runs", 4, "")
	fs.IntVar(&f.debounceSeconds, "debounce-seconds", 20, "")
	fs.IntVar(&f.leaseSeconds, "lease-seconds", 90, "")
	fs.StringVar(&f.sandboxRunner, "sandbox-runner", envOrDefault("SANDBOX_RUNNER", "k8s"), "")
	fs.StringVar(&f.sandboxNamespace, "sandbox-namespace", envOrDefault("SANDBOX_NAMESPACE", "review-sandbox"), "")
	fs.StringVar(&f.runtimeClass, "runtime-class", envOrDefault("SANDBOX_RUNTIME_CLASS", "gvisor"), "")
	fs.StringVar(&f.kubeconfig, "kubeconfig", os.Getenv("KUBECONFIG"), "")
	fs.StringVar(&f.dockerImage, "docker-image", envOrDefault("SANDBOX_DOCKER_IMAGE", sandbox.DefaultSandboxImage), "")
	fs.StringVar(&f.dockerNetwork, "docker-network", envOrDefault("SANDBOX_DOCKER_NETWORK", "bridge"), "")
	fs.StringVar(&f.upstreamKind, "upstream-kind", envOrDefault("ADAPTER_UPSTREAM_KIND", "llmd-gateway"), "")
	fs.StringVar(&f.upstreamURL, "upstream-url", envOrDefault("ADAPTER_UPSTREAM_URL", "http://127.0.0.1:8000"), "")
	fs.StringVar(&f.upstreamKey, "upstream-api-key", os.Getenv("ADAPTER_UPSTREAM_API_KEY"), "")
	fs.StringVar(&f.upstreamModel, "upstream-model", envOrDefault("ADAPTER_UPSTREAM_MODEL", "Qwen/Qwen3-Coder-30B-A3B-Instruct"), "")
	fs.StringVar(&f.objective, "objective", envOrDefault("ADAPTER_LLMD_OBJECTIVE", "code-review-batch"), "")
	fs.StringVar(&f.modelAlias, "model-alias", envOrDefault("MODEL_ALIAS", "review-coder"), "")
	fs.Int64Var(&f.githubAppID, "github-app-id", appIDDefault, "")
	fs.Int64Var(&f.githubInstallID, "github-installation-id", installIDDefault, "")
	fs.StringVar(&f.githubKeyFile, "github-private-key-file", os.Getenv("GITHUB_PRIVATE_KEY_FILE"), "")
	fs.StringVar(&f.githubKeyPEM, "github-private-key-pem", os.Getenv("GITHUB_PRIVATE_KEY_PEM"), "")
	fs.StringVar(&f.githubToken, "github-token", os.Getenv("GITHUB_TOKEN"), "")
	fs.StringVar(&f.githubWebhookSec, "github-webhook-secret", os.Getenv("GITHUB_WEBHOOK_SECRET"), "")
	fs.StringVar(&f.githubAPIURL, "github-api-url", os.Getenv("GITHUB_API_URL"), "")
	fs.IntVar(&f.reapIntervalSec, "reap-interval-seconds", 300, "")
	fs.IntVar(&f.feedbackPollSec, "feedback-poll-seconds", 0, "")
	if err := fs.Parse(args); err != nil {
		return f, err
	}
	return f, nil
}

func buildUpstreamProvider(f controllerFlags) (contract.UpstreamProvider, error) {
	switch f.upstreamKind {
	case "llmd-gateway":
		return &inference.LlmdGatewayUpstream{
			BaseURL:   f.upstreamURL,
			APIKey:    f.upstreamKey,
			Objective: f.objective,
		}, nil
	case "vllm-direct", "ollama":
		return &inference.VllmDirectUpstream{
			BaseURL: f.upstreamURL,
			APIKey:  f.upstreamKey,
		}, nil
	default:
		return nil, fmt.Errorf("unknown --upstream-kind %q", f.upstreamKind)
	}
}

func runController(ctx context.Context, args []string) error {
	flags, err := parseControllerFlags(args)
	if err != nil {
		return err
	}
	if strings.TrimSpace(flags.githubWebhookSec) == "" {
		return errors.New("GitHub webhook secret is required (--github-webhook-secret or GITHUB_WEBHOOK_SECRET)")
	}
	upstream, err := buildUpstreamProvider(flags)
	if err != nil {
		return err
	}

	st := store.NewInMemoryStore()

	var privKey *rsa.PrivateKey
	if flags.githubKeyFile != "" {
		pemBytes, readErr := os.ReadFile(flags.githubKeyFile)
		if readErr != nil {
			return fmt.Errorf("read GitHub private key file: %w", readErr)
		}
		privKey, err = ghvcs.ParseRSAPrivateKeyPEM(pemBytes)
		if err != nil {
			return err
		}
	} else if flags.githubKeyPEM != "" {
		privKey, err = ghvcs.ParseRSAPrivateKeyPEM([]byte(flags.githubKeyPEM))
		if err != nil {
			return err
		}
	}

	vcs := ghvcs.NewPlugin(ghvcs.Config{
		AppID:          flags.githubAppID,
		InstallationID: flags.githubInstallID,
		PrivateKey:     privKey,
		StaticToken:    flags.githubToken,
		WebhookSecret:  []byte(flags.githubWebhookSec),
		APIBaseURL:     flags.githubAPIURL,
	})
	if flags.sandboxRunner == "k8s" || flags.githubAppID > 0 {
		checkCtx, cancelCheck := context.WithTimeout(ctx, 30*time.Second)
		checkErr := vcs.CheckCredentials(checkCtx)
		cancelCheck()
		if checkErr != nil {
			return fmt.Errorf("check GitHub credentials: %w", checkErr)
		}
	}

	sbxRunner, reaper, closeRunner, err := buildControllerSandbox(flags)
	if err != nil {
		return err
	}
	defer closeRunner()

	harnessCfg := orchestrator.DefaultHarnessConfig()
	if flags.configPath != "" {
		loaded, loadErr := loadHarnessConfigJSON(flags.configPath)
		if loadErr != nil {
			return loadErr
		}
		harnessCfg = loaded
	}

	tel := telemetry.NewRegistry()
	proxy := inference.NewProxyHandler(inference.Config{
		Upstream:      upstream,
		UpstreamModel: flags.upstreamModel,
		ModelAlias:    flags.modelAlias,
		Telemetry:     tel,
	})

	orch := orchestrator.New(orchestrator.Config{
		WorkerID:             flags.workerID,
		LeaseTTL:             time.Duration(flags.leaseSeconds) * time.Second,
		DebounceWindow:       time.Duration(flags.debounceSeconds) * time.Second,
		MaxConcurrentRuns:    flags.maxConcurrentRuns,
		ControllerSandboxURL: flags.controllerSandboxURL,
		ModelAlias:           flags.modelAlias,
		Harness:              harnessCfg,
	}, st, vcs, sbxRunner, proxy, tel)

	poller := orchestrator.NewFeedbackPoller(st, vcs, 30*24*time.Hour)

	publicSrv := newPublicServer(flags.listenAddr, NewControllerMux(vcs, orch, tel))
	sandboxSrv := newSandboxServer(flags.sandboxListenAddr, proxy.SandboxMux())

	var bgWG sync.WaitGroup
	bgWG.Add(1)
	go func() {
		defer bgWG.Done()
		orch.WorkerLoop(ctx)
	}()

	if reaper != nil && flags.reapIntervalSec > 0 {
		bgWG.Add(1)
		go func() {
			defer bgWG.Done()
			reaper.Run(ctx, time.Duration(flags.reapIntervalSec)*time.Second)
		}()
	}

	if flags.feedbackPollSec > 0 {
		bgWG.Add(1)
		go func() {
			defer bgWG.Done()
			poller.Run(ctx, time.Duration(flags.feedbackPollSec)*time.Second)
		}()
	}

	errCh := make(chan error, 2)
	go func() {
		if serveErr := publicSrv.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errCh <- serveErr
		}
	}()
	go func() {
		if serveErr := sandboxSrv.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errCh <- serveErr
		}
	}()

	select {
	case <-ctx.Done():
	case serveErr := <-errCh:
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = publicSrv.Shutdown(shutCtx)
		_ = sandboxSrv.Shutdown(shutCtx)
		cancel()
		return serveErr
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = publicSrv.Shutdown(shutCtx)
	_ = sandboxSrv.Shutdown(shutCtx)
	bgWG.Wait()
	return nil
}

func NewControllerMux(vcs contract.VCSPlugin, orch *orchestrator.Orchestrator, tel *telemetry.Registry) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"status":"ok"}`+"\n")
	})
	if tel != nil {
		mux.Handle("GET /metrics", tel.Handler())
	}
	mux.HandleFunc("POST /webhook/github", func(w http.ResponseWriter, r *http.Request) {
		handleGitHubWebhook(w, r, vcs, orch)
	})
	return mux
}

func handleGitHubWebhook(w http.ResponseWriter, r *http.Request, vcs contract.VCSPlugin, orch *orchestrator.Orchestrator) {
	r.Body = http.MaxBytesReader(w, r.Body, maxWebhookBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusRequestEntityTooLarge, webhookResponse{Status: "error", Reason: "body_too_large"})
		return
	}

	ev, err := vcs.VerifyAndParseWebhook(r.Context(), r.Header, body)
	if err != nil {
		if errors.Is(err, ghvcs.ErrInvalidWebhookSignature) {
			slog.Warn("webhook rejected", "reason", "invalid_signature")
			writeJSON(w, http.StatusUnauthorized, webhookResponse{Status: "unauthorized", Reason: "invalid_signature"})
			return
		}
		slog.Warn("webhook rejected", "reason", "bad_request", "error", err.Error())
		writeJSON(w, http.StatusBadRequest, webhookResponse{Status: "bad_request", Reason: err.Error()})
		return
	}

	res, err := orch.HandleEvent(r.Context(), ev)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, webhookResponse{Status: "error", Reason: err.Error()})
		return
	}

	resp := webhookResponse{
		Status:   res.Action,
		Allowed:  res.Allowed,
		Reason:   res.Reason,
		Enqueued: res.Enqueued,
	}
	if res.RunID.String() != "00000000-0000-0000-0000-000000000000" {
		resp.RunID = res.RunID.String()
	}

	code := http.StatusOK
	if !res.Allowed && (res.Reason == "maintainer_required" || res.Reason == "unknown_harness_profile") {
		code = http.StatusForbidden
	} else if !res.Allowed && strings.HasSuffix(res.Reason, "rate_limit") {
		code = http.StatusTooManyRequests
	} else if res.Enqueued {
		code = http.StatusAccepted
	}
	slog.Info("webhook", "event", ev.GetEventKind().String(), "repo", ev.GetRepo(), "pr", ev.GetPrNumber(), "trigger", ev.GetTriggerType().String(), "http_status", code, "action", res.Action, "allowed", res.Allowed, "reason", res.Reason, "run_id", resp.RunID)
	writeJSON(w, code, resp)
}

type webhookResponse struct {
	Status   string `json:"status"`
	Allowed  bool   `json:"allowed"`
	Reason   string `json:"reason,omitempty"`
	RunID    string `json:"run_id,omitempty"`
	Enqueued bool   `json:"enqueued"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func buildControllerSandbox(f controllerFlags) (contract.SandboxRunner, *sandbox.OrphanPodReaper, func(), error) {
	switch f.sandboxRunner {
	case "k8s":
		var restCfg *rest.Config
		var err error
		if f.kubeconfig != "" {
			restCfg, err = clientcmd.BuildConfigFromFlags("", f.kubeconfig)
		} else {
			restCfg, err = rest.InClusterConfig()
		}
		if err != nil {
			return nil, nil, nil, fmt.Errorf("load Kubernetes config: %w", err)
		}
		cs, err := kubernetes.NewForConfig(restCfg)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("create Kubernetes clientset: %w", err)
		}
		runner := sandbox.NewK8sSandboxRunner(cs, sandbox.K8sConfig{
			Namespace:        f.sandboxNamespace,
			RuntimeClassName: f.runtimeClass,
		})
		reaper := sandbox.NewOrphanPodReaper(cs, f.sandboxNamespace, 5*time.Minute)
		return runner, reaper, func() {}, nil
	case "docker":
		return sandbox.NewDockerRunner(f.dockerImage, f.dockerNetwork), nil, func() {}, nil
	case "local":
		r := sandbox.NewLocalProcessRunner("")
		return r, nil, func() { _ = r.Close() }, nil
	default:
		return nil, nil, nil, fmt.Errorf("unknown --sandbox-runner %q", f.sandboxRunner)
	}
}

func loadHarnessConfigJSON(path string) (orchestrator.HarnessConfig, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return orchestrator.HarnessConfig{}, err
	}
	cfg := orchestrator.DefaultHarnessConfig()
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return orchestrator.HarnessConfig{}, fmt.Errorf("decode harness config %s: %w", path, err)
	}
	for name, profile := range cfg.Profiles {
		if profile.Budget == nil {
			return orchestrator.HarnessConfig{}, fmt.Errorf("harness profile %q in %s has no budget", name, path)
		}
	}
	if _, ok := cfg.Profiles[cfg.Default]; !ok {
		return orchestrator.HarnessConfig{}, fmt.Errorf("default harness profile %q in %s is not defined", cfg.Default, path)
	}
	return cfg, nil
}

func defaultWorkerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		return "controller-0"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

func envInt64(name string) (int64, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return 0, nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	return v, nil
}

func envOrDefault(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func newPublicServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
}

func newSandboxServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
}
