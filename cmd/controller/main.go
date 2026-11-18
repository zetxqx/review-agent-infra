package main

import (
	"context"
	"crypto/rsa"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
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
	listenAddr         string
	databaseURL        string
	inMemoryStore      bool
	configPath         string
	workerID           string
	maxConcurrentRuns  int
	debounceSeconds    int
	leaseSeconds       int
	sandboxRunner      string
	sandboxNamespace   string
	loaderImage        string
	runtimeClass       string
	kubeconfig         string
	dockerImage        string
	dockerNetwork      string
	adapterSandboxURL  string
	adapterInternalURL string
	modelAlias         string
	tokenKey           string
	tokenKeyHex        string
	githubAppID        int64
	githubInstallID    int64
	githubKeyFile      string
	githubKeyPEM       string
	githubToken        string
	githubWebhookSec   string
	githubAPIURL       string
	reapIntervalSec    int
	feedbackPollSec    int
}

func main() {
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
	fs.StringVar(&f.listenAddr, "listen-addr", envOrDefault("CONTROLLER_LISTEN_ADDR", ":8080"), "")
	fs.StringVar(&f.databaseURL, "database-url", os.Getenv("DATABASE_URL"), "")
	fs.BoolVar(&f.inMemoryStore, "in-memory-store", os.Getenv("CONTROLLER_IN_MEMORY_STORE") == "true", "")
	fs.StringVar(&f.configPath, "config", os.Getenv("CONTROLLER_CONFIG_PATH"), "")
	fs.StringVar(&f.workerID, "worker-id", envOrDefault("CONTROLLER_WORKER_ID", defaultWorkerID()), "")
	fs.IntVar(&f.maxConcurrentRuns, "max-concurrent-runs", 4, "")
	fs.IntVar(&f.debounceSeconds, "debounce-seconds", 20, "")
	fs.IntVar(&f.leaseSeconds, "lease-seconds", 90, "")
	fs.StringVar(&f.sandboxRunner, "sandbox-runner", envOrDefault("SANDBOX_RUNNER", "k8s"), "")
	fs.StringVar(&f.sandboxNamespace, "sandbox-namespace", envOrDefault("SANDBOX_NAMESPACE", "review-sandbox"), "")
	fs.StringVar(&f.loaderImage, "loader-image", envOrDefault("SANDBOX_LOADER_IMAGE", "ghcr.io/zetxqx/review-loader:0.1.0"), "")
	fs.StringVar(&f.runtimeClass, "runtime-class", envOrDefault("SANDBOX_RUNTIME_CLASS", "gvisor"), "")
	fs.StringVar(&f.kubeconfig, "kubeconfig", os.Getenv("KUBECONFIG"), "")
	fs.StringVar(&f.dockerImage, "docker-image", envOrDefault("SANDBOX_DOCKER_IMAGE", sandbox.DefaultSandboxImage), "")
	fs.StringVar(&f.dockerNetwork, "docker-network", envOrDefault("SANDBOX_DOCKER_NETWORK", "bridge"), "")
	fs.StringVar(&f.adapterSandboxURL, "adapter-sandbox-url", envOrDefault("ADAPTER_SANDBOX_URL", "http://inference-adapter.review-system.svc.cluster.local:8080"), "")
	fs.StringVar(&f.adapterInternalURL, "adapter-internal-url", envOrDefault("ADAPTER_INTERNAL_URL", "http://inference-adapter.review-system.svc.cluster.local:9090"), "")
	fs.StringVar(&f.modelAlias, "model-alias", envOrDefault("MODEL_ALIAS", "review-coder"), "")
	fs.StringVar(&f.tokenKey, "token-key", os.Getenv("ADAPTER_TOKEN_KEY"), "")
	fs.StringVar(&f.tokenKeyHex, "token-key-hex", os.Getenv("ADAPTER_TOKEN_KEY_HEX"), "")
	fs.Int64Var(&f.githubAppID, "github-app-id", 0, "")
	fs.Int64Var(&f.githubInstallID, "github-installation-id", 0, "")
	fs.StringVar(&f.githubKeyFile, "github-private-key-file", os.Getenv("GITHUB_PRIVATE_KEY_FILE"), "")
	fs.StringVar(&f.githubKeyPEM, "github-private-key-pem", os.Getenv("GITHUB_PRIVATE_KEY_PEM"), "")
	fs.StringVar(&f.githubToken, "github-token", os.Getenv("GITHUB_TOKEN"), "")
	fs.StringVar(&f.githubWebhookSec, "github-webhook-secret", os.Getenv("GITHUB_WEBHOOK_SECRET"), "")
	fs.StringVar(&f.githubAPIURL, "github-api-url", os.Getenv("GITHUB_API_URL"), "")
	fs.IntVar(&f.reapIntervalSec, "reap-interval-seconds", 300, "")
	fs.IntVar(&f.feedbackPollSec, "feedback-poll-seconds", 900, "")
	if err := fs.Parse(args); err != nil {
		return f, err
	}
	return f, nil
}

func runController(ctx context.Context, args []string) error {
	flags, err := parseControllerFlags(args)
	if err != nil {
		return err
	}
	tokKey, err := resolveSigningKey(flags.tokenKey, flags.tokenKeyHex)
	if err != nil {
		return err
	}
	if strings.TrimSpace(flags.githubWebhookSec) == "" {
		return errors.New("GitHub webhook secret is required (--github-webhook-secret or GITHUB_WEBHOOK_SECRET)")
	}

	var st store.Store
	var closeStore func()
	if flags.inMemoryStore {
		st = store.NewInMemoryStore()
		closeStore = func() {}
	} else {
		if flags.databaseURL == "" {
			return errors.New("PostgreSQL DSN is required (--database-url or DATABASE_URL) unless --in-memory-store is set")
		}
		pool, poolErr := pgxpool.New(ctx, flags.databaseURL)
		if poolErr != nil {
			return fmt.Errorf("connect PostgreSQL: %w", poolErr)
		}
		pgSt := store.NewPGStore(pool)
		if migErr := pgSt.Migrate(ctx); migErr != nil {
			pool.Close()
			return fmt.Errorf("apply PostgreSQL migrations: %w", migErr)
		}
		st = pgSt
		closeStore = pool.Close
	}
	defer closeStore()

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
	orch := orchestrator.New(orchestrator.Config{
		WorkerID:           flags.workerID,
		LeaseTTL:           time.Duration(flags.leaseSeconds) * time.Second,
		DebounceWindow:     time.Duration(flags.debounceSeconds) * time.Second,
		MaxConcurrentRuns:  flags.maxConcurrentRuns,
		AdapterSandboxURL:  flags.adapterSandboxURL,
		AdapterInternalURL: flags.adapterInternalURL,
		TokenKey:           tokKey,
		ModelAlias:         flags.modelAlias,
		Harness:            harnessCfg,
	}, st, vcs, sbxRunner, tel)

	poller := orchestrator.NewFeedbackPoller(st, vcs, 30*24*time.Hour)

	mux := NewControllerMux(vcs, orch, tel)
	srv := &http.Server{
		Addr:              flags.listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

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

	errCh := make(chan error, 1)
	go func() {
		if serveErr := srv.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errCh <- serveErr
		}
	}()

	select {
	case <-ctx.Done():
	case serveErr := <-errCh:
		return serveErr
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)
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
			writeJSON(w, http.StatusUnauthorized, webhookResponse{Status: "unauthorized", Reason: "invalid_signature"})
			return
		}
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
		runner := sandbox.NewK8sSandboxRunner(cs, restCfg, sandbox.K8sConfig{
			Namespace:        f.sandboxNamespace,
			LoaderImage:      f.loaderImage,
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
	return cfg, nil
}

func resolveSigningKey(rawKey, hexKey string) ([]byte, error) {
	if hexKey != "" {
		return hex.DecodeString(hexKey)
	}
	if rawKey != "" {
		return []byte(rawKey), nil
	}
	return nil, errors.New("token signing key must be set via --token-key or --token-key-hex")
}

func defaultWorkerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		return "controller-0"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

func envOrDefault(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}
