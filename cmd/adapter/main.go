package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zetxqx/review-agent-infra/internal/inference"
	"github.com/zetxqx/review-agent-infra/pkg/contract"
)

type adapterFlags struct {
	listenAddr    string
	internalAddr  string
	upstreamKind  string
	upstreamURL   string
	upstreamKey   string
	upstreamModel string
	modelAlias    string
	objective     string
	tokenKey      string
	tokenKeyHex   string
}

func envOrDefault(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func parseFlags(args []string) (adapterFlags, error) {
	fs := flag.NewFlagSet("adapter", flag.ContinueOnError)
	var cfg adapterFlags
	fs.StringVar(&cfg.listenAddr, "listen-addr", envOrDefault("ADAPTER_LISTEN_ADDR", ":8080"), "")
	fs.StringVar(&cfg.internalAddr, "internal-addr", envOrDefault("ADAPTER_INTERNAL_ADDR", ":9090"), "")
	fs.StringVar(&cfg.upstreamKind, "upstream-kind", envOrDefault("ADAPTER_UPSTREAM_KIND", "llmd-gateway"), "")
	fs.StringVar(&cfg.upstreamURL, "upstream-url", envOrDefault("ADAPTER_UPSTREAM_URL", "http://127.0.0.1:8000"), "")
	fs.StringVar(&cfg.upstreamKey, "upstream-api-key", os.Getenv("ADAPTER_UPSTREAM_API_KEY"), "")
	fs.StringVar(&cfg.upstreamModel, "upstream-model", envOrDefault("ADAPTER_UPSTREAM_MODEL", "Qwen/Qwen3-Coder-30B-A3B-Instruct"), "")
	fs.StringVar(&cfg.modelAlias, "model-alias", envOrDefault("ADAPTER_MODEL_ALIAS", "review-coder"), "")
	fs.StringVar(&cfg.objective, "objective", envOrDefault("ADAPTER_LLMD_OBJECTIVE", "code-review-batch"), "")
	fs.StringVar(&cfg.tokenKey, "token-key", os.Getenv("ADAPTER_TOKEN_KEY"), "")
	fs.StringVar(&cfg.tokenKeyHex, "token-key-hex", os.Getenv("ADAPTER_TOKEN_KEY_HEX"), "")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func resolveTokenKey(cfg adapterFlags) ([]byte, error) {
	if cfg.tokenKeyHex != "" {
		return hex.DecodeString(cfg.tokenKeyHex)
	}
	if cfg.tokenKey != "" {
		return []byte(cfg.tokenKey), nil
	}
	return nil, errors.New("token signing key must be set via --token-key or --token-key-hex")
}

func buildUpstream(cfg adapterFlags) (contract.UpstreamProvider, error) {
	switch cfg.upstreamKind {
	case "llmd-gateway":
		return &inference.LlmdGatewayUpstream{
			BaseURL:   cfg.upstreamURL,
			APIKey:    cfg.upstreamKey,
			Objective: cfg.objective,
		}, nil
	case "vllm-direct", "ollama":
		return &inference.VllmDirectUpstream{
			BaseURL: cfg.upstreamURL,
			APIKey:  cfg.upstreamKey,
		}, nil
	default:
		return nil, fmt.Errorf("unknown upstream-kind %q", cfg.upstreamKind)
	}
}

func run(ctx context.Context, args []string) error {
	cfg, err := parseFlags(args)
	if err != nil {
		return err
	}
	key, err := resolveTokenKey(cfg)
	if err != nil {
		return err
	}
	up, err := buildUpstream(cfg)
	if err != nil {
		return err
	}

	proxy := inference.NewProxyHandler(inference.Config{
		TokenKey:      key,
		Upstream:      up,
		UpstreamModel: cfg.upstreamModel,
		ModelAlias:    cfg.modelAlias,
	})

	sandboxSrv := &http.Server{
		Addr:              cfg.listenAddr,
		Handler:           proxy.SandboxMux(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	internalSrv := &http.Server{
		Addr:              cfg.internalAddr,
		Handler:           proxy.InternalMux(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 2)
	go func() {
		if err := sandboxSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	go func() {
		if err := internalSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		return err
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = sandboxSrv.Shutdown(shutCtx)
	_ = internalSrv.Shutdown(shutCtx)
	return nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
