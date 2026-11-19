package orchestrator

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"slices"
	"time"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
	"github.com/zetxqx/review-agent-infra/internal/publisher"
	"github.com/zetxqx/review-agent-infra/internal/sandbox"
)

type ProfileConfig struct {
	Name              string
	Adapter           string
	Image             string
	DefaultConfidence float64
	// key: harness adapter option key; value: option string value
	Options map[string]string
	Budget  *reviewv1.TaskBudget
}

type SplitWeight struct {
	Name   string
	Weight int
}

type RoutingRule struct {
	Repos  []string
	Split  []SplitWeight
	Shadow []string
}

type HarnessConfig struct {
	Default string
	// key: harness profile name; value: configured profile specification
	Profiles map[string]ProfileConfig
	Routing  []RoutingRule
}

type Config struct {
	WorkerID                string
	LeaseTTL                time.Duration
	PollInterval            time.Duration
	DebounceWindow          time.Duration
	RetryBackoff            time.Duration
	MaxConcurrentRuns       int
	MaxConcurrentShards     int
	MinConfidence           float64
	AdjacentLineWindow      int32
	TopN                    int
	SupportsFileComments    bool
	MaxCheckLogBytes        int
	AdapterSandboxURL       string
	AdapterInternalURL      string
	TokenKey                []byte
	ModelAlias              string
	RateLimitWindow         time.Duration
	MaxRunsPerRepoWindow    int
	MaxRunsPerAuthorWindow  int
	UntrustedMaxRequests    int32
	UntrustedMaxTotalTokens int64
	TrustedContributors     []string
	Now                     func() time.Time
	Harness                 HarnessConfig
}

func DefaultHarnessConfig() HarnessConfig {
	stdBudget := func(maxReq int32, maxTotal int64, wallSec int32) *reviewv1.TaskBudget {
		reserve := int64(20000)
		if maxTotal >= 5000000 {
			reserve = 50000
		}
		return &reviewv1.TaskBudget{
			MaxRequests:             maxReq,
			MaxContextTokens:        32768,
			MaxTotalTokens:          maxTotal,
			NormalizerReserveTokens: reserve,
			WallClockSeconds:        wallSec,
		}
	}
	return HarnessConfig{
		Default: "openhands",
		// key: harness profile name; value: configured profile specification
		Profiles: map[string]ProfileConfig{
			"openhands": {
				Name: "openhands", Adapter: "openhands", Image: sandbox.DefaultSandboxImage,
				DefaultConfidence: 0.80, Budget: stdBudget(60, 5000000, 900),
			},
			"pr-agent": {
				Name: "pr-agent", Adapter: "pr-agent", Image: sandbox.DefaultSandboxImage,
				DefaultConfidence: 0.80, Budget: stdBudget(8, 150000, 300),
			},
			"codex": {
				Name: "codex", Adapter: "codex", Image: sandbox.DefaultSandboxImage,
				DefaultConfidence: 0.80, Budget: stdBudget(60, 5000000, 900),
			},
			"mini-swe-agent": {
				Name: "mini-swe-agent", Adapter: "mini-swe-agent", Image: sandbox.DefaultSandboxImage,
				DefaultConfidence: 0.80, Budget: stdBudget(60, 5000000, 900),
			},
			"opencode": {
				Name: "opencode", Adapter: "opencode", Image: sandbox.DefaultSandboxImage,
				DefaultConfidence: 0.80, Budget: stdBudget(60, 5000000, 900),
			},
			"goose": {
				Name: "goose", Adapter: "goose", Image: sandbox.DefaultSandboxImage,
				DefaultConfidence: 0.80, Budget: stdBudget(60, 5000000, 900),
			},
			"aider": {
				Name: "aider", Adapter: "aider", Image: sandbox.DefaultSandboxImage,
				DefaultConfidence: 0.80, Budget: stdBudget(12, 300000, 300),
			},
			"single-shot": {
				Name: "single-shot", Adapter: "single-shot", Image: sandbox.DefaultSandboxImage,
				DefaultConfidence: 0.80, Budget: stdBudget(8, 150000, 180),
			},
			"mock-llm": {
				Name: "mock-llm", Adapter: "mock-llm", Image: sandbox.DefaultSandboxImage,
				DefaultConfidence: 0.80, Budget: stdBudget(20, 300000, 180),
			},
		},
	}
}

func normalizeConfig(cfg Config) Config {
	if cfg.WorkerID == "" {
		cfg.WorkerID = "worker-0"
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = 90 * time.Second
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}
	if cfg.DebounceWindow < 0 {
		cfg.DebounceWindow = 0
	}
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = 15 * time.Second
	}
	if cfg.MaxConcurrentRuns <= 0 {
		cfg.MaxConcurrentRuns = 4
	}
	if cfg.MaxConcurrentShards <= 0 {
		cfg.MaxConcurrentShards = 4
	}
	if cfg.MinConfidence <= 0 {
		cfg.MinConfidence = publisher.DefaultMinConfidence
	}
	if cfg.AdjacentLineWindow == 0 {
		cfg.AdjacentLineWindow = publisher.DefaultAdjacentLineWindow
	}
	if cfg.TopN <= 0 {
		cfg.TopN = publisher.DefaultTopN
	}
	if cfg.MaxCheckLogBytes <= 0 {
		cfg.MaxCheckLogBytes = 32 << 10
	}
	if cfg.ModelAlias == "" {
		cfg.ModelAlias = "review-coder"
	}
	if cfg.RateLimitWindow <= 0 {
		cfg.RateLimitWindow = 10 * time.Minute
	}
	if cfg.MaxRunsPerRepoWindow <= 0 {
		cfg.MaxRunsPerRepoWindow = 30
	}
	if cfg.MaxRunsPerAuthorWindow <= 0 {
		cfg.MaxRunsPerAuthorWindow = 10
	}
	if cfg.UntrustedMaxRequests <= 0 {
		cfg.UntrustedMaxRequests = 20
	}
	if cfg.UntrustedMaxTotalTokens <= 0 {
		cfg.UntrustedMaxTotalTokens = 500000
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if len(cfg.Harness.Profiles) == 0 {
		cfg.Harness = DefaultHarnessConfig()
	}
	if cfg.Harness.Default == "" {
		for k := range cfg.Harness.Profiles {
			cfg.Harness.Default = k
			break
		}
	}
	return cfg
}

func effectiveBudget(cfg Config, base *reviewv1.TaskBudget, isFork, isMaintainer bool) *reviewv1.TaskBudget {
	out := &reviewv1.TaskBudget{}
	if base != nil {
		*out = *base
	}
	if isFork && !isMaintainer {
		if cfg.UntrustedMaxRequests > 0 && (out.MaxRequests <= 0 || cfg.UntrustedMaxRequests < out.MaxRequests) {
			out.MaxRequests = cfg.UntrustedMaxRequests
		}
		if cfg.UntrustedMaxTotalTokens > 0 && (out.MaxTotalTokens <= 0 || cfg.UntrustedMaxTotalTokens < out.MaxTotalTokens) {
			out.MaxTotalTokens = cfg.UntrustedMaxTotalTokens
		}
		if out.MaxTotalTokens > 0 && out.NormalizerReserveTokens > out.MaxTotalTokens/2 {
			out.NormalizerReserveTokens = out.MaxTotalTokens / 2
		}
	}
	return out
}

func PickProfile(cfg HarnessConfig, repo string, prNumber int, requested *string) string {
	if requested != nil && *requested != "" {
		if _, ok := cfg.Profiles[*requested]; ok {
			return *requested
		}
	}
	for _, rule := range cfg.Routing {
		if !slices.Contains(rule.Repos, repo) && !slices.Contains(rule.Repos, "*") {
			continue
		}
		if len(rule.Split) == 0 {
			continue
		}
		sum := sha256.Sum256([]byte(fmt.Sprintf("%s#%d", repo, prNumber)))
		bucket := int(binary.BigEndian.Uint32(sum[:4]) % 100)
		edge := 0
		for _, entry := range rule.Split {
			edge += entry.Weight
			if bucket < edge {
				if _, ok := cfg.Profiles[entry.Name]; ok {
					return entry.Name
				}
			}
		}
	}
	return cfg.Default
}

func ShadowProfiles(cfg HarnessConfig, repo, primary string) []string {
	var out []string
	for _, rule := range cfg.Routing {
		if !slices.Contains(rule.Repos, repo) && !slices.Contains(rule.Repos, "*") {
			continue
		}
		for _, name := range rule.Shadow {
			if name == primary {
				continue
			}
			if _, ok := cfg.HarnessProfileExists(name); ok && !slices.Contains(out, name) {
				out = append(out, name)
			}
		}
	}
	return out
}

func (c HarnessConfig) HarnessProfileExists(name string) (ProfileConfig, bool) {
	p, ok := c.Profiles[name]
	return p, ok
}
