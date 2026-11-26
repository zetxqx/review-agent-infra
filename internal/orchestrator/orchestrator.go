package orchestrator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
	"github.com/zetxqx/review-agent-infra/internal/loader"
	"github.com/zetxqx/review-agent-infra/internal/publisher"
	"github.com/zetxqx/review-agent-infra/internal/sandbox"
	"github.com/zetxqx/review-agent-infra/internal/store"
	"github.com/zetxqx/review-agent-infra/internal/telemetry"
	"github.com/zetxqx/review-agent-infra/pkg/contract"
	"google.golang.org/protobuf/encoding/protojson"
)

var errLeaseLost = errors.New("run lease superseded or lost")

type SandboxSessionBroker interface {
	RegisterSession(claims *reviewv1.TaskClaims, bundlePath string) (taskToken string, unregister func(), err error)
	FinishSession(taskToken string) (reportBytes []byte, usage *reviewv1.SandboxUsage)
}

type inFlightEntry struct {
	runID   uuid.UUID
	headSHA string
	cancel  context.CancelFunc
}

type shardOutcome struct {
	shardIndex int
	shardName  string
	report     *reviewv1.ReviewReport
	rawToken   string
	usage      *reviewv1.SandboxUsage
	duration   time.Duration
	err        error
}

type Orchestrator struct {
	cfg              Config
	store            store.Store
	vcs              contract.VCSPlugin
	sandbox          contract.SandboxRunner
	broker           SandboxSessionBroker
	contextProviders []contract.ContextProvider
	telemetry        *telemetry.Registry
	limiter          *SlidingWindowLimiter
	wakeCh           chan struct{}

	activeMu   sync.Mutex
	activeRuns map[prGroupKey][]inFlightEntry
}

func New(
	cfg Config,
	st store.Store,
	vcs contract.VCSPlugin,
	sbx contract.SandboxRunner,
	broker SandboxSessionBroker,
	tel *telemetry.Registry,
	providers ...contract.ContextProvider,
) *Orchestrator {
	cfg = normalizeConfig(cfg)
	if tel == nil {
		tel = telemetry.NewRegistry()
	}
	return &Orchestrator{
		cfg:              cfg,
		store:            st,
		vcs:              vcs,
		sandbox:          sbx,
		broker:           broker,
		contextProviders: providers,
		telemetry:        tel,
		limiter:          NewSlidingWindowLimiter(),
		wakeCh:           make(chan struct{}, 1),
		activeRuns:       make(map[prGroupKey][]inFlightEntry),
	}
}

func (o *Orchestrator) Telemetry() *telemetry.Registry {
	return o.telemetry
}

func (o *Orchestrator) WorkerLoop(ctx context.Context) {
	sem := make(chan struct{}, o.cfg.MaxConcurrentRuns)
	var wg sync.WaitGroup
	ticker := time.NewTicker(o.cfg.PollInterval)
	defer ticker.Stop()

	for {
		for {
			select {
			case <-ctx.Done():
				wg.Wait()
				return
			default:
			}

			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				wg.Wait()
				return
			}

			rec, err := o.store.ClaimNextRun(ctx, o.cfg.WorkerID, o.cfg.LeaseTTL)
			if err != nil || rec == nil {
				<-sem
				break
			}

			wg.Add(1)
			go func(claimed *store.RunRecord) {
				defer wg.Done()
				defer func() { <-sem }()
				o.processRun(ctx, claimed)
			}(rec)
		}

		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case <-o.wakeCh:
		case <-ticker.C:
		}
	}
}

func (o *Orchestrator) RunOnce(ctx context.Context) (bool, error) {
	rec, err := o.store.ClaimNextRun(ctx, o.cfg.WorkerID, o.cfg.LeaseTTL)
	if err != nil || rec == nil {
		return false, err
	}
	o.processRun(ctx, rec)
	return true, nil
}

func (o *Orchestrator) processRun(parentCtx context.Context, rec *store.RunRecord) {
	runCtx, runCancel := context.WithCancel(parentCtx)
	defer runCancel()

	o.registerInFlight(rec, runCancel)
	defer o.unregisterInFlight(rec)

	go o.keepLeaseAlive(runCtx, runCancel, rec.ID)

	slog.Info("run claimed", "run_id", rec.ID.String(), "repo", rec.Repo, "pr", rec.PRNumber, "head_sha", rec.HeadSHA, "attempt", rec.Attempt, "trigger", rec.TriggerType)
	started := time.Now()
	err := o.executeReview(runCtx, rec)
	elapsed := time.Since(started)
	if err != nil {
		slog.Error("run failed", "run_id", rec.ID.String(), "duration_ms", elapsed.Milliseconds(), "error", err.Error())
	} else {
		slog.Info("run finished", "run_id", rec.ID.String(), "duration_ms", elapsed.Milliseconds())
	}

	profileName := o.cfg.Harness.Default
	if rec.HarnessProfile != nil && *rec.HarnessProfile != "" {
		profileName = *rec.HarnessProfile
	}
	o.telemetry.ObserveRunDuration(rec.Repo, profileName, elapsed.Seconds())

	if err != nil {
		o.handleRunFailure(parentCtx, runCtx, rec, profileName, err)
	}
}

func (o *Orchestrator) keepLeaseAlive(runCtx context.Context, runCancel context.CancelFunc, runID uuid.UUID) {
	interval := max(25*time.Millisecond, o.cfg.LeaseTTL/3)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	lastOK := time.Now()

	for {
		select {
		case <-runCtx.Done():
			return
		case <-ticker.C:
			dbCtx, cancel := context.WithTimeout(runCtx, 5*time.Second)
			alive, err := o.store.RenewLease(dbCtx, runID, o.cfg.WorkerID, o.cfg.LeaseTTL)
			cancel()
			if err != nil {
				if time.Since(lastOK) > o.cfg.LeaseTTL {
					runCancel()
					return
				}
				continue
			}
			if !alive {
				runCancel()
				return
			}
			lastOK = time.Now()
		}
	}
}

func (o *Orchestrator) executeReview(ctx context.Context, rec *store.RunRecord) error {
	profileName := PickProfile(o.cfg.Harness, rec.Repo, rec.PRNumber, rec.RequestedHarness)
	if rec.HarnessProfile != nil && *rec.HarnessProfile != "" {
		profileName = *rec.HarnessProfile
	}
	ok, err := o.store.SetHarnessProfile(ctx, rec.ID, o.cfg.WorkerID, profileName)
	if err != nil {
		return err
	}
	if !ok {
		return errLeaseLost
	}
	rec.HarnessProfile = &profileName

	profile, exists := o.cfg.Harness.Profiles[profileName]
	if !exists {
		return fmt.Errorf("configured harness profile %q not found", profileName)
	}

	diffPatch, tarStream, err := o.vcs.FetchDiffAndTarball(ctx, rec.Repo, rec.PRNumber, rec.BaseSHA, rec.HeadSHA)
	if err != nil {
		return err
	}
	snapDir, err := os.MkdirTemp("", "orch-snap-*")
	if err != nil {
		_ = tarStream.Close()
		return err
	}
	defer os.RemoveAll(snapDir)

	tarPath := filepath.Join(snapDir, "snapshot.tar.gz")
	tarFile, err := os.Create(tarPath)
	if err != nil {
		_ = tarStream.Close()
		return err
	}
	_, copyErr := io.Copy(tarFile, tarStream)
	_ = tarStream.Close()
	_ = tarFile.Close()
	if copyErr != nil {
		return copyErr
	}

	parsedFullDiff := publisher.ParseUnifiedDiff(diffPatch)
	baseRules, err := o.vcs.FetchBaseRules(ctx, rec.Repo, rec.BaseSHA, parsedFullDiff.Files())
	if err != nil {
		return err
	}

	// key: shared context file path relative to /workspace/.review/in; value: raw file bytes
	sharedInputs := make(map[string][]byte)
	for relPath, content := range baseRules {
		sharedInputs["rules/"+relPath] = content
	}
	var ciLogsPath *string
	if logFetcher, hasLogs := o.vcs.(contract.CheckLogFetcher); hasLogs && o.cfg.MaxCheckLogBytes > 0 {
		if logs, logErr := logFetcher.FetchCheckLogs(ctx, rec.Repo, rec.PRNumber, rec.HeadSHA, o.cfg.MaxCheckLogBytes); logErr == nil && len(logs) > 0 {
			sharedInputs["ci_failure.log"] = logs
			inContainerPath := "/workspace/.review/in/ci_failure.log"
			ciLogsPath = &inContainerPath
		}
	}

	contractRec := &contract.RunRecord{
		ID:             rec.ID.String(),
		Repo:           rec.Repo,
		PRNumber:       rec.PRNumber,
		BaseSHA:        rec.BaseSHA,
		HeadSHA:        rec.HeadSHA,
		TriggerType:    rec.TriggerType,
		HarnessProfile: profileName,
	}
	for _, cp := range o.contextProviders {
		if err := cp.Contribute(ctx, contractRec, sharedInputs); err != nil {
			return err
		}
	}

	shards := SplitDiffIntoShards(diffPatch, int(profile.Budget.MaxContextTokens)/3)
	if len(shards) == 0 {
		doneOK, doneErr := o.store.MarkDoneIfActive(ctx, rec.ID, o.cfg.WorkerID)
		if doneErr != nil {
			return doneErr
		}
		if !doneOK {
			return errLeaseLost
		}
		o.telemetry.IncRun(rec.Repo, profileName, store.StatusDone)
		return nil
	}

	sharded := len(shards) > 1
	shadows := ShadowProfiles(o.cfg.Harness, rec.Repo, profileName)
	primaryOutcomes := make([]shardOutcome, len(shards))

	shardSem := make(chan struct{}, o.cfg.MaxConcurrentShards)
	var shardWG sync.WaitGroup

	for i, sh := range shards {
		idx, shard := i, sh
		shardWG.Add(1)
		go func() {
			defer shardWG.Done()
			select {
			case shardSem <- struct{}{}:
			case <-ctx.Done():
				primaryOutcomes[idx] = shardOutcome{shardIndex: idx, shardName: shard.Name, err: ctx.Err()}
				return
			}
			defer func() { <-shardSem }()

			primaryOutcomes[idx] = o.runShard(ctx, rec, profile, "primary", tarPath, sharedInputs, ciLogsPath, idx, shard, sharded)
		}()

		for _, sName := range shadows {
			shadowProf := o.cfg.Harness.Profiles[sName]
			shardWG.Add(1)
			go func(sp ProfileConfig) {
				defer shardWG.Done()
				select {
				case shardSem <- struct{}{}:
				case <-ctx.Done():
					return
				}
				defer func() { <-shardSem }()

				out := o.runShard(ctx, rec, sp, "shadow", tarPath, sharedInputs, ciLogsPath, idx, shard, sharded)
				o.recordExecutionRow(ctx, rec, sp.Name, "shadow", out, 0)
			}(shadowProf)
		}
	}
	shardWG.Wait()

	if ctx.Err() != nil {
		return ctx.Err()
	}

	return o.denoiseAndPublish(ctx, rec, profile, primaryOutcomes, diffPatch)
}

func (o *Orchestrator) runShard(
	ctx context.Context,
	rec *store.RunRecord,
	profile ProfileConfig,
	role string,
	tarPath string,
	// key: shared context file path relative to /workspace/.review/in; value: raw file bytes
	sharedInputs map[string][]byte,
	ciLogsPath *string,
	shardIdx int,
	shard Shard,
	sharded bool,
) shardOutcome {
	if o.broker == nil {
		return shardOutcome{shardIndex: shardIdx, shardName: shard.Name, err: errors.New("sandbox session broker is not configured")}
	}
	jti := randomTokenHex(12)
	runIDStr := rec.ID.String()
	sessionID := fmt.Sprintf("%s-s%d", runIDStr, shardIdx)
	if role == "shadow" {
		sessionID = fmt.Sprintf("%s-s%d-%s", runIDStr, shardIdx, profile.Name)
	}
	var parentSession *string
	if sharded {
		parentSession = &runIDStr
	}

	claims := &reviewv1.TaskClaims{
		RunId:                   runIDStr,
		SessionId:               sessionID,
		ParentSessionId:         parentSession,
		Repo:                    rec.Repo,
		HarnessProfile:          profile.Name,
		MaxRequests:             profile.Budget.MaxRequests,
		MaxTotalTokens:          profile.Budget.MaxTotalTokens,
		NormalizerReserveTokens: profile.Budget.NormalizerReserveTokens,
		MaxOutputTokens:         32768,
		Exp:                     o.cfg.Now().Add(time.Duration(profile.Budget.WallClockSeconds+600) * time.Second).Unix(),
		Jti:                     jti,
	}

	task := &reviewv1.AgentTask{
		RunId:      runIDStr,
		ShardIndex: int32(shardIdx),
		Harness: &reviewv1.HarnessRef{
			Profile: profile.Name,
			Adapter: profile.Adapter,
			Options: profile.Options,
		},
		ModelAlias: o.cfg.ModelAlias,
		PrTitle:    rec.PRTitle,
		PrBody:     rec.PRBody,
		BaseBranch: shortCommit(rec.BaseSHA),
		CiLogsFile: ciLogsPath,
		Budget:     profile.Budget,
	}
	taskBytes, err := (protojson.MarshalOptions{Indent: "  "}).Marshal(task)
	if err != nil {
		return shardOutcome{shardIndex: shardIdx, shardName: shard.Name, err: err}
	}

	// key: file path relative to /workspace/.review/in; value: raw file bytes
	inputs := make(map[string][]byte, len(sharedInputs)+2)
	for k, v := range sharedInputs {
		inputs[k] = v
	}
	inputs["task.json"] = append(taskBytes, '\n')
	inputs["diff.patch"] = shard.DiffPatch

	bundlePath := filepath.Join(filepath.Dir(tarPath), fmt.Sprintf("bundle-s%d-%s-%s.tar.gz", shardIdx, role, jti))
	bundleFile, err := os.Create(bundlePath)
	if err != nil {
		return shardOutcome{shardIndex: shardIdx, shardName: shard.Name, err: err}
	}
	buildErr := loader.BuildBundleTGZ(tarPath, inputs, bundleFile)
	closeErr := bundleFile.Close()
	if buildErr != nil {
		_ = os.Remove(bundlePath)
		return shardOutcome{shardIndex: shardIdx, shardName: shard.Name, err: buildErr}
	}
	if closeErr != nil {
		_ = os.Remove(bundlePath)
		return shardOutcome{shardIndex: shardIdx, shardName: shard.Name, err: closeErr}
	}
	defer os.Remove(bundlePath)

	rawToken, unregister, err := o.broker.RegisterSession(claims, bundlePath)
	if err != nil {
		return shardOutcome{shardIndex: shardIdx, shardName: shard.Name, err: err}
	}
	defer unregister()

	shortRun := strings.ReplaceAll(runIDStr, "-", "")
	if len(shortRun) > 8 {
		shortRun = shortRun[:8]
	}

	resolvedControllerURL, err := sandbox.ResolveSandboxURL(ctx, o.cfg.ControllerSandboxURL)
	if err != nil {
		return shardOutcome{shardIndex: shardIdx, shardName: shard.Name, err: err}
	}

	started := time.Now()
	runErr := o.sandbox.Run(ctx, contract.SandboxSpec{
		ShortRunID:     shortRun,
		ShardIndex:     shardIdx,
		Attempt:        rec.Attempt,
		Role:           role,
		HarnessProfile: profile.Name,
		HarnessImage:   profile.Image,
		ControllerURL:  resolvedControllerURL,
		TaskToken:      rawToken,
		TimeoutSeconds: int(profile.Budget.WallClockSeconds),
	})
	elapsed := time.Since(started)

	reportBytes, usage := o.broker.FinishSession(rawToken)
	var report *reviewv1.ReviewReport
	if runErr == nil {
		decoded, decErr := contract.DecodeAndValidateReport(reportBytes)
		if decErr != nil {
			runErr = decErr
		} else {
			report = decoded
		}
	}
	contract.FailReportOnRejectedRequests(report, usage)
	logShardFinished(runIDStr, shardIdx, role, profile.Name, elapsed, report, usage, runErr)

	return shardOutcome{
		shardIndex: shardIdx,
		shardName:  shard.Name,
		report:     report,
		rawToken:   rawToken,
		usage:      usage,
		duration:   elapsed,
		err:        runErr,
	}
}

func (o *Orchestrator) denoiseAndPublish(
	ctx context.Context,
	rec *store.RunRecord,
	profile ProfileConfig,
	outcomes []shardOutcome,
	fullDiffPatch []byte,
) error {
	diffIdx := publisher.ParseUnifiedDiff(fullDiffPatch)

	type shardFinding struct {
		shardIdx int
		finding  *reviewv1.ReviewFinding
	}

	var candidates []shardFinding
	var failedShards []string
	var mintedTokens []string
	var summaries []string

	for _, out := range outcomes {
		if out.rawToken != "" {
			mintedTokens = append(mintedTokens, out.rawToken)
		}
		if out.err != nil || out.report == nil || out.report.GetProvenance().GetStatus() != reviewv1.ReportStatus_REPORT_STATUS_OK {
			failedShards = append(failedShards, out.shardName)
			continue
		}
		contract.ValidateReport(out.report)
		if trimmed := strings.TrimSpace(out.report.Summary); trimmed != "" {
			summaries = append(summaries, trimmed)
		}
		for _, f := range out.report.Findings {
			if diffIdx.ContainsFile(f.File) {
				candidates = append(candidates, shardFinding{shardIdx: out.shardIndex, finding: f})
			}
		}
	}

	if len(failedShards) == len(outcomes) {
		for _, out := range outcomes {
			o.recordExecutionRow(ctx, rec, profile.Name, "primary", out, 0)
		}
		return errors.New("all primary review shards failed or produced invalid reports")
	}

	pubOK, err := o.store.MarkPublishing(ctx, rec.ID, o.cfg.WorkerID)
	if err != nil {
		return err
	}
	if !pubOK {
		return errLeaseLost
	}

	type placedWithShard struct {
		shardIdx int
		aligned  publisher.AlignedFinding
	}
	var confidentItems []placedWithShard
	var candidateFPs []string
	for _, cf := range candidates {
		eff := publisher.EffectiveConfidence(cf.finding, profile.DefaultConfidence)
		if eff < o.cfg.MinConfidence {
			continue
		}
		p := publisher.Place(cf.finding, diffIdx, o.cfg.SupportsFileComments)
		fp := publisher.Fingerprint(cf.finding, p, diffIdx)
		confidentItems = append(confidentItems, placedWithShard{
			shardIdx: cf.shardIdx,
			aligned: publisher.AlignedFinding{
				Finding:             cf.finding,
				Placement:           p,
				Fingerprint:         fp,
				EffectiveConfidence: eff,
			},
		})
		candidateFPs = append(candidateFPs, fp)
	}

	unseenFPs, err := o.store.FilterUnseenFingerprints(ctx, rec.Repo, rec.PRNumber, candidateFPs)
	if err != nil {
		return err
	}
	// key: unseen SHA-256 fingerprint; value: true when not yet published on this PR
	unseenSet := make(map[string]bool, len(unseenFPs))
	for _, fp := range unseenFPs {
		unseenSet[fp] = true
	}
	if remoteFPs, remoteErr := o.vcs.ListPublishedFingerprints(ctx, rec.Repo, rec.PRNumber); remoteErr == nil {
		for _, fp := range remoteFPs {
			delete(unseenSet, fp)
		}
	}

	// key: finding pointer identity key (fingerprint); value: originating shard index
	shardByFP := make(map[string]int, len(confidentItems))
	var unseenAligned []publisher.AlignedFinding
	for _, item := range confidentItems {
		if !unseenSet[item.aligned.Fingerprint] {
			continue
		}
		if _, exists := shardByFP[item.aligned.Fingerprint]; !exists {
			shardByFP[item.aligned.Fingerprint] = item.shardIdx
		}
		unseenAligned = append(unseenAligned, item.aligned)
	}

	finalItems := publisher.ClusterAndCap(unseenAligned, o.cfg.AdjacentLineWindow, o.cfg.TopN)

	publishedPerShard := make(map[int]int)
	for _, it := range finalItems {
		publishedPerShard[shardByFP[it.Fingerprint]]++
	}
	for _, out := range outcomes {
		o.recordExecutionRow(ctx, rec, profile.Name, "primary", out, publishedPerShard[out.shardIndex])
	}

	if rec.TriggerType == "auto" && len(finalItems) == 0 && len(failedShards) == 0 {
		slog.Info("review not published", "run_id", rec.ID.String(), "reason", "no_findings")
		doneOK, doneErr := o.store.MarkDoneIfActive(ctx, rec.ID, o.cfg.WorkerID)
		if doneErr != nil {
			return doneErr
		}
		if !doneOK {
			return errLeaseLost
		}
		o.telemetry.IncRun(rec.Repo, profile.Name, store.StatusDone)
		return nil
	}

	pubReq := o.buildSanitizedPublishRequest(rec, summaries, finalItems, failedShards, mintedTokens)

	liveSHA, err := o.vcs.GetHeadSHA(ctx, rec.Repo, rec.PRNumber)
	if err != nil {
		return err
	}
	if liveSHA != rec.HeadSHA {
		slog.Info("review not published", "run_id", rec.ID.String(), "reason", "head_moved", "live_head_sha", liveSHA)
		_ = o.store.MarkSuperseded(ctx, rec.ID)
		o.telemetry.IncRun(rec.Repo, profile.Name, store.StatusSuperseded)
		return nil
	}

	pubRes, err := o.vcs.PublishReview(ctx, pubReq)
	if err != nil {
		return err
	}
	if pubRes != nil {
		slog.Info("review published", "run_id", rec.ID.String(), "repo", rec.Repo, "pr", rec.PRNumber, "review_id", pubRes.ReviewID, "comments", len(pubRes.Comments), "findings", len(finalItems))
	}

	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()

	// key: finding fingerprint; value: posted GitHub review comment ID
	commentIDByFP := make(map[string]int64)
	if pubRes != nil {
		for _, c := range pubRes.Comments {
			if c.Fingerprint != "" {
				commentIDByFP[c.Fingerprint] = c.CommentID
			}
		}
	}

	publishedItems := make([]store.PublishedItem, 0, len(finalItems))
	for _, it := range finalItems {
		publishedItems = append(publishedItems, store.PublishedItem{
			Fingerprint: it.Fingerprint,
			Category:    it.Finding.GetCategory().String(),
			CommentID:   commentIDByFP[it.Fingerprint],
		})
	}
	if len(publishedItems) > 0 {
		if err := o.store.SavePublishedFingerprints(saveCtx, rec.Repo, rec.PRNumber, rec.ID, profile.Name, publishedItems); err != nil {
			return err
		}
	}
	if _, err := o.store.MarkDoneIfActive(saveCtx, rec.ID, o.cfg.WorkerID); err != nil {
		return err
	}
	o.telemetry.IncRun(rec.Repo, profile.Name, store.StatusDone)
	return nil
}

func (o *Orchestrator) buildSanitizedPublishRequest(
	rec *store.RunRecord,
	rawSummaries []string,
	items []publisher.AlignedFinding,
	failedShards []string,
	mintedTokens []string,
) contract.PublishRequest {
	var summaryParts []string
	if len(rawSummaries) > 0 {
		summaryParts = append(summaryParts, publisher.SanitizeMarkdown(strings.Join(rawSummaries, "\n\n"), mintedTokens...))
	}

	var inlines []contract.InlineComment
	var fileComments []contract.InlineComment
	var fallbackSummary []string
	fingerprints := make([]string, 0, len(items))

	for _, it := range items {
		if it.Fingerprint != "" {
			fingerprints = append(fingerprints, it.Fingerprint)
		}
		f := it.Finding
		f.Title = publisher.SanitizeMarkdown(f.Title, mintedTokens...)
		f.Explanation = publisher.SanitizeMarkdown(f.Explanation, mintedTokens...)
		f.Suggestion = publisher.SanitizeMarkdown(f.Suggestion, mintedTokens...)
		f.Evidence = publisher.SanitizeMarkdown(f.Evidence, mintedTokens...)

		body := publisher.FormatFindingMarkdown(f)
		sideStr := "RIGHT"
		if it.Placement.Side == reviewv1.DiffSide_DIFF_SIDE_LEFT {
			sideStr = "LEFT"
		}
		switch it.Placement.Kind {
		case publisher.PlacementInline:
			inlines = append(inlines, contract.InlineComment{
				Fingerprint: it.Fingerprint,
				Category:    f.Category.String(),
				File:        f.File,
				Line:        it.Placement.Line,
				Side:        sideStr,
				Body:        body,
			})
		case publisher.PlacementFile:
			fileComments = append(fileComments, contract.InlineComment{
				Fingerprint: it.Fingerprint,
				Category:    f.Category.String(),
				File:        f.File,
				Line:        0,
				Side:        sideStr,
				Body:        body,
			})
		default:
			loc := publisher.SanitizeMarkdown(f.File, mintedTokens...)
			if f.Line > 0 {
				loc = fmt.Sprintf("%s:%d", loc, f.Line)
			}
			fallbackSummary = append(fallbackSummary, fmt.Sprintf("### `%s`\n%s", loc, body))
		}
	}

	if len(fallbackSummary) > 0 {
		summaryParts = append(summaryParts, strings.Join(fallbackSummary, "\n\n"))
	}
	if len(failedShards) > 0 {
		summaryParts = append(summaryParts, fmt.Sprintf("Note: partial review result; failed shards: `%s`.", strings.Join(failedShards, "`, `")))
	}
	if len(summaryParts) == 0 && len(inlines) == 0 && len(fileComments) == 0 {
		summaryParts = append(summaryParts, "No actionable defects found.")
	}

	return contract.PublishRequest{
		Repo:         rec.Repo,
		PRNumber:     rec.PRNumber,
		CommitID:     rec.HeadSHA,
		RunID:        rec.ID.String(),
		Event:        "COMMENT",
		Summary:      strings.Join(summaryParts, "\n\n"),
		Inlines:      inlines,
		FileComments: fileComments,
		Fingerprints: fingerprints,
	}
}

func (o *Orchestrator) recordExecutionRow(
	ctx context.Context,
	rec *store.RunRecord,
	profileName string,
	role string,
	out shardOutcome,
	publishedCount int,
) {
	recCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	row := store.HarnessExecutionRecord{
		RunID:             rec.ID,
		ShardIndex:        out.shardIndex,
		Attempt:           rec.Attempt,
		Role:              role,
		HarnessProfile:    profileName,
		FindingsPublished: publishedCount,
		DurationMs:        out.duration.Milliseconds(),
		Report:            out.report,
	}
	if out.report != nil {
		row.FindingsRaw = len(out.report.Findings)
		row.HarnessVersion = out.report.GetProvenance().GetHarnessVersion()
		row.NormalizerPath = normalizerLabel(out.report.GetProvenance().GetNormalizer())
	}
	if out.err != nil {
		row.Status = "sandbox_error"
	} else if out.report != nil && out.report.GetProvenance().GetStatus() == reviewv1.ReportStatus_REPORT_STATUS_OK {
		row.Status = "ok"
	} else {
		row.Status = "harness_failed"
	}
	if out.usage != nil {
		row.Requests = int(out.usage.Requests + out.usage.NormalizeRequests)
		row.PromptTokens = out.usage.PromptTokens
		row.CachedTokens = out.usage.CachedTokens
		row.CompletionTokens = out.usage.CompletionTokens
	}
	_ = o.store.RecordHarnessExecution(recCtx, row)
}

func (o *Orchestrator) handleRunFailure(parentCtx, runCtx context.Context, rec *store.RunRecord, profileName string, runErr error) {
	if parentCtx.Err() != nil {
		relCtx, cancel := context.WithTimeout(context.WithoutCancel(parentCtx), 5*time.Second)
		defer cancel()
		_, _ = o.store.ReleaseLeaseToQueued(relCtx, rec.ID, o.cfg.WorkerID)
		return
	}
	if errors.Is(runErr, errLeaseLost) || runCtx.Err() != nil {
		o.telemetry.IncRun(rec.Repo, profileName, store.StatusSuperseded)
		return
	}
	finalFailed, err := o.store.RecordErrorOrRequeue(parentCtx, rec.ID, o.cfg.WorkerID, runErr.Error(), o.cfg.RetryBackoff)
	if err != nil {
		return
	}
	if finalFailed {
		o.telemetry.IncRun(rec.Repo, profileName, store.StatusFailed)
		if rec.TriggerType == "manual_command" {
			_, _ = o.vcs.PublishReview(parentCtx, contract.PublishRequest{
				Repo:     rec.Repo,
				PRNumber: rec.PRNumber,
				CommitID: rec.HeadSHA,
				RunID:    rec.ID.String(),
				Event:    "COMMENT",
				Summary:  fmt.Sprintf("Manual code review failed (run_id: `%s`); check controller logs.", rec.ID),
			})
		}
	}
}

func (o *Orchestrator) registerInFlight(rec *store.RunRecord, cancel context.CancelFunc) {
	k := prGroupKey{repo: rec.Repo, pr: rec.PRNumber}
	o.activeMu.Lock()
	o.activeRuns[k] = append(o.activeRuns[k], inFlightEntry{
		runID:   rec.ID,
		headSHA: rec.HeadSHA,
		cancel:  cancel,
	})
	o.activeMu.Unlock()
}

func (o *Orchestrator) unregisterInFlight(rec *store.RunRecord) {
	k := prGroupKey{repo: rec.Repo, pr: rec.PRNumber}
	o.activeMu.Lock()
	list := o.activeRuns[k]
	kept := list[:0]
	for _, item := range list {
		if item.runID != rec.ID {
			kept = append(kept, item)
		}
	}
	if len(kept) == 0 {
		delete(o.activeRuns, k)
	} else {
		o.activeRuns[k] = kept
	}
	o.activeMu.Unlock()
}

func (o *Orchestrator) cancelInFlightForPR(repo string, pr int, keepRunID uuid.UUID) {
	k := prGroupKey{repo: repo, pr: pr}
	o.activeMu.Lock()
	var toCancel []context.CancelFunc
	for _, item := range o.activeRuns[k] {
		if keepRunID == uuid.Nil || item.runID != keepRunID {
			toCancel = append(toCancel, item.cancel)
		}
	}
	o.activeMu.Unlock()

	for _, fn := range toCancel {
		fn()
	}
}

func (o *Orchestrator) signalWake() {
	select {
	case o.wakeCh <- struct{}{}:
	default:
	}
}

func normalizerLabel(p reviewv1.NormalizerPath) string {
	switch p {
	case reviewv1.NormalizerPath_NORMALIZER_PATH_DETERMINISTIC:
		return "deterministic"
	case reviewv1.NormalizerPath_NORMALIZER_PATH_LLM:
		return "llm"
	default:
		return "none"
	}
}

func shortCommit(sha string) string {
	if len(sha) > 8 {
		return sha[:8]
	}
	return sha
}

func randomTokenHex(nBytes int) string {
	b := make([]byte, nBytes)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func logShardFinished(runID string, shardIdx int, role, profile string, elapsed time.Duration, report *reviewv1.ReviewReport, usage *reviewv1.SandboxUsage, runErr error) {
	status := report.GetProvenance().GetStatus().String()
	if runErr != nil {
		status = "sandbox_error"
	}
	attrs := []any{"run_id", runID, "shard", shardIdx, "role", role, "profile", profile, "status", status, "duration_ms", elapsed.Milliseconds()}
	if usage != nil {
		attrs = append(attrs,
			"requests", usage.GetRequests(),
			"normalize_requests", usage.GetNormalizeRequests(),
			"rejected_requests", usage.GetRejectedRequests(),
			"prompt_tokens", usage.GetPromptTokens(),
			"cached_tokens", usage.GetCachedTokens(),
			"completion_tokens", usage.GetCompletionTokens(),
		)
	}
	if runErr != nil {
		attrs = append(attrs, "error", runErr.Error())
	}
	slog.Info("shard finished", attrs...)
}
