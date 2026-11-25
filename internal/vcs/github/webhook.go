package github

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
)

var (
	ErrInvalidWebhookSignature = errors.New("invalid or missing X-Hub-Signature-256")
	reCodeFenceWebhook         = regexp.MustCompile("(?s)(```.*?```|~~~.*?~~~)")
	reReviewSlashCmd           = regexp.MustCompile(`(?m)^[ \t]*/review(?:[ \t]+([A-Za-z0-9._-]+))?[ \t]*\r?$`)
)

type ghUser struct {
	Login string `json:"login"`
	Type  string `json:"type"`
}

type ghRepo struct {
	FullName string `json:"full_name"`
	Fork     bool   `json:"fork"`
}

type ghRef struct {
	SHA  string  `json:"sha"`
	Ref  string  `json:"ref"`
	Repo *ghRepo `json:"repo"`
}

type ghPullRequest struct {
	Number            int    `json:"number"`
	State             string `json:"state"`
	Draft             bool   `json:"draft"`
	Title             string `json:"title"`
	Body              string `json:"body"`
	AuthorAssociation string `json:"author_association"`
	User              ghUser `json:"user"`
	Base              ghRef  `json:"base"`
	Head              ghRef  `json:"head"`
}

type pullRequestWebhookPayload struct {
	Action      string        `json:"action"`
	Number      int           `json:"number"`
	PullRequest ghPullRequest `json:"pull_request"`
	Repository  ghRepo        `json:"repository"`
	Sender      ghUser        `json:"sender"`
}

type issueCommentWebhookPayload struct {
	Action string `json:"action"`
	Issue  struct {
		Number      int             `json:"number"`
		Title       string          `json:"title"`
		Body        string          `json:"body"`
		User        ghUser          `json:"user"`
		PullRequest json.RawMessage `json:"pull_request"`
	} `json:"issue"`
	Comment struct {
		ID                int64  `json:"id"`
		Body              string `json:"body"`
		AuthorAssociation string `json:"author_association"`
		User              ghUser `json:"user"`
	} `json:"comment"`
	Repository ghRepo `json:"repository"`
	Sender     ghUser `json:"sender"`
}

type reviewCommentWebhookPayload struct {
	Action  string `json:"action"`
	Comment struct {
		ID          int64  `json:"id"`
		InReplyToID int64  `json:"in_reply_to_id"`
		Body        string `json:"body"`
		User        ghUser `json:"user"`
	} `json:"comment"`
	PullRequest ghPullRequest `json:"pull_request"`
	Repository  ghRepo        `json:"repository"`
	Sender      ghUser        `json:"sender"`
}

func (p *Plugin) VerifyAndParseWebhook(ctx context.Context, headers http.Header, body []byte) (*reviewv1.ReviewEvent, error) {
	if err := verifyHMACSignature(p.webhookSecret, headers.Get("X-Hub-Signature-256"), body); err != nil {
		return nil, err
	}

	deliveryID := strings.TrimSpace(headers.Get("X-GitHub-Delivery"))
	eventType := strings.TrimSpace(headers.Get("X-GitHub-Event"))

	switch eventType {
	case "pull_request":
		return parsePullRequestEvent(deliveryID, body)
	case "issue_comment":
		return p.parseIssueCommentEvent(ctx, deliveryID, body)
	case "pull_request_review_comment":
		return parseReviewCommentReplyEvent(deliveryID, body)
	default:
		return &reviewv1.ReviewEvent{
			EventId:   deliveryID,
			EventKind: reviewv1.EventKind_EVENT_KIND_IGNORE,
			Source:    reviewv1.TriggerSource_TRIGGER_SOURCE_GITHUB,
		}, nil
	}
}

func verifyHMACSignature(secret []byte, sigHeader string, body []byte) error {
	if len(secret) == 0 {
		return errors.New("webhook secret is not configured")
	}
	const prefix = "sha256="
	if !strings.HasPrefix(sigHeader, prefix) {
		return ErrInvalidWebhookSignature
	}
	gotMAC, err := hex.DecodeString(strings.TrimPrefix(sigHeader, prefix))
	if err != nil {
		return ErrInvalidWebhookSignature
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(body)
	expectedMAC := mac.Sum(nil)
	if !hmac.Equal(gotMAC, expectedMAC) {
		return ErrInvalidWebhookSignature
	}
	return nil
}

func SignWebhookPayload(secret []byte, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func parsePullRequestEvent(deliveryID string, body []byte) (*reviewv1.ReviewEvent, error) {
	var payload pullRequestWebhookPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("decode pull_request webhook: %w", err)
	}
	prNum := payload.PullRequest.Number
	if prNum == 0 {
		prNum = payload.Number
	}

	var kind reviewv1.EventKind
	switch payload.Action {
	case "opened", "synchronize", "reopened", "ready_for_review":
		kind = reviewv1.EventKind_EVENT_KIND_REVIEW
	case "closed":
		kind = reviewv1.EventKind_EVENT_KIND_CLOSE
	default:
		kind = reviewv1.EventKind_EVENT_KIND_IGNORE
	}

	isFork := true
	if payload.PullRequest.Head.Repo != nil {
		isFork = payload.PullRequest.Head.Repo.Fork ||
			!strings.EqualFold(payload.PullRequest.Head.Repo.FullName, payload.Repository.FullName)
	}

	return &reviewv1.ReviewEvent{
		EventId:      deliveryID,
		EventKind:    kind,
		Source:       reviewv1.TriggerSource_TRIGGER_SOURCE_GITHUB,
		Repo:         payload.Repository.FullName,
		PrNumber:     int32(prNum),
		BaseSha:      payload.PullRequest.Base.SHA,
		HeadSha:      payload.PullRequest.Head.SHA,
		IsDraft:      payload.PullRequest.Draft,
		IsBotAuthor:  isBotUser(payload.PullRequest.User),
		IsFork:       isFork,
		Author:       payload.PullRequest.User.Login,
		TriggeredBy:  payload.Sender.Login,
		IsMaintainer: isMaintainerAssociation(payload.PullRequest.AuthorAssociation),
		TriggerType:  reviewv1.TriggerType_TRIGGER_TYPE_AUTO,
		PrTitle:      payload.PullRequest.Title,
		PrBody:       payload.PullRequest.Body,
	}, nil
}

func (p *Plugin) parseIssueCommentEvent(ctx context.Context, deliveryID string, body []byte) (*reviewv1.ReviewEvent, error) {
	var payload issueCommentWebhookPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("decode issue_comment webhook: %w", err)
	}
	if payload.Action != "created" || len(payload.Issue.PullRequest) == 0 || string(payload.Issue.PullRequest) == "null" {
		return &reviewv1.ReviewEvent{
			EventId:   deliveryID,
			EventKind: reviewv1.EventKind_EVENT_KIND_IGNORE,
			Source:    reviewv1.TriggerSource_TRIGGER_SOURCE_GITHUB,
			Repo:      payload.Repository.FullName,
		}, nil
	}
	if isBotUser(payload.Comment.User) || isBotUser(payload.Sender) {
		return &reviewv1.ReviewEvent{
			EventId:     deliveryID,
			EventKind:   reviewv1.EventKind_EVENT_KIND_IGNORE,
			Source:      reviewv1.TriggerSource_TRIGGER_SOURCE_GITHUB,
			Repo:        payload.Repository.FullName,
			PrNumber:    int32(payload.Issue.Number),
			IsBotAuthor: true,
		}, nil
	}

	matched, requestedProfile := parseReviewCommand(payload.Comment.Body)
	if !matched {
		return &reviewv1.ReviewEvent{
			EventId:   deliveryID,
			EventKind: reviewv1.EventKind_EVENT_KIND_IGNORE,
			Source:    reviewv1.TriggerSource_TRIGGER_SOURCE_GITHUB,
			Repo:      payload.Repository.FullName,
			PrNumber:  int32(payload.Issue.Number),
		}, nil
	}

	ev := &reviewv1.ReviewEvent{
		EventId:       deliveryID,
		EventKind:     reviewv1.EventKind_EVENT_KIND_REVIEW,
		Source:        reviewv1.TriggerSource_TRIGGER_SOURCE_GITHUB,
		Repo:          payload.Repository.FullName,
		PrNumber:      int32(payload.Issue.Number),
		Author:        payload.Issue.User.Login,
		IsBotAuthor:   isBotUser(payload.Issue.User),
		TriggeredBy:   payload.Comment.User.Login,
		IsMaintainer:  isMaintainerAssociation(payload.Comment.AuthorAssociation),
		TriggerType:   reviewv1.TriggerType_TRIGGER_TYPE_MANUAL_COMMAND,
		ForceRerunSeq: payload.Comment.ID,
		PrTitle:       payload.Issue.Title,
		PrBody:        payload.Issue.Body,
	}
	if requestedProfile != "" {
		ev.RequestedHarness = &requestedProfile
	}

	if p.canFetchLivePR() && payload.Repository.FullName != "" && payload.Issue.Number > 0 {
		if pr, err := p.fetchPullRequest(ctx, payload.Repository.FullName, payload.Issue.Number); err == nil {
			ev.BaseSha = pr.Base.SHA
			ev.HeadSha = pr.Head.SHA
			ev.IsDraft = pr.Draft
			ev.Author = pr.User.Login
			ev.IsBotAuthor = isBotUser(pr.User)
			if pr.Title != "" {
				ev.PrTitle = pr.Title
			}
			if pr.Body != "" {
				ev.PrBody = pr.Body
			}
			ev.IsFork = true
			if pr.Head.Repo != nil {
				ev.IsFork = pr.Head.Repo.Fork || !strings.EqualFold(pr.Head.Repo.FullName, payload.Repository.FullName)
			}
		}
	}
	return ev, nil
}

func parseReviewCommentReplyEvent(deliveryID string, body []byte) (*reviewv1.ReviewEvent, error) {
	var payload reviewCommentWebhookPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("decode pull_request_review_comment webhook: %w", err)
	}
	if payload.Action != "created" || payload.Comment.InReplyToID <= 0 || isBotUser(payload.Comment.User) {
		return &reviewv1.ReviewEvent{
			EventId:   deliveryID,
			EventKind: reviewv1.EventKind_EVENT_KIND_IGNORE,
			Source:    reviewv1.TriggerSource_TRIGGER_SOURCE_GITHUB,
			Repo:      payload.Repository.FullName,
		}, nil
	}
	return &reviewv1.ReviewEvent{
		EventId:       deliveryID,
		EventKind:     reviewv1.EventKind_EVENT_KIND_FEEDBACK,
		Source:        reviewv1.TriggerSource_TRIGGER_SOURCE_GITHUB,
		Repo:          payload.Repository.FullName,
		PrNumber:      int32(payload.PullRequest.Number),
		BaseSha:       payload.PullRequest.Base.SHA,
		HeadSha:       payload.PullRequest.Head.SHA,
		Author:        payload.PullRequest.User.Login,
		TriggeredBy:   payload.Comment.User.Login,
		ForceRerunSeq: payload.Comment.InReplyToID,
	}, nil
}

func parseReviewCommand(body string) (bool, string) {
	stripped := reCodeFenceWebhook.ReplaceAllString(body, "")
	m := reReviewSlashCmd.FindStringSubmatch(stripped)
	if m == nil {
		return false, ""
	}
	return true, strings.TrimSpace(m[1])
}

func isMaintainerAssociation(assoc string) bool {
	switch strings.ToUpper(strings.TrimSpace(assoc)) {
	case "OWNER", "MEMBER", "COLLABORATOR":
		return true
	default:
		return false
	}
}

func isBotUser(u ghUser) bool {
	if strings.EqualFold(strings.TrimSpace(u.Type), "Bot") {
		return true
	}
	login := strings.ToLower(strings.TrimSpace(u.Login))
	return strings.HasSuffix(login, "[bot]")
}
