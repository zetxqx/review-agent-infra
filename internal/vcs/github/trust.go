package github

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

type markerTrustMode int

const (
	markerTrustAll markerTrustMode = iota
	markerTrustApp
	markerTrustLogin
	markerTrustBots
)

type markerTrust struct {
	mode     markerTrustMode
	appID    int64
	botLogin string
	login    string
}

func (t markerTrust) allows(app *ghAppMeta, user ghUser) bool {
	switch t.mode {
	case markerTrustApp:
		if app != nil && app.ID > 0 {
			return app.ID == t.appID
		}
		if t.botLogin != "" {
			return strings.EqualFold(strings.TrimSpace(user.Login), t.botLogin)
		}
		return isBotUser(user)
	case markerTrustLogin:
		return strings.EqualFold(strings.TrimSpace(user.Login), t.login)
	case markerTrustBots:
		return isBotUser(user)
	default:
		return true
	}
}

func hasAuthorMetadata(app *ghAppMeta, user ghUser) bool {
	return app != nil || strings.TrimSpace(user.Login) != "" || strings.TrimSpace(user.Type) != ""
}

func (p *Plugin) markerTrustFor(ctx context.Context, repo string) markerTrust {
	if appID := p.auth.AppID(); appID > 0 {
		return markerTrust{mode: markerTrustApp, appID: appID, botLogin: p.auth.BotLogin()}
	}
	if !p.auth.UsesStaticToken() {
		return markerTrust{mode: markerTrustAll}
	}
	return p.staticTokenTrust(ctx, repo)
}

func (p *Plugin) staticTokenTrust(ctx context.Context, repo string) markerTrust {
	p.staticTrustMu.Lock()
	cached := p.staticTrust
	p.staticTrustMu.Unlock()
	if cached != nil {
		return *cached
	}
	trust, cacheable := p.fetchStaticTokenTrust(ctx, repo)
	if cacheable {
		p.staticTrustMu.Lock()
		if p.staticTrust == nil {
			p.staticTrust = &trust
		}
		trust = *p.staticTrust
		p.staticTrustMu.Unlock()
	}
	return trust
}

func (p *Plugin) fetchStaticTokenTrust(ctx context.Context, repo string) (markerTrust, bool) {
	botsOnly := markerTrust{mode: markerTrustBots}
	resp, err := p.doRaw(ctx, repo, http.MethodGet, "/user", "application/vnd.github+json", nil)
	if err != nil {
		return botsOnly, false
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		return botsOnly, false
	}
	switch resp.StatusCode {
	case http.StatusOK:
		var owner ghUser
		if json.Unmarshal(body, &owner) != nil {
			return botsOnly, false
		}
		login := strings.TrimSpace(owner.Login)
		if login == "" {
			return markerTrust{mode: markerTrustAll}, true
		}
		return markerTrust{mode: markerTrustLogin, login: login}, true
	case http.StatusNotFound, http.StatusNotImplemented:
		return markerTrust{mode: markerTrustAll}, true
	case http.StatusForbidden:
		return botsOnly, true
	default:
		return botsOnly, false
	}
}
