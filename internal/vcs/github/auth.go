package github

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type cachedToken struct {
	token     string
	expiresAt time.Time
}

type AppAuthenticator struct {
	appID          int64
	installationID int64
	privateKey     *rsa.PrivateKey
	staticToken    string
	apiBaseURL     string
	httpClient     *http.Client
	nowFn          func() time.Time

	mu sync.Mutex
	// key: GitHub repository "owner/repo" slug; value: resolved GitHub App installation ID
	repoInstallations map[string]int64
	// key: installation ID decimal string; value: cached GitHub installation access token
	tokensByInstall map[string]cachedToken
}

func ParseRSAPrivateKeyPEM(pemBytes []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("invalid PEM block for GitHub App private key")
	}
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse GitHub App RSA private key: %w", err)
	}
	rsaKey, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("PKCS#8 key is not an RSA private key")
	}
	return rsaKey, nil
}

func newAppAuthenticator(cfg Config) *AppAuthenticator {
	baseURL := strings.TrimRight(cfg.APIBaseURL, "/")
	if baseURL == "" {
		baseURL = defaultGitHubAPIBase
	}
	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: 30 * time.Second}
	}
	nowFn := cfg.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	return &AppAuthenticator{
		appID:          cfg.AppID,
		installationID: cfg.InstallationID,
		privateKey:     cfg.PrivateKey,
		staticToken:    cfg.StaticToken,
		apiBaseURL:     baseURL,
		httpClient:     hc,
		nowFn:          nowFn,
		// key: GitHub repository "owner/repo" slug; value: resolved GitHub App installation ID
		repoInstallations: make(map[string]int64),
		// key: installation ID decimal string; value: cached GitHub installation access token
		tokensByInstall: make(map[string]cachedToken),
	}
}

func (a *AppAuthenticator) HasCredentials() bool {
	return a.staticToken != "" || (a.appID > 0 && a.privateKey != nil)
}

func (a *AppAuthenticator) MintAppJWT() (string, error) {
	if a.appID <= 0 || a.privateKey == nil {
		return "", errors.New("GitHub App ID and RSA private key are required to mint App JWT")
	}
	now := a.nowFn()
	claims := jwt.RegisteredClaims{
		IssuedAt:  jwt.NewNumericDate(now.Add(-60 * time.Second)),
		ExpiresAt: jwt.NewNumericDate(now.Add(9 * time.Minute)),
		Issuer:    strconv.FormatInt(a.appID, 10),
	}
	return jwt.NewWithClaims(jwt.SigningMethodRS256, claims).SignedString(a.privateKey)
}

func (a *AppAuthenticator) Check(ctx context.Context) error {
	if a.staticToken != "" {
		return nil
	}
	appJWT, err := a.MintAppJWT()
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.apiBaseURL+"/app", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+appJWT)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	resp, err := a.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("verify GitHub App: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if a.installationID > 0 {
		if _, err := a.TokenForRepo(ctx, ""); err != nil {
			return err
		}
	}
	return nil
}

func (a *AppAuthenticator) TokenForRepo(ctx context.Context, repo string) (string, error) {
	if a.staticToken != "" {
		return a.staticToken, nil
	}
	if a.appID <= 0 || a.privateKey == nil {
		return "", nil
	}

	installID, err := a.resolveInstallationID(ctx, repo)
	if err != nil {
		return "", err
	}
	installKey := strconv.FormatInt(installID, 10)

	now := a.nowFn()
	a.mu.Lock()
	if cached, ok := a.tokensByInstall[installKey]; ok && now.Add(2*time.Minute).Before(cached.expiresAt) {
		a.mu.Unlock()
		return cached.token, nil
	}
	a.mu.Unlock()

	appJWT, err := a.MintAppJWT()
	if err != nil {
		return "", err
	}

	endpoint := fmt.Sprintf("%s/app/installations/%d/access_tokens", a.apiBaseURL, installID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+appJWT)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("exchange installation token: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var parsed struct {
		Token     string    `json:"token"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", err
	}
	if parsed.Token == "" {
		return "", errors.New("empty installation token in GitHub response")
	}
	if parsed.ExpiresAt.IsZero() {
		parsed.ExpiresAt = now.Add(55 * time.Minute)
	}

	a.mu.Lock()
	a.tokensByInstall[installKey] = cachedToken{
		token:     parsed.Token,
		expiresAt: parsed.ExpiresAt,
	}
	a.mu.Unlock()
	return parsed.Token, nil
}

func (a *AppAuthenticator) resolveInstallationID(ctx context.Context, repo string) (int64, error) {
	if a.installationID > 0 {
		return a.installationID, nil
	}
	owner, name, err := splitOwnerRepo(repo)
	if err != nil {
		return 0, err
	}
	slug := owner + "/" + name

	a.mu.Lock()
	if id, ok := a.repoInstallations[slug]; ok && id > 0 {
		a.mu.Unlock()
		return id, nil
	}
	a.mu.Unlock()

	appJWT, err := a.MintAppJWT()
	if err != nil {
		return 0, err
	}
	endpoint := fmt.Sprintf("%s/repos/%s/%s/installation", a.apiBaseURL, owner, name)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+appJWT)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("lookup repo installation for %s: status %d: %s", slug, resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var parsed struct {
		ID int64 `json:"id"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return 0, err
	}
	if parsed.ID <= 0 {
		return 0, fmt.Errorf("invalid installation id %d for %s", parsed.ID, slug)
	}

	a.mu.Lock()
	a.repoInstallations[slug] = parsed.ID
	a.mu.Unlock()
	return parsed.ID, nil
}
