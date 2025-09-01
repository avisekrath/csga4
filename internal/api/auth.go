package api

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"ga4admin/internal/config"
	"ga4admin/internal/preset"
)

const (
	// OAuth2 scopes required for GA4 API access
	AnalyticsReadOnlyScope = "https://www.googleapis.com/auth/analytics.readonly"
	
	// Token refresh buffer - refresh tokens 5 minutes before expiry
	TokenRefreshBuffer = 5 * time.Minute
)

// AuthClient manages OAuth2 authentication for GA4 API calls
type AuthClient struct {
	clientID     string
	clientSecret string
	config       *oauth2.Config
	
	// Token cache to avoid repeated refresh calls
	tokenMutex   sync.RWMutex
	cachedToken  *oauth2.Token
	cacheExpiry  time.Time
	lastRefreshToken string // Track which refresh token was used for cache
}

// NewAuthClient creates a new authentication client using global OAuth credentials
func NewAuthClient() (*AuthClient, error) {
	// Get global OAuth credentials
	clientID, clientSecret, err := config.GetClientCredentials()
	if err != nil {
		return nil, fmt.Errorf("failed to get OAuth credentials: %w", err)
	}

	if clientID == "" || clientSecret == "" {
		return nil, fmt.Errorf("OAuth credentials not configured - run 'ga4admin config set' first")
	}

	// Create OAuth2 config
	oauth2Config := &oauth2.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		Endpoint:     google.Endpoint,
		Scopes:       []string{AnalyticsReadOnlyScope},
	}

	return &AuthClient{
		clientID:     clientID,
		clientSecret: clientSecret,
		config:       oauth2Config,
	}, nil
}

// GetAccessToken gets a valid access token using the active preset's refresh token
func (a *AuthClient) GetAccessToken(ctx context.Context) (*oauth2.Token, error) {
	// Get active preset for refresh token
	activePreset, err := preset.GetActivePreset()
	if err != nil {
		return nil, fmt.Errorf("failed to get active preset: %w", err)
	}
	
	if activePreset == nil {
		return nil, fmt.Errorf("no active preset set - run 'ga4admin preset use <name>' first")
	}

	if activePreset.RefreshToken == "" {
		return nil, fmt.Errorf("active preset '%s' has no refresh token", activePreset.Name)
	}

	// Check if we have a cached valid token for this refresh token
	a.tokenMutex.RLock()
	if a.cachedToken != nil && 
		a.lastRefreshToken == activePreset.RefreshToken &&
		time.Now().Before(a.cacheExpiry) {
		token := a.cachedToken
		a.tokenMutex.RUnlock()
		return token, nil
	}
	a.tokenMutex.RUnlock()

	// Need to refresh token
	return a.refreshToken(ctx, activePreset.RefreshToken)
}

// refreshToken exchanges a refresh token for a new access token
func (a *AuthClient) refreshToken(ctx context.Context, refreshToken string) (*oauth2.Token, error) {
	a.tokenMutex.Lock()
	defer a.tokenMutex.Unlock()

	// Double-check cache after acquiring write lock
	if a.cachedToken != nil && 
		a.lastRefreshToken == refreshToken &&
		time.Now().Before(a.cacheExpiry) {
		return a.cachedToken, nil
	}

	// Create token with refresh token
	token := &oauth2.Token{
		RefreshToken: refreshToken,
	}

	// Use OAuth2 client to refresh the token
	tokenSource := a.config.TokenSource(ctx, token)
	newToken, err := tokenSource.Token()
	if err != nil {
		return nil, fmt.Errorf("failed to refresh access token: %w", err)
	}

	// Validate token
	if newToken.AccessToken == "" {
		return nil, fmt.Errorf("received empty access token")
	}

	if !newToken.Valid() {
		return nil, fmt.Errorf("received invalid token")
	}

	// Cache the token with buffer for proactive refresh
	cacheExpiry := newToken.Expiry
	if !cacheExpiry.IsZero() {
		cacheExpiry = cacheExpiry.Add(-TokenRefreshBuffer)
	} else {
		// Default 1-hour cache if no expiry provided
		cacheExpiry = time.Now().Add(1 * time.Hour)
	}

	a.cachedToken = newToken
	a.cacheExpiry = cacheExpiry
	a.lastRefreshToken = refreshToken

	return newToken, nil
}

// AuthenticatedHTTPClient returns an HTTP client with automatic OAuth authentication
func (a *AuthClient) AuthenticatedHTTPClient(ctx context.Context) (*http.Client, error) {
	// Get valid access token
	token, err := a.GetAccessToken(ctx)
	if err != nil {
		return nil, err
	}

	// Create token source that will automatically refresh if needed
	tokenSource := oauth2.ReuseTokenSource(token, &refreshTokenSource{
		authClient: a,
		ctx:        ctx,
	})

	// Return HTTP client with automatic auth
	return oauth2.NewClient(ctx, tokenSource), nil
}

// ClearTokenCache clears the cached access token (useful for testing or forcing refresh)
func (a *AuthClient) ClearTokenCache() {
	a.tokenMutex.Lock()
	defer a.tokenMutex.Unlock()
	
	a.cachedToken = nil
	a.cacheExpiry = time.Time{}
	a.lastRefreshToken = ""
}

// ValidateRefreshToken tests if a refresh token is valid by attempting to refresh it
func (a *AuthClient) ValidateRefreshToken(ctx context.Context, refreshToken string) error {
	if refreshToken == "" {
		return fmt.Errorf("refresh token is empty")
	}

	// Basic format validation - Google refresh tokens start with "1//"
	if len(refreshToken) < 3 || refreshToken[:3] != "1//" {
		return fmt.Errorf("invalid refresh token format - Google refresh tokens start with '1//'")
	}

	// Test token by attempting to refresh it
	_, err := a.refreshToken(ctx, refreshToken)
	if err != nil {
		return fmt.Errorf("refresh token validation failed: %w", err)
	}

	return nil
}

// GetTokenInfo returns information about the current cached token
func (a *AuthClient) GetTokenInfo() map[string]interface{} {
	a.tokenMutex.RLock()
	defer a.tokenMutex.RUnlock()

	info := map[string]interface{}{
		"has_cached_token": a.cachedToken != nil,
		"cache_expiry":     a.cacheExpiry,
	}

	if a.cachedToken != nil {
		info["token_expiry"] = a.cachedToken.Expiry
		info["token_valid"] = a.cachedToken.Valid()
		info["needs_refresh"] = time.Now().After(a.cacheExpiry)
	}

	return info
}

// refreshTokenSource implements oauth2.TokenSource for automatic token refresh
type refreshTokenSource struct {
	authClient *AuthClient
	ctx        context.Context
}

func (r *refreshTokenSource) Token() (*oauth2.Token, error) {
	return r.authClient.GetAccessToken(r.ctx)
}