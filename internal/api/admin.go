package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"ga4admin/internal/config"
)

// AdminClient handles GA4 Admin API operations
type AdminClient struct {
	authClient *AuthClient
	baseURL    string
}

// NewAdminClient creates a new GA4 Admin API client
func NewAdminClient() (*AdminClient, error) {
	authClient, err := NewAuthClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create auth client: %w", err)
	}

	return &AdminClient{
		authClient: authClient,
		baseURL:    "https://analyticsadmin.googleapis.com/v1alpha",
	}, nil
}

// GA4 Admin API response structures
type accountsResponse struct {
	Accounts []struct {
		Name         string    `json:"name"`         // "accounts/71671299"
		DisplayName  string    `json:"displayName"`  // "T-Mobile Tuesdays"
		RegionCode   string    `json:"regionCode"`   // "US"
		CreateTime   string    `json:"createTime"`   // "2015-12-22T21:15:23.770Z"
		UpdateTime   string    `json:"updateTime"`   // "2025-05-14T18:23:42.123Z"
		Industry     string    `json:"industry"`     // "INTERNET_AND_TELECOM"
		Deleted      bool      `json:"deleted"`
	} `json:"accounts"`
	NextPageToken string `json:"nextPageToken"`
}

type propertiesResponse struct {
	Properties []struct {
		Name             string `json:"name"`             // "properties/328687832"
		DisplayName      string `json:"displayName"`      // "GA4 Metro - Prod"
		PropertyType     string `json:"propertyType"`     // "PROPERTY_TYPE_ORDINARY"
		CreateTime       string `json:"createTime"`       // "2022-08-24T17:32:15.234Z"
		UpdateTime       string `json:"updateTime"`       // "2025-08-30T14:25:17.456Z"
		Parent           string `json:"parent"`           // "accounts/71671299"
		CurrencyCode     string `json:"currencyCode"`     // "USD"
		TimeZone         string `json:"timeZone"`         // "America/Los_Angeles"
		IndustryCategory string `json:"industryCategory"` // "INTERNET_AND_TELECOM"
		ServiceLevel     string `json:"serviceLevel"`     // "GOOGLE_ANALYTICS_STANDARD"
		Deleted          bool   `json:"deleted"`
	} `json:"properties"`
	NextPageToken string `json:"nextPageToken"`
}

type propertyResponse struct {
	Name             string `json:"name"`             // "properties/328687832"
	DisplayName      string `json:"displayName"`      // "GA4 Metro - Prod"
	PropertyType     string `json:"propertyType"`     // "PROPERTY_TYPE_ORDINARY"
	CreateTime       string `json:"createTime"`       // "2022-08-24T17:32:15.234Z"
	UpdateTime       string `json:"updateTime"`       // "2025-08-30T14:25:17.456Z"
	Parent           string `json:"parent"`           // "accounts/71671299"
	CurrencyCode     string `json:"currencyCode"`     // "USD"
	TimeZone         string `json:"timeZone"`         // "America/Los_Angeles"
	IndustryCategory string `json:"industryCategory"` // "INTERNET_AND_TELECOM"
	ServiceLevel     string `json:"serviceLevel"`     // "GOOGLE_ANALYTICS_STANDARD"
	Deleted          bool   `json:"deleted"`
}

// ListAccounts retrieves all GA4 accounts accessible by the current preset
func (c *AdminClient) ListAccounts(ctx context.Context) ([]config.Account, error) {
	httpClient, err := c.authClient.AuthenticatedHTTPClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get authenticated HTTP client: %w", err)
	}

	url := fmt.Sprintf("%s/accounts", c.baseURL)
	resp, err := httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to make request to GA4 Admin API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GA4 Admin API returned status %d: %s", resp.StatusCode, resp.Status)
	}

	var apiResponse accountsResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResponse); err != nil {
		return nil, fmt.Errorf("failed to decode accounts response: %w", err)
	}

	// Convert API response to our internal Account structure
	accounts := make([]config.Account, 0, len(apiResponse.Accounts))
	for _, apiAccount := range apiResponse.Accounts {
		if apiAccount.Deleted {
			continue // Skip deleted accounts
		}

		// Extract account ID from name field (format: "accounts/71671299")
		accountID := extractIDFromResource(apiAccount.Name, "accounts/")

		// Parse create time
		createTime, err := time.Parse(time.RFC3339, apiAccount.CreateTime)
		if err != nil {
			createTime = time.Now() // fallback to current time
		}

		account := config.Account{
			ID:          accountID,
			Name:        apiAccount.Name,
			DisplayName: apiAccount.DisplayName,
			RegionCode:  apiAccount.RegionCode,
			CreateTime:  createTime,
			Properties:  []config.Property{}, // Will be populated by separate API call
		}

		accounts = append(accounts, account)
	}

	return accounts, nil
}

// ListProperties retrieves all properties accessible to the current user for a given account
func (c *AdminClient) ListProperties(ctx context.Context, accountID string) ([]config.Property, error) {
	httpClient, err := c.authClient.AuthenticatedHTTPClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get authenticated HTTP client: %w", err)
	}

	// GA4 Admin API requires a filter parameter for listing properties
	url := fmt.Sprintf("%s/properties?filter=parent:accounts/%s", c.baseURL, accountID)
	resp, err := httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to make request to GA4 Admin API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GA4 Admin API returned status %d: %s", resp.StatusCode, resp.Status)
	}

	var apiResponse propertiesResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResponse); err != nil {
		return nil, fmt.Errorf("failed to decode properties response: %w", err)
	}

	// Convert API response to our internal Property structure
	properties := make([]config.Property, 0, len(apiResponse.Properties))
	for _, apiProperty := range apiResponse.Properties {
		if apiProperty.Deleted {
			continue // Skip deleted properties
		}

		// Extract property ID from name field (format: "properties/328687832")
		propertyID := extractIDFromResource(apiProperty.Name, "properties/")

		// Parse create time
		createTime, err := time.Parse(time.RFC3339, apiProperty.CreateTime)
		if err != nil {
			createTime = time.Now() // fallback to current time
		}

		property := config.Property{
			ID:              propertyID,
			Name:            apiProperty.Name,
			DisplayName:     apiProperty.DisplayName,
			IndustryCategory: apiProperty.IndustryCategory,
			TimeZone:        apiProperty.TimeZone,
			CurrencyCode:    apiProperty.CurrencyCode,
			ServiceLevel:    apiProperty.ServiceLevel,
			CreateTime:      createTime,
			LastAccessed:    time.Now(), // Update on each API call
			CacheStatus: config.CacheInfo{
				LastUpdated: time.Now(),
				IsStale:     true, // New property data is always considered fresh for caching
			},
		}

		properties = append(properties, property)
	}

	return properties, nil
}

// GetProperty retrieves detailed information for a specific property
func (c *AdminClient) GetProperty(ctx context.Context, propertyID string) (*config.Property, error) {
	httpClient, err := c.authClient.AuthenticatedHTTPClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get authenticated HTTP client: %w", err)
	}

	url := fmt.Sprintf("%s/properties/%s", c.baseURL, propertyID)
	resp, err := httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to make request to GA4 Admin API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("property %s not found or not accessible", propertyID)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GA4 Admin API returned status %d: %s", resp.StatusCode, resp.Status)
	}

	var apiResponse propertyResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResponse); err != nil {
		return nil, fmt.Errorf("failed to decode property response: %w", err)
	}

	if apiResponse.Deleted {
		return nil, fmt.Errorf("property %s has been deleted", propertyID)
	}

	// Extract property ID from name field (format: "properties/328687832")
	extractedID := extractIDFromResource(apiResponse.Name, "properties/")

	// Parse create time
	createTime, err := time.Parse(time.RFC3339, apiResponse.CreateTime)
	if err != nil {
		createTime = time.Now() // fallback to current time
	}

	property := &config.Property{
		ID:              extractedID,
		Name:            apiResponse.Name,
		DisplayName:     apiResponse.DisplayName,
		IndustryCategory: apiResponse.IndustryCategory,
		TimeZone:        apiResponse.TimeZone,
		CurrencyCode:    apiResponse.CurrencyCode,
		ServiceLevel:    apiResponse.ServiceLevel,
		CreateTime:      createTime,
		LastAccessed:    time.Now(),
		CacheStatus: config.CacheInfo{
			LastUpdated: time.Now(),
			IsStale:     false, // Fresh data from API
		},
	}

	return property, nil
}

// Helper function to extract ID from GA4 resource names
func extractIDFromResource(resourceName, prefix string) string {
	if len(resourceName) <= len(prefix) {
		return resourceName // fallback to full name if format is unexpected
	}
	return resourceName[len(prefix):]
}