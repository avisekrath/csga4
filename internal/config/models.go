package config

import "time"

// AppConfig holds global application configuration
type AppConfig struct {
	ClientID     string `json:"client_id" yaml:"client_id"`                           // Global OAuth client ID
	ClientSecret string `json:"client_secret" yaml:"client_secret"`                   // Global OAuth client secret
	ActivePreset string `json:"active_preset,omitempty" yaml:"active_preset,omitempty"` // Current active preset
	CreatedAt    time.Time `json:"created_at" yaml:"created_at"`
	UpdatedAt    time.Time `json:"updated_at" yaml:"updated_at"`
}

// Preset represents a saved GA4 configuration with user credentials
type Preset struct {
	Name         string    `json:"name" yaml:"name"`
	RefreshToken string    `json:"refresh_token" yaml:"refresh_token"`
	UserEmail    string    `json:"user_email,omitempty" yaml:"user_email,omitempty"` // For identification
	CreatedAt    time.Time `json:"created_at" yaml:"created_at"`
	LastUsed     time.Time `json:"last_used" yaml:"last_used"`
	Accounts     []Account `json:"accounts,omitempty" yaml:"accounts,omitempty"`
}

// Account represents a GA4 account
type Account struct {
	ID           string     `json:"id" yaml:"id"`
	Name         string     `json:"name" yaml:"name"`
	DisplayName  string     `json:"display_name" yaml:"display_name"`
	RegionCode   string     `json:"region_code" yaml:"region_code"`
	CreateTime   time.Time  `json:"create_time" yaml:"create_time"`
	Properties   []Property `json:"properties,omitempty" yaml:"properties,omitempty"`
}

// Property represents a GA4 property
type Property struct {
	ID              string    `json:"id" yaml:"id"`                                // e.g., "263883430"
	Name            string    `json:"name" yaml:"name"`                            // e.g., "T-Mobile GA4 - Prod"
	DisplayName     string    `json:"display_name" yaml:"display_name"`
	IndustryCategory string   `json:"industry_category" yaml:"industry_category"`
	TimeZone        string    `json:"time_zone" yaml:"time_zone"`                  // e.g., "America/Los_Angeles"
	CurrencyCode    string    `json:"currency_code" yaml:"currency_code"`          // e.g., "USD"
	ServiceLevel    string    `json:"service_level" yaml:"service_level"`          // "GOOGLE_ANALYTICS_STANDARD"
	CreateTime      time.Time `json:"create_time" yaml:"create_time"`
	LastAccessed    time.Time `json:"last_accessed" yaml:"last_accessed"`
	CacheStatus     CacheInfo `json:"cache_status" yaml:"cache_status"`
}

// CacheInfo tracks data freshness
type CacheInfo struct {
	LastUpdated   time.Time `json:"last_updated" yaml:"last_updated"`
	DimensionsTTL time.Time `json:"dimensions_ttl" yaml:"dimensions_ttl"`
	MetricsTTL    time.Time `json:"metrics_ttl" yaml:"metrics_ttl"`
	EventsTTL     time.Time `json:"events_ttl" yaml:"events_ttl"`
	IsStale       bool      `json:"is_stale" yaml:"is_stale"`
}

// PropertyMetadata holds cached metadata for a GA4 property
type PropertyMetadata struct {
	PropertyID    string                  `json:"property_id" yaml:"property_id"`
	LastUpdated   time.Time               `json:"last_updated" yaml:"last_updated"`
	DimensionCount int                    `json:"dimension_count" yaml:"dimension_count"`
	MetricCount    int                    `json:"metric_count" yaml:"metric_count"`
	Dimensions     map[string]DimensionInfo `json:"dimensions" yaml:"dimensions"`
	Metrics        map[string]MetricInfo    `json:"metrics" yaml:"metrics"`
	CustomDimensions int                  `json:"custom_dimensions" yaml:"custom_dimensions"`
	CustomMetrics    int                  `json:"custom_metrics" yaml:"custom_metrics"`
}

// DimensionInfo stores essential dimension metadata
type DimensionInfo struct {
	APIName         string `json:"api_name" yaml:"api_name"`
	UIName          string `json:"ui_name" yaml:"ui_name"`
	Description     string `json:"description" yaml:"description"`
	Category        string `json:"category" yaml:"category"`
	CustomDefinition bool  `json:"custom_definition" yaml:"custom_definition"`
}

// MetricInfo stores essential metric metadata  
type MetricInfo struct {
	APIName         string `json:"api_name" yaml:"api_name"`
	UIName          string `json:"ui_name" yaml:"ui_name"`
	Description     string `json:"description" yaml:"description"`
	Type            string `json:"type" yaml:"type"`
	Category        string `json:"category" yaml:"category"`
	CustomDefinition bool  `json:"custom_definition" yaml:"custom_definition"`
}

// EventAnalysisResult holds cached event analysis data
type EventAnalysisResult struct {
	PropertyID       string               `json:"property_id" yaml:"property_id"`
	DateRange        string               `json:"date_range" yaml:"date_range"`
	AnalyzedAt       time.Time            `json:"analyzed_at" yaml:"analyzed_at"`
	TotalEvents      int                  `json:"total_events" yaml:"total_events"`
	TotalEventCount  int64                `json:"total_event_count" yaml:"total_event_count"`
	TotalActiveUsers int64                `json:"total_active_users" yaml:"total_active_users"`
	TopEvents        []EventInfo          `json:"top_events" yaml:"top_events"`
	ConversionEvents []string             `json:"conversion_events" yaml:"conversion_events"`
}

// EventInfo holds data about individual events
type EventInfo struct {
	EventName     string  `json:"event_name" yaml:"event_name"`
	EventCount    int64   `json:"event_count" yaml:"event_count"`
	ActiveUsers   int64   `json:"active_users" yaml:"active_users"`
	EventsPerUser float64 `json:"events_per_user" yaml:"events_per_user"`
	IsConversion  bool    `json:"is_conversion" yaml:"is_conversion"`
}

// CacheStats holds cache performance metrics
type CacheStats struct {
	TotalHits     int        `json:"total_hits"`
	TotalMisses   int        `json:"total_misses"`
	HitRate       float64    `json:"hit_rate"`
	EntriesCount  int        `json:"entries_count"`
	LastCleanup   *time.Time `json:"last_cleanup"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

// NamedTable represents a named query result table
type NamedTable struct {
	Name           string    `json:"name"`
	Description    string    `json:"description"`
	RowCount       int       `json:"row_count"`
	CreatedAt      time.Time `json:"created_at"`
	LastAccessed   time.Time `json:"last_accessed"`
	QueryCreatedAt time.Time `json:"query_created_at"`
}