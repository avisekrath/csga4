package export

import (
	"time"
)

// PropertyExport represents the JSON structure for exported property data
type PropertyExport struct {
	PropertyInfo           PropertyInfo                       `json:"property_info"`
	CollectionMetadata     CollectionMetadata                 `json:"collection_metadata"`
	CustomDimensions       map[string][]CustomDimensionInfo   `json:"custom_dimensions"`
	ClarisightsIntegration ClarisightsIntegration             `json:"clarisights_integration"`
}

// PropertyInfo contains basic property information
type PropertyInfo struct {
	PropertyID   string     `json:"property_id"`
	PropertyName string     `json:"property_name"`
	AccountID    string     `json:"account_id"`
	AccountName  string     `json:"account_name"`
	Currency     string     `json:"currency"`
	Timezone     string     `json:"timezone"`
	Industry     string     `json:"industry"`
	ServiceLevel string     `json:"service_level"`
	CreatedDate  *time.Time `json:"created_date"`
	LastAccessed *time.Time `json:"last_accessed"`
}

// CollectionMetadata contains metadata about the collection process
type CollectionMetadata struct {
	Timestamp           time.Time `json:"timestamp"`
	TotalDimensions     int       `json:"total_dimensions"`
	CustomDimensions    int       `json:"custom_dimensions"`
	CollectorVersion    string    `json:"collector_version"`
	PresetUsed          string    `json:"preset_used"`
	CollectionDuration  string    `json:"collection_duration"`
	ApiCallCount        int       `json:"api_call_count"`
}

// CustomDimensionInfo represents a single custom dimension
type CustomDimensionInfo struct {
	APIName          string `json:"api_name"`
	UIName           string `json:"ui_name"`
	Description      string `json:"description"`
	Scope            string `json:"scope"`
	Category         string `json:"category"`
	CustomDefinition bool   `json:"custom_definition"`
}

// ClarisightsIntegration tracks Clarisights-specific integration status
type ClarisightsIntegration struct {
	HasCustomChannelGroups bool   `json:"has_custom_channel_groups"`
	ChannelGroupID         string `json:"channel_group_id"`
	ChannelGroupName       string `json:"channel_group_name"`
}

// ExportResult contains summary information about an export operation
type ExportResult struct {
	TotalProperties    int                    `json:"total_properties"`
	TotalDimensions    int                    `json:"total_dimensions"`
	ClarisightsReady   int                    `json:"clarisights_ready"`
	AccountSummary     map[string]AccountInfo `json:"account_summary"`
	CollectionTimestamp time.Time             `json:"collection_timestamp"`
}

// AccountInfo contains summary information about an account
type AccountInfo struct {
	AccountName      string `json:"account_name"`
	PropertyCount    int    `json:"property_count"`
	DimensionCount   int    `json:"dimension_count"`
	ClarisightsReady int    `json:"clarisights_ready"`
}