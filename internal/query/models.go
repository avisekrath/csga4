package query

import (
	"time"

	"ga4admin/internal/api"
)

// QueryConfig represents a complete GA4 query configuration
type QueryConfig struct {
	// Basic query parameters
	PropertyID  string   `json:"property_id" yaml:"property_id"`
	Name        string   `json:"name,omitempty" yaml:"name,omitempty"`
	Description string   `json:"description,omitempty" yaml:"description,omitempty"`
	Dimensions  []string `json:"dimensions" yaml:"dimensions"`
	Metrics     []string `json:"metrics" yaml:"metrics"`

	// Date range
	StartDate string `json:"start_date" yaml:"start_date"`
	EndDate   string `json:"end_date" yaml:"end_date"`

	// Query options
	Limit                int64    `json:"limit,omitempty" yaml:"limit,omitempty"`
	Offset               int64    `json:"offset,omitempty" yaml:"offset,omitempty"`
	KeepEmptyRows        bool     `json:"keep_empty_rows,omitempty" yaml:"keep_empty_rows,omitempty"`
	MetricAggregations   []string `json:"metric_aggregations,omitempty" yaml:"metric_aggregations,omitempty"`
	CurrencyCode         string   `json:"currency_code,omitempty" yaml:"currency_code,omitempty"`
	ReturnPropertyQuota  bool     `json:"return_property_quota,omitempty" yaml:"return_property_quota,omitempty"`

	// Filters
	Filters []FilterConfig `json:"filters,omitempty" yaml:"filters,omitempty"`

	// Sorting
	OrderBy []OrderByConfig `json:"order_by,omitempty" yaml:"order_by,omitempty"`

	// Metadata
	CreatedAt time.Time `json:"created_at" yaml:"created_at"`
	UpdatedAt time.Time `json:"updated_at" yaml:"updated_at"`
	CreatedBy string    `json:"created_by,omitempty" yaml:"created_by,omitempty"`
}

// FilterConfig represents a single filter in a query
type FilterConfig struct {
	FieldName string `json:"field_name" yaml:"field_name"`
	Type      string `json:"type" yaml:"type"` // "string", "numeric", "between", "in_list"

	// String filter options
	StringMatchType   string `json:"string_match_type,omitempty" yaml:"string_match_type,omitempty"`     // EXACT, CONTAINS, etc.
	StringValue       string `json:"string_value,omitempty" yaml:"string_value,omitempty"`
	StringCaseSensitive bool `json:"string_case_sensitive,omitempty" yaml:"string_case_sensitive,omitempty"`

	// Numeric filter options
	NumericOperation string  `json:"numeric_operation,omitempty" yaml:"numeric_operation,omitempty"` // EQUAL, GREATER_THAN, etc.
	NumericValue     float64 `json:"numeric_value,omitempty" yaml:"numeric_value,omitempty"`

	// Between filter options
	BetweenFrom float64 `json:"between_from,omitempty" yaml:"between_from,omitempty"`
	BetweenTo   float64 `json:"between_to,omitempty" yaml:"between_to,omitempty"`

	// In-list filter options
	InListValues        []string `json:"in_list_values,omitempty" yaml:"in_list_values,omitempty"`
	InListCaseSensitive bool     `json:"in_list_case_sensitive,omitempty" yaml:"in_list_case_sensitive,omitempty"`

	// Logic operators for combining filters
	LogicOperator string `json:"logic_operator,omitempty" yaml:"logic_operator,omitempty"` // "AND", "OR", "NOT"
}

// OrderByConfig represents sorting configuration
type OrderByConfig struct {
	FieldName  string `json:"field_name" yaml:"field_name"`   // dimension or metric name
	FieldType  string `json:"field_type" yaml:"field_type"`   // "dimension" or "metric"
	Descending bool   `json:"descending" yaml:"descending"`   // true for DESC, false for ASC
	OrderType  string `json:"order_type,omitempty" yaml:"order_type,omitempty"` // for dimensions: ALPHANUMERIC, CASE_INSENSITIVE_ALPHANUMERIC, NUMERIC
}

// QueryResult represents the result of a query execution
type QueryResult struct {
	// Query metadata
	QueryID      string       `json:"query_id"`
	PropertyID   string       `json:"property_id"`
	QueryHash    string       `json:"query_hash"`
	QueryConfig  *QueryConfig `json:"query_config"`

	// Execution metadata
	ExecutedAt    time.Time `json:"executed_at"`
	ExecutionTime string    `json:"execution_time"`
	RowCount      int       `json:"row_count"`
	FromCache     bool      `json:"from_cache"`

	// Result data
	DimensionHeaders []api.DimensionHeader `json:"dimension_headers"`
	MetricHeaders    []api.MetricHeader    `json:"metric_headers"`
	Rows             []api.Row             `json:"rows"`
	Totals           []api.Row             `json:"totals,omitempty"`
	Maximums         []api.Row             `json:"maximums,omitempty"`
	Minimums         []api.Row             `json:"minimums,omitempty"`

	// GA4 metadata
	ResponseMetadata *api.ResponseMetadata `json:"response_metadata,omitempty"`
	PropertyQuota    *api.PropertyQuota    `json:"property_quota,omitempty"`

	// Error information
	Error string `json:"error,omitempty"`
}

// QueryTemplate represents a saved query template
type QueryTemplate struct {
	Name        string       `json:"name" yaml:"name"`
	Description string       `json:"description" yaml:"description"`
	Category    string       `json:"category,omitempty" yaml:"category,omitempty"`
	Query       *QueryConfig `json:"query" yaml:"query"`
	CreatedAt   time.Time    `json:"created_at" yaml:"created_at"`
	UpdatedAt   time.Time    `json:"updated_at" yaml:"updated_at"`
	UsageCount  int          `json:"usage_count" yaml:"usage_count"`
	LastUsed    *time.Time   `json:"last_used,omitempty" yaml:"last_used,omitempty"`
}

// QueryStats represents statistics about query performance
type QueryStats struct {
	PropertyID         string  `json:"property_id"`
	TotalQueries       int     `json:"total_queries"`
	CacheHitRate       float64 `json:"cache_hit_rate"`
	AverageRowCount    float64 `json:"average_row_count"`
	AverageExecutionMs float64 `json:"average_execution_ms"`
	PopularDimensions  []string `json:"popular_dimensions"`
	PopularMetrics     []string `json:"popular_metrics"`
	LastAnalyzed       time.Time `json:"last_analyzed"`
}

// FilterExpression represents a complex filter combination
type FilterExpression struct {
	Operator   string              `json:"operator,omitempty" yaml:"operator,omitempty"`     // "AND", "OR", "NOT"
	Filters    []FilterConfig      `json:"filters,omitempty" yaml:"filters,omitempty"`       // Simple filters
	Groups     []FilterExpression  `json:"groups,omitempty" yaml:"groups,omitempty"`         // Nested filter groups
}

// DateRangePreset represents common date range configurations
type DateRangePreset struct {
	Name      string `json:"name" yaml:"name"`
	StartDate string `json:"start_date" yaml:"start_date"`
	EndDate   string `json:"end_date" yaml:"end_date"`
}

// Common date range presets
var CommonDateRanges = []DateRangePreset{
	{"Last 7 days", "7daysAgo", "yesterday"},
	{"Last 14 days", "14daysAgo", "yesterday"},
	{"Last 30 days", "30daysAgo", "yesterday"},
	{"Last 90 days", "90daysAgo", "yesterday"},
	{"This month", "2025-08-01", "today"},
	{"Last month", "2025-07-01", "2025-07-31"},
	{"This quarter", "2025-07-01", "today"},
	{"Last quarter", "2025-04-01", "2025-06-30"},
	{"This year", "2025-01-01", "today"},
	{"Last year", "2024-01-01", "2024-12-31"},
}