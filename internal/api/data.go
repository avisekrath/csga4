package api

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// DataClient handles GA4 Data API operations
type DataClient struct {
	authClient *AuthClient
	baseURL    string
	cacheClient CacheInterface // Interface for pluggable caching
}

// CacheInterface defines the caching contract
type CacheInterface interface {
	GetCachedMetadata(ctx context.Context, propertyID, cacheType string, result interface{}) (bool, error)
	CacheMetadata(ctx context.Context, propertyID, cacheType string, data interface{}, ttlHours int) error
	GetCachedQuery(ctx context.Context, queryHash string, queryParams, resultData interface{}) (bool, error)
	CacheQuery(ctx context.Context, queryID, propertyID, queryHash string, queryParams, resultData interface{}, rowCount int, ttlHours *int) error
	Close() error
}

// NewDataClient creates a new GA4 Data API client
func NewDataClient() (*DataClient, error) {
	authClient, err := NewAuthClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create auth client: %w", err)
	}

	return &DataClient{
		authClient: authClient,
		baseURL:    "https://analyticsdata.googleapis.com/v1beta",
	}, nil
}

// NewDataClientWithCache creates a new GA4 Data API client with caching
func NewDataClientWithCache(cacheClient CacheInterface) (*DataClient, error) {
	authClient, err := NewAuthClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create auth client: %w", err)
	}

	return &DataClient{
		authClient:  authClient,
		baseURL:     "https://analyticsdata.googleapis.com/v1beta",
		cacheClient: cacheClient,
	}, nil
}

// Close closes any resources (like cache connections)
func (c *DataClient) Close() error {
	if c.cacheClient != nil {
		return c.cacheClient.Close()
	}
	return nil
}

// GA4 Data API response structures
type MetadataResponse struct {
	Name        string               `json:"name"`
	Dimensions  []DimensionMetadata  `json:"dimensions"`
	Metrics     []MetricMetadata     `json:"metrics"`
	Comparisons []ComparisonMetadata `json:"comparisons"`
}

type DimensionMetadata struct {
	APIName                string   `json:"apiName"`
	UIName                 string   `json:"uiName"`
	Description            string   `json:"description"`
	DeprecatedAPINames     []string `json:"deprecatedApiNames"`
	CustomDefinition       bool     `json:"customDefinition"`
	Category               string   `json:"category"`
}

type MetricMetadata struct {
	APIName                string   `json:"apiName"`
	UIName                 string   `json:"uiName"`
	Description            string   `json:"description"`
	Type                   string   `json:"type"`
	Expression             string   `json:"expression"`
	CustomDefinition       bool     `json:"customDefinition"`
	Category               string   `json:"category"`
	DeprecatedAPINames     []string `json:"deprecatedApiNames"`
	RestrictedMetricType   []string `json:"restrictedMetricType"`
}

type ComparisonMetadata struct {
	APIName     string `json:"apiName"`
	UIName      string `json:"uiName"`
	Description string `json:"description"`
}

// RunReport API structures
type RunReportRequest struct {
	Property             string               `json:"-"`                                 // Property ID (not in JSON body)
	Dimensions           []Dimension          `json:"dimensions,omitempty"`
	Metrics              []Metric             `json:"metrics,omitempty"`
	DateRanges           []DateRange          `json:"dateRanges"`
	DimensionFilter      *FilterExpression    `json:"dimensionFilter,omitempty"`
	MetricFilter         *FilterExpression    `json:"metricFilter,omitempty"`
	Offset               int64                `json:"offset,omitempty"`
	Limit                int64                `json:"limit,omitempty"`
	MetricAggregations   []string             `json:"metricAggregations,omitempty"`
	OrderBys             []OrderBy            `json:"orderBys,omitempty"`
	CurrencyCode         string               `json:"currencyCode,omitempty"`
	KeepEmptyRows        bool                 `json:"keepEmptyRows,omitempty"`
	ReturnPropertyQuota  bool                 `json:"returnPropertyQuota,omitempty"`
}

type RunReportResponse struct {
	DimensionHeaders []DimensionHeader `json:"dimensionHeaders"`
	MetricHeaders    []MetricHeader    `json:"metricHeaders"`
	Rows             []Row             `json:"rows"`
	Totals           []Row             `json:"totals"`
	Maximums         []Row             `json:"maximums"`
	Minimums         []Row             `json:"minimums"`
	RowCount         int               `json:"rowCount"`
	Metadata         ResponseMetadata  `json:"metadata"`
	PropertyQuota    *PropertyQuota    `json:"propertyQuota"`
	Kind             string            `json:"kind"`
}

type Dimension struct {
	Name                string `json:"name"`
	DimensionExpression string `json:"dimensionExpression,omitempty"`
}

type Metric struct {
	Name       string `json:"name"`
	Expression string `json:"expression,omitempty"`
}

type DateRange struct {
	StartDate string `json:"startDate"`
	EndDate   string `json:"endDate"`
	Name      string `json:"name,omitempty"`
}

type FilterExpression struct {
	AndGroup  *FilterExpressionList `json:"andGroup,omitempty"`
	OrGroup   *FilterExpressionList `json:"orGroup,omitempty"`
	NotExpression *FilterExpression `json:"notExpression,omitempty"`
	Filter    *Filter               `json:"filter,omitempty"`
}

type FilterExpressionList struct {
	Expressions []FilterExpression `json:"expressions"`
}

type Filter struct {
	FieldName     string         `json:"fieldName"`
	StringFilter  *StringFilter  `json:"stringFilter,omitempty"`
	NumericFilter *NumericFilter `json:"numericFilter,omitempty"`
	BetweenFilter *BetweenFilter `json:"betweenFilter,omitempty"`
	InListFilter  *InListFilter  `json:"inListFilter,omitempty"`
}

type StringFilter struct {
	MatchType     string `json:"matchType"`     // EXACT, CONTAINS, STARTS_WITH, ENDS_WITH, REGEX
	Value         string `json:"value"`
	CaseSensitive bool   `json:"caseSensitive"`
}

type NumericFilter struct {
	Operation string       `json:"operation"` // EQUAL, LESS_THAN, GREATER_THAN, etc.
	Value     NumericValue `json:"value"`
}

type BetweenFilter struct {
	FromValue NumericValue `json:"fromValue"`
	ToValue   NumericValue `json:"toValue"`
}

type InListFilter struct {
	Values        []string `json:"values"`
	CaseSensitive bool     `json:"caseSensitive"`
}

type NumericValue struct {
	Int64Value  string `json:"int64Value,omitempty"`
	DoubleValue string `json:"doubleValue,omitempty"`
}

type OrderBy struct {
	Desc       bool                `json:"desc,omitempty"`
	Dimension  *DimensionOrderBy   `json:"dimension,omitempty"`
	Metric     *MetricOrderBy      `json:"metric,omitempty"`
}

type DimensionOrderBy struct {
	DimensionName string `json:"dimensionName"`
	OrderType     string `json:"orderType,omitempty"` // ALPHANUMERIC, CASE_INSENSITIVE_ALPHANUMERIC, NUMERIC
}

type MetricOrderBy struct {
	MetricName string `json:"metricName"`
}

type DimensionHeader struct {
	Name string `json:"name"`
}

type MetricHeader struct {
	Name string `json:"name"`
	Type string `json:"type"` // TYPE_INTEGER, TYPE_FLOAT, TYPE_SECONDS, TYPE_CURRENCY, etc.
}

type Row struct {
	DimensionValues []DimensionValue `json:"dimensionValues"`
	MetricValues    []MetricValue    `json:"metricValues"`
}

type DimensionValue struct {
	Value string `json:"value"`
}

type MetricValue struct {
	Value string `json:"value"`
}

type ResponseMetadata struct {
	CurrencyCode                string `json:"currencyCode"`
	TimeZone                    string `json:"timeZone"`
	EmptyReason                 string `json:"emptyReason,omitempty"`
	DataLossFromOtherRow       bool   `json:"dataLossFromOtherRow,omitempty"`
}

type PropertyQuota struct {
	TokensPerDay      *QuotaStatus `json:"tokensPerDay,omitempty"`
	TokensPerHour     *QuotaStatus `json:"tokensPerHour,omitempty"`
	ConcurrentRequests *QuotaStatus `json:"concurrentRequests,omitempty"`
	ServerErrorsPerProjectPerHour *QuotaStatus `json:"serverErrorsPerProjectPerHour,omitempty"`
}

type QuotaStatus struct {
	Consumed   int    `json:"consumed,omitempty"`
	Remaining  int    `json:"remaining,omitempty"`
}

// GetMetadata retrieves all dimensions and metrics available for a GA4 property
func (c *DataClient) GetMetadata(ctx context.Context, propertyID string) (*MetadataResponse, error) {
	// Try cache first if available
	if c.cacheClient != nil {
		var cached MetadataResponse
		if found, err := c.cacheClient.GetCachedMetadata(ctx, propertyID, "metadata", &cached); err == nil && found {
			return &cached, nil
		}
	}

	httpClient, err := c.authClient.AuthenticatedHTTPClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get authenticated HTTP client: %w", err)
	}

	url := fmt.Sprintf("%s/properties/%s/metadata", c.baseURL, propertyID)
	resp, err := httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to make request to GA4 Data API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("property %s not found or not accessible", propertyID)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GA4 Data API returned status %d: %s", resp.StatusCode, resp.Status)
	}

	var metadata MetadataResponse
	if err := json.NewDecoder(resp.Body).Decode(&metadata); err != nil {
		return nil, fmt.Errorf("failed to decode metadata response: %w", err)
	}

	// Cache the result for 24 hours if caching is available
	if c.cacheClient != nil {
		c.cacheClient.CacheMetadata(ctx, propertyID, "metadata", metadata, 24)
	}

	return &metadata, nil
}

// RunReport executes a GA4 report query
func (c *DataClient) RunReport(ctx context.Context, request *RunReportRequest) (*RunReportResponse, error) {
	// Validate required fields
	if request.Property == "" {
		return nil, fmt.Errorf("property ID is required")
	}
	if len(request.DateRanges) == 0 {
		return nil, fmt.Errorf("at least one date range is required")
	}

	// Set default limit if not specified
	if request.Limit == 0 {
		request.Limit = 10000 // GA4 default
	}

	// Validate limit
	if request.Limit > 250000 {
		return nil, fmt.Errorf("limit cannot exceed 250,000 rows")
	}

	// Try cache first if available
	var queryHash string
	if c.cacheClient != nil {
		queryHash = c.generateQueryHash(request)
		var cached RunReportResponse
		if found, err := c.cacheClient.GetCachedQuery(ctx, queryHash, request, &cached); err == nil && found {
			return &cached, nil
		}
	}

	httpClient, err := c.authClient.AuthenticatedHTTPClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get authenticated HTTP client: %w", err)
	}

	url := fmt.Sprintf("%s/properties/%s:runReport", c.baseURL, request.Property)
	
	jsonData, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := httpClient.Post(url, "application/json", 
		strings.NewReader(string(jsonData)))
	if err != nil {
		return nil, fmt.Errorf("failed to make request to GA4 Data API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("property %s not found or not accessible", request.Property)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GA4 Data API returned status %d: %s", resp.StatusCode, resp.Status)
	}

	var reportResponse RunReportResponse
	if err := json.NewDecoder(resp.Body).Decode(&reportResponse); err != nil {
		return nil, fmt.Errorf("failed to decode report response: %w", err)
	}

	// Cache the result for 1 hour if caching is available
	if c.cacheClient != nil && queryHash != "" {
		queryID := fmt.Sprintf("query_%d", time.Now().Unix())
		ttl := 1 // 1 hour for query results
		c.cacheClient.CacheQuery(ctx, queryID, request.Property, queryHash, request, reportResponse, reportResponse.RowCount, &ttl)
	}

	return &reportResponse, nil
}

// generateQueryHash creates a unique hash for a query request
func (c *DataClient) generateQueryHash(request *RunReportRequest) string {
	// Create a deterministic JSON representation
	jsonData, _ := json.Marshal(request)
	hash := sha256.Sum256(jsonData)
	return fmt.Sprintf("%x", hash)
}

// AnalyzeEvents performs event volume analysis for a property
func (c *DataClient) AnalyzeEvents(ctx context.Context, propertyID string, days int) (*EventAnalysis, error) {
	// Validate parameters
	if days <= 0 || days > 365 {
		return nil, fmt.Errorf("days must be between 1 and 365")
	}

	cacheKey := fmt.Sprintf("events_%d", days)
	
	// Try cache first if available (1 hour TTL for events)
	if c.cacheClient != nil {
		var cached EventAnalysis
		if found, err := c.cacheClient.GetCachedMetadata(ctx, propertyID, cacheKey, &cached); err == nil && found {
			return &cached, nil
		}
	}

	// Build report request for event analysis
	request := &RunReportRequest{
		Property: propertyID,
		Dimensions: []Dimension{
			{Name: "eventName"},
		},
		Metrics: []Metric{
			{Name: "eventCount"},
			{Name: "activeUsers"},
		},
		DateRanges: []DateRange{
			{
				StartDate: fmt.Sprintf("%ddaysAgo", days),
				EndDate:   "yesterday",
			},
		},
		OrderBys: []OrderBy{
			{
				Desc: true,
				Metric: &MetricOrderBy{
					MetricName: "eventCount",
				},
			},
		},
		Limit: 100, // Top 100 events
	}

	reportResponse, err := c.RunReport(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("failed to run event analysis report: %w", err)
	}

	// Process the response into business-friendly format
	analysis := &EventAnalysis{
		PropertyID:    propertyID,
		DateRange:     fmt.Sprintf("%d days", days),
		TotalEvents:   len(reportResponse.Rows),
		AnalyzedAt:    time.Now(),
		Events:        make([]EventSummary, 0, len(reportResponse.Rows)),
	}

	var totalEventCount int64
	var totalUsers int64

	for _, row := range reportResponse.Rows {
		if len(row.DimensionValues) > 0 && len(row.MetricValues) >= 2 {
			eventName := row.DimensionValues[0].Value
			eventCount, _ := strconv.ParseInt(row.MetricValues[0].Value, 10, 64)
			activeUsers, _ := strconv.ParseInt(row.MetricValues[1].Value, 10, 64)

			totalEventCount += eventCount
			totalUsers += activeUsers

			analysis.Events = append(analysis.Events, EventSummary{
				EventName:    eventName,
				EventCount:   eventCount,
				ActiveUsers:  activeUsers,
				EventsPerUser: float64(eventCount) / float64(activeUsers),
			})
		}
	}

	analysis.TotalEventCount = totalEventCount
	analysis.TotalActiveUsers = totalUsers

	// Cache the result for 1 hour if caching is available
	if c.cacheClient != nil {
		c.cacheClient.CacheMetadata(ctx, propertyID, cacheKey, *analysis, 1)
	}

	return analysis, nil
}

// EventAnalysis represents the results of event volume analysis
type EventAnalysis struct {
	PropertyID       string         `json:"property_id"`
	DateRange        string         `json:"date_range"`
	TotalEvents      int            `json:"total_events"`
	TotalEventCount  int64          `json:"total_event_count"`
	TotalActiveUsers int64          `json:"total_active_users"`
	AnalyzedAt       time.Time      `json:"analyzed_at"`
	Events           []EventSummary `json:"events"`
}

type EventSummary struct {
	EventName     string  `json:"event_name"`
	EventCount    int64   `json:"event_count"`
	ActiveUsers   int64   `json:"active_users"`
	EventsPerUser float64 `json:"events_per_user"`
}

// Helper method to create common date ranges
func NewDateRange(startDate, endDate string) DateRange {
	return DateRange{
		StartDate: startDate,
		EndDate:   endDate,
	}
}

// Helper method to create dimension filter
func NewDimensionFilter(dimensionName, value string) *FilterExpression {
	return &FilterExpression{
		Filter: &Filter{
			FieldName: dimensionName,
			StringFilter: &StringFilter{
				MatchType: "EXACT",
				Value:     value,
			},
		},
	}
}

// Helper method to create metric filter
func NewMetricFilter(metricName string, operation string, value int64) *FilterExpression {
	return &FilterExpression{
		Filter: &Filter{
			FieldName: metricName,
			NumericFilter: &NumericFilter{
				Operation: operation,
				Value: NumericValue{
					Int64Value: strconv.FormatInt(value, 10),
				},
			},
		},
	}
}