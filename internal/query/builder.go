package query

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"ga4admin/internal/api"
)

// QueryBuilder provides interactive query construction capabilities
type QueryBuilder struct {
	dataClient *api.DataClient
	propertyID string
	metadata   *api.MetadataResponse
}

// NewQueryBuilder creates a new query builder for a property
func NewQueryBuilder(dataClient *api.DataClient, propertyID string) *QueryBuilder {
	return &QueryBuilder{
		dataClient: dataClient,
		propertyID: propertyID,
	}
}

// LoadMetadata loads property metadata for dimension/metric selection
func (qb *QueryBuilder) LoadMetadata(ctx context.Context) error {
	metadata, err := qb.dataClient.GetMetadata(ctx, qb.propertyID)
	if err != nil {
		return fmt.Errorf("failed to load metadata: %w", err)
	}
	qb.metadata = metadata
	return nil
}

// BuildInteractively creates a query through interactive prompts
func (qb *QueryBuilder) BuildInteractively(ctx context.Context) (*QueryConfig, error) {
	if qb.metadata == nil {
		if err := qb.LoadMetadata(ctx); err != nil {
			return nil, err
		}
	}

	config := &QueryConfig{
		PropertyID: qb.propertyID,
		Dimensions: make([]string, 0),
		Metrics:    make([]string, 0),
		Filters:    make([]FilterConfig, 0),
	}

	fmt.Println("🔧 Interactive Query Builder")
	fmt.Printf("📊 Property: %s\n", qb.propertyID)
	fmt.Println()

	// Step 1: Date Range
	if err := qb.configureDateRange(config); err != nil {
		return nil, err
	}

	// Step 2: Dimensions
	if err := qb.configureDimensions(config); err != nil {
		return nil, err
	}

	// Step 3: Metrics
	if err := qb.configureMetrics(config); err != nil {
		return nil, err
	}

	// Step 4: Filters (optional)
	if err := qb.configureFilters(config); err != nil {
		return nil, err
	}

	// Step 5: Query Options
	if err := qb.configureOptions(config); err != nil {
		return nil, err
	}

	return config, nil
}

// GetAvailableDimensions returns filtered dimensions for selection
func (qb *QueryBuilder) GetAvailableDimensions(customOnly bool, category string) []api.DimensionMetadata {
	if qb.metadata == nil {
		return nil
	}

	filtered := make([]api.DimensionMetadata, 0)
	for _, dim := range qb.metadata.Dimensions {
		if customOnly && !dim.CustomDefinition {
			continue
		}
		if category != "" && dim.Category != category {
			continue
		}
		filtered = append(filtered, dim)
	}
	return filtered
}

// GetAvailableMetrics returns filtered metrics for selection
func (qb *QueryBuilder) GetAvailableMetrics(customOnly bool, category string) []api.MetricMetadata {
	if qb.metadata == nil {
		return nil
	}

	filtered := make([]api.MetricMetadata, 0)
	for _, metric := range qb.metadata.Metrics {
		if customOnly && !metric.CustomDefinition {
			continue
		}
		if category != "" && metric.Category != category {
			continue
		}
		filtered = append(filtered, metric)
	}
	return filtered
}

// ValidateQuery checks if the query configuration is valid
func (qb *QueryBuilder) ValidateQuery(config *QueryConfig) error {
	// Basic validation
	if config.PropertyID == "" {
		return fmt.Errorf("property ID is required")
	}
	if len(config.Dimensions) == 0 && len(config.Metrics) == 0 {
		return fmt.Errorf("at least one dimension or metric is required")
	}
	if config.StartDate == "" || config.EndDate == "" {
		return fmt.Errorf("date range is required")
	}

	// Validate date format
	if _, err := time.Parse("2006-01-02", config.StartDate); err != nil && !isRelativeDate(config.StartDate) {
		return fmt.Errorf("invalid start date format: %s", config.StartDate)
	}
	if _, err := time.Parse("2006-01-02", config.EndDate); err != nil && !isRelativeDate(config.EndDate) {
		return fmt.Errorf("invalid end date format: %s", config.EndDate)
	}

	// Validate dimensions exist
	if qb.metadata != nil {
		for _, dimName := range config.Dimensions {
			if !qb.dimensionExists(dimName) {
				return fmt.Errorf("dimension '%s' not found in property", dimName)
			}
		}

		// Validate metrics exist
		for _, metricName := range config.Metrics {
			if !qb.metricExists(metricName) {
				return fmt.Errorf("metric '%s' not found in property", metricName)
			}
		}
	}

	return nil
}

// Helper methods for interactive configuration
func (qb *QueryBuilder) configureDateRange(config *QueryConfig) error {
	fmt.Println("📅 Step 1: Date Range")
	fmt.Println("Choose date range:")
	fmt.Println("  1. Last 7 days")
	fmt.Println("  2. Last 30 days")
	fmt.Println("  3. Last 90 days")
	fmt.Println("  4. Custom date range")
	fmt.Print("Selection (1-4): ")

	var choice string
	fmt.Scanln(&choice)

	switch choice {
	case "1":
		config.StartDate = "7daysAgo"
		config.EndDate = "yesterday"
	case "2":
		config.StartDate = "30daysAgo"
		config.EndDate = "yesterday"
	case "3":
		config.StartDate = "90daysAgo"
		config.EndDate = "yesterday"
	case "4":
		fmt.Print("Start date (YYYY-MM-DD or relative like '30daysAgo'): ")
		fmt.Scanln(&config.StartDate)
		fmt.Print("End date (YYYY-MM-DD or relative like 'yesterday'): ")
		fmt.Scanln(&config.EndDate)
	default:
		return fmt.Errorf("invalid date range selection")
	}

	fmt.Printf("✅ Date range: %s to %s\n\n", config.StartDate, config.EndDate)
	return nil
}

func (qb *QueryBuilder) configureDimensions(config *QueryConfig) error {
	fmt.Println("📏 Step 2: Dimensions")
	fmt.Println("Select dimensions (comma-separated list or 'none'):")
	
	// Show common dimensions
	commonDims := []string{"sessionSource", "sessionMedium", "sessionCampaignName", "country", "deviceCategory"}
	fmt.Println("Common dimensions:", strings.Join(commonDims, ", "))
	fmt.Print("Dimensions: ")

	var input string
	fmt.Scanln(&input)
	
	if strings.ToLower(strings.TrimSpace(input)) == "none" {
		return nil
	}

	dimensions := strings.Split(input, ",")
	for _, dim := range dimensions {
		dim = strings.TrimSpace(dim)
		if dim != "" {
			config.Dimensions = append(config.Dimensions, dim)
		}
	}

	fmt.Printf("✅ Selected %d dimension(s)\n\n", len(config.Dimensions))
	return nil
}

func (qb *QueryBuilder) configureMetrics(config *QueryConfig) error {
	fmt.Println("📈 Step 3: Metrics")
	fmt.Println("Select metrics (comma-separated list or 'none'):")
	
	// Show common metrics
	commonMetrics := []string{"activeUsers", "sessions", "screenPageViews", "eventCount"}
	fmt.Println("Common metrics:", strings.Join(commonMetrics, ", "))
	fmt.Print("Metrics: ")

	var input string
	fmt.Scanln(&input)
	
	if strings.ToLower(strings.TrimSpace(input)) == "none" {
		return nil
	}

	metrics := strings.Split(input, ",")
	for _, metric := range metrics {
		metric = strings.TrimSpace(metric)
		if metric != "" {
			config.Metrics = append(config.Metrics, metric)
		}
	}

	fmt.Printf("✅ Selected %d metric(s)\n\n", len(config.Metrics))
	return nil
}

func (qb *QueryBuilder) configureFilters(config *QueryConfig) error {
	fmt.Println("🔍 Step 4: Filters (Optional)")
	fmt.Print("Add filters? (y/N): ")

	var addFilters string
	fmt.Scanln(&addFilters)
	
	if strings.ToLower(strings.TrimSpace(addFilters)) != "y" {
		return nil
	}

	for {
		filter := FilterConfig{}
		
		fmt.Print("Filter field name: ")
		fmt.Scanln(&filter.FieldName)
		
		fmt.Println("Filter type: 1=String, 2=Numeric")
		fmt.Print("Type (1-2): ")
		var filterType string
		fmt.Scanln(&filterType)
		
		switch filterType {
		case "1":
			filter.Type = "string"
			fmt.Print("Match type (EXACT, CONTAINS, STARTS_WITH, ENDS_WITH): ")
			fmt.Scanln(&filter.StringMatchType)
			fmt.Print("Value: ")
			fmt.Scanln(&filter.StringValue)
		case "2":
			filter.Type = "numeric"
			fmt.Print("Operation (EQUAL, GREATER_THAN, LESS_THAN): ")
			fmt.Scanln(&filter.NumericOperation)
			fmt.Print("Value: ")
			var valueStr string
			fmt.Scanln(&valueStr)
			if value, err := strconv.ParseFloat(valueStr, 64); err == nil {
				filter.NumericValue = value
			}
		}
		
		config.Filters = append(config.Filters, filter)
		
		fmt.Print("Add another filter? (y/N): ")
		var more string
		fmt.Scanln(&more)
		if strings.ToLower(strings.TrimSpace(more)) != "y" {
			break
		}
	}

	fmt.Printf("✅ Added %d filter(s)\n\n", len(config.Filters))
	return nil
}

func (qb *QueryBuilder) configureOptions(config *QueryConfig) error {
	fmt.Println("⚙️ Step 5: Query Options")
	
	fmt.Print("Result limit (default 10000): ")
	var limitStr string
	fmt.Scanln(&limitStr)
	
	if limitStr != "" {
		if limit, err := strconv.ParseInt(limitStr, 10, 64); err == nil {
			config.Limit = limit
		}
	}
	if config.Limit == 0 {
		config.Limit = 10000
	}

	fmt.Print("Query name (for saving, optional): ")
	fmt.Scanln(&config.Name)

	fmt.Printf("✅ Query configured with limit %d\n\n", config.Limit)
	return nil
}

// Helper validation methods
func (qb *QueryBuilder) dimensionExists(name string) bool {
	if qb.metadata == nil {
		return true // Skip validation if metadata not loaded
	}
	for _, dim := range qb.metadata.Dimensions {
		if dim.APIName == name {
			return true
		}
	}
	return false
}

func (qb *QueryBuilder) metricExists(name string) bool {
	if qb.metadata == nil {
		return true // Skip validation if metadata not loaded
	}
	for _, metric := range qb.metadata.Metrics {
		if metric.APIName == name {
			return true
		}
	}
	return false
}

func isRelativeDate(date string) bool {
	relativeDates := []string{"today", "yesterday", "daysAgo", "weeksAgo", "monthsAgo", "yearsAgo"}
	for _, relative := range relativeDates {
		if strings.Contains(date, relative) {
			return true
		}
	}
	return false
}