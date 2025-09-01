package query

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"ga4admin/internal/api"
)

// Executor handles GA4 query execution with caching and result management
type Executor struct {
	dataClient *api.DataClient
}

// NewExecutor creates a new query executor
func NewExecutor(dataClient *api.DataClient) *Executor {
	return &Executor{
		dataClient: dataClient,
	}
}

// Execute runs a query configuration and returns results
func (e *Executor) Execute(ctx context.Context, config *QueryConfig) (*QueryResult, error) {
	startTime := time.Now()

	// Validate query configuration
	if err := e.validateQuery(config); err != nil {
		return nil, fmt.Errorf("query validation failed: %w", err)
	}

	// Convert to GA4 API request
	request, err := e.configToRequest(config)
	if err != nil {
		return nil, fmt.Errorf("failed to convert query config to API request: %w", err)
	}

	// Execute the query
	response, err := e.dataClient.RunReport(ctx, request)
	if err != nil {
		return &QueryResult{
			QueryID:       e.generateQueryID(config),
			PropertyID:    config.PropertyID,
			QueryHash:     e.generateQueryHash(config),
			QueryConfig:   config,
			ExecutedAt:    startTime,
			ExecutionTime: time.Since(startTime).String(),
			Error:         err.Error(),
		}, err
	}

	// Build result object
	result := &QueryResult{
		QueryID:          e.generateQueryID(config),
		PropertyID:       config.PropertyID,
		QueryHash:        e.generateQueryHash(config),
		QueryConfig:      config,
		ExecutedAt:       startTime,
		ExecutionTime:    time.Since(startTime).String(),
		RowCount:         response.RowCount,
		DimensionHeaders: response.DimensionHeaders,
		MetricHeaders:    response.MetricHeaders,
		Rows:             response.Rows,
		Totals:           response.Totals,
		Maximums:         response.Maximums,
		Minimums:         response.Minimums,
		ResponseMetadata: &response.Metadata,
		PropertyQuota:    response.PropertyQuota,
	}

	return result, nil
}

// ExecuteTemplate runs a saved query template with optional parameter overrides
func (e *Executor) ExecuteTemplate(ctx context.Context, template *QueryTemplate, overrides map[string]interface{}) (*QueryResult, error) {
	// Create a copy of the template query
	config := *template.Query

	// Apply parameter overrides
	if err := e.applyOverrides(&config, overrides); err != nil {
		return nil, fmt.Errorf("failed to apply parameter overrides: %w", err)
	}

	// Update template usage statistics
	template.UsageCount++
	now := time.Now()
	template.LastUsed = &now

	return e.Execute(ctx, &config)
}

// validateQuery performs comprehensive query validation
func (e *Executor) validateQuery(config *QueryConfig) error {
	// Required fields
	if config.PropertyID == "" {
		return fmt.Errorf("property ID is required")
	}
	if config.StartDate == "" || config.EndDate == "" {
		return fmt.Errorf("date range is required (start_date and end_date)")
	}
	if len(config.Dimensions) == 0 && len(config.Metrics) == 0 {
		return fmt.Errorf("at least one dimension or metric is required")
	}

	// Limit validation
	if config.Limit > 250000 {
		return fmt.Errorf("limit cannot exceed 250,000 rows")
	}
	if config.Limit <= 0 {
		config.Limit = 10000 // Set default
	}

	// Offset validation
	if config.Offset < 0 {
		return fmt.Errorf("offset cannot be negative")
	}

	// Validate filter configurations
	for i, filter := range config.Filters {
		if err := e.validateFilter(&filter); err != nil {
			return fmt.Errorf("filter %d is invalid: %w", i+1, err)
		}
	}

	// Validate order by configurations
	for i, orderBy := range config.OrderBy {
		if err := e.validateOrderBy(&orderBy, config); err != nil {
			return fmt.Errorf("order by %d is invalid: %w", i+1, err)
		}
	}

	return nil
}

// validateFilter validates individual filter configuration
func (e *Executor) validateFilter(filter *FilterConfig) error {
	if filter.FieldName == "" {
		return fmt.Errorf("field name is required")
	}

	switch filter.Type {
	case "string":
		if filter.StringMatchType == "" {
			filter.StringMatchType = "EXACT" // Default
		}
		if filter.StringValue == "" {
			return fmt.Errorf("string value is required for string filter")
		}
		
		// Validate match type
		validMatchTypes := []string{"EXACT", "CONTAINS", "STARTS_WITH", "ENDS_WITH", "REGEX"}
		if !contains(validMatchTypes, filter.StringMatchType) {
			return fmt.Errorf("invalid string match type: %s", filter.StringMatchType)
		}

	case "numeric":
		if filter.NumericOperation == "" {
			return fmt.Errorf("numeric operation is required for numeric filter")
		}
		
		// Validate operation
		validOperations := []string{"EQUAL", "GREATER_THAN", "LESS_THAN", "GREATER_THAN_OR_EQUAL", "LESS_THAN_OR_EQUAL"}
		if !contains(validOperations, filter.NumericOperation) {
			return fmt.Errorf("invalid numeric operation: %s", filter.NumericOperation)
		}

	case "between":
		if filter.BetweenFrom >= filter.BetweenTo {
			return fmt.Errorf("between filter 'from' value must be less than 'to' value")
		}

	case "in_list":
		if len(filter.InListValues) == 0 {
			return fmt.Errorf("in-list values are required for in_list filter")
		}

	default:
		if filter.Type == "" {
			filter.Type = "string" // Default to string filter
		} else {
			return fmt.Errorf("invalid filter type: %s", filter.Type)
		}
	}

	return nil
}

// validateOrderBy validates order by configuration
func (e *Executor) validateOrderBy(orderBy *OrderByConfig, config *QueryConfig) error {
	if orderBy.FieldName == "" {
		return fmt.Errorf("field name is required for order by")
	}

	// Validate field type and existence
	switch orderBy.FieldType {
	case "dimension":
		if !contains(config.Dimensions, orderBy.FieldName) {
			return fmt.Errorf("dimension '%s' not found in query dimensions", orderBy.FieldName)
		}
		
		// Validate order type for dimensions
		if orderBy.OrderType != "" {
			validTypes := []string{"ALPHANUMERIC", "CASE_INSENSITIVE_ALPHANUMERIC", "NUMERIC"}
			if !contains(validTypes, orderBy.OrderType) {
				return fmt.Errorf("invalid order type for dimension: %s", orderBy.OrderType)
			}
		}

	case "metric":
		if !contains(config.Metrics, orderBy.FieldName) {
			return fmt.Errorf("metric '%s' not found in query metrics", orderBy.FieldName)
		}

	default:
		// Try to determine field type automatically
		if contains(config.Dimensions, orderBy.FieldName) {
			orderBy.FieldType = "dimension"
		} else if contains(config.Metrics, orderBy.FieldName) {
			orderBy.FieldType = "metric"
		} else {
			return fmt.Errorf("field '%s' not found in dimensions or metrics", orderBy.FieldName)
		}
	}

	return nil
}

// configToRequest converts QueryConfig to GA4 API RunReportRequest
func (e *Executor) configToRequest(config *QueryConfig) (*api.RunReportRequest, error) {
	request := &api.RunReportRequest{
		Property: config.PropertyID,
		DateRanges: []api.DateRange{
			{
				StartDate: config.StartDate,
				EndDate:   config.EndDate,
			},
		},
		Limit:                config.Limit,
		Offset:               config.Offset,
		KeepEmptyRows:        config.KeepEmptyRows,
		MetricAggregations:   config.MetricAggregations,
		CurrencyCode:         config.CurrencyCode,
		ReturnPropertyQuota:  config.ReturnPropertyQuota,
	}

	// Convert dimensions
	for _, dimName := range config.Dimensions {
		request.Dimensions = append(request.Dimensions, api.Dimension{Name: dimName})
	}

	// Convert metrics
	for _, metricName := range config.Metrics {
		request.Metrics = append(request.Metrics, api.Metric{Name: metricName})
	}

	// Convert filters
	if len(config.Filters) > 0 {
		filterExpr, err := e.convertFilters(config.Filters)
		if err != nil {
			return nil, fmt.Errorf("failed to convert filters: %w", err)
		}
		
		// Apply dimension filters vs metric filters based on field type
		// For now, assume all filters are dimension filters
		// TODO: Add logic to determine if field is dimension or metric
		request.DimensionFilter = filterExpr
	}

	// Convert order by
	for _, orderBy := range config.OrderBy {
		apiOrderBy := api.OrderBy{
			Desc: orderBy.Descending,
		}

		if orderBy.FieldType == "dimension" {
			apiOrderBy.Dimension = &api.DimensionOrderBy{
				DimensionName: orderBy.FieldName,
				OrderType:     orderBy.OrderType,
			}
		} else {
			apiOrderBy.Metric = &api.MetricOrderBy{
				MetricName: orderBy.FieldName,
			}
		}

		request.OrderBys = append(request.OrderBys, apiOrderBy)
	}

	return request, nil
}

// convertFilters converts filter configurations to GA4 API filter expressions
func (e *Executor) convertFilters(filters []FilterConfig) (*api.FilterExpression, error) {
	if len(filters) == 0 {
		return nil, nil
	}

	// For now, combine all filters with AND logic
	expressions := make([]api.FilterExpression, 0, len(filters))

	for _, filter := range filters {
		expr, err := e.convertSingleFilter(filter)
		if err != nil {
			return nil, err
		}
		expressions = append(expressions, *expr)
	}

	if len(expressions) == 1 {
		return &expressions[0], nil
	}

	// Combine multiple filters with AND
	return &api.FilterExpression{
		AndGroup: &api.FilterExpressionList{
			Expressions: expressions,
		},
	}, nil
}

// convertSingleFilter converts a single filter to GA4 API filter expression
func (e *Executor) convertSingleFilter(filter FilterConfig) (*api.FilterExpression, error) {
	apiFilter := &api.Filter{
		FieldName: filter.FieldName,
	}

	switch filter.Type {
	case "string":
		apiFilter.StringFilter = &api.StringFilter{
			MatchType:     filter.StringMatchType,
			Value:         filter.StringValue,
			CaseSensitive: filter.StringCaseSensitive,
		}

	case "numeric":
		value := api.NumericValue{}
		if filter.NumericValue == float64(int64(filter.NumericValue)) {
			// Integer value
			value.Int64Value = strconv.FormatInt(int64(filter.NumericValue), 10)
		} else {
			// Float value
			value.DoubleValue = strconv.FormatFloat(filter.NumericValue, 'f', -1, 64)
		}

		apiFilter.NumericFilter = &api.NumericFilter{
			Operation: filter.NumericOperation,
			Value:     value,
		}

	case "between":
		fromValue := api.NumericValue{}
		toValue := api.NumericValue{}

		if filter.BetweenFrom == float64(int64(filter.BetweenFrom)) {
			fromValue.Int64Value = strconv.FormatInt(int64(filter.BetweenFrom), 10)
		} else {
			fromValue.DoubleValue = strconv.FormatFloat(filter.BetweenFrom, 'f', -1, 64)
		}

		if filter.BetweenTo == float64(int64(filter.BetweenTo)) {
			toValue.Int64Value = strconv.FormatInt(int64(filter.BetweenTo), 10)
		} else {
			toValue.DoubleValue = strconv.FormatFloat(filter.BetweenTo, 'f', -1, 64)
		}

		apiFilter.BetweenFilter = &api.BetweenFilter{
			FromValue: fromValue,
			ToValue:   toValue,
		}

	case "in_list":
		apiFilter.InListFilter = &api.InListFilter{
			Values:        filter.InListValues,
			CaseSensitive: filter.InListCaseSensitive,
		}

	default:
		return nil, fmt.Errorf("unsupported filter type: %s", filter.Type)
	}

	return &api.FilterExpression{
		Filter: apiFilter,
	}, nil
}

// applyOverrides applies parameter overrides to a query configuration
func (e *Executor) applyOverrides(config *QueryConfig, overrides map[string]interface{}) error {
	for key, value := range overrides {
		switch key {
		case "start_date":
			if str, ok := value.(string); ok {
				config.StartDate = str
			}
		case "end_date":
			if str, ok := value.(string); ok {
				config.EndDate = str
			}
		case "limit":
			if num, ok := value.(int64); ok {
				config.Limit = num
			} else if num, ok := value.(int); ok {
				config.Limit = int64(num)
			}
		case "offset":
			if num, ok := value.(int64); ok {
				config.Offset = num
			} else if num, ok := value.(int); ok {
				config.Offset = int64(num)
			}
		}
	}
	return nil
}

// generateQueryID creates a unique identifier for a query
func (e *Executor) generateQueryID(config *QueryConfig) string {
	return fmt.Sprintf("query_%d", time.Now().Unix())
}

// generateQueryHash creates a hash for caching purposes
func (e *Executor) generateQueryHash(config *QueryConfig) string {
	// Create a deterministic representation for hashing
	data, _ := json.Marshal(config)
	hash := sha256.Sum256(data)
	return fmt.Sprintf("%x", hash)
}

// Helper function to check if slice contains string
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}