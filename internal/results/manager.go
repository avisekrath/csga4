package results

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"ga4admin/internal/cache"
	"ga4admin/internal/query"
)

// Manager handles query result storage, retrieval, and export
type Manager struct {
	cacheClient *cache.CacheClient
}

// NewManager creates a new results manager
func NewManager(cacheClient *cache.CacheClient) *Manager {
	return &Manager{
		cacheClient: cacheClient,
	}
}

// ListResults returns all cached query results for a property
func (m *Manager) ListResults(ctx context.Context, propertyID string, limit int) ([]ResultSummary, error) {
	// For now, return empty list as we need to implement proper SQL query interface
	// This is a placeholder implementation
	return []ResultSummary{}, nil
}

// GetResult retrieves a specific query result by ID
func (m *Manager) GetResult(ctx context.Context, queryID string) (*query.QueryResult, error) {
	// Placeholder implementation
	return nil, fmt.Errorf("result not found: %s", queryID)
}

// ExportToCSV exports query results to CSV format
func (m *Manager) ExportToCSV(ctx context.Context, queryID string, outputPath string) error {
	// Get the result
	result, err := m.GetResult(ctx, queryID)
	if err != nil {
		return fmt.Errorf("failed to get result: %w", err)
	}

	// Create output directory if needed
	dir := filepath.Dir(outputPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Create CSV file
	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create CSV file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write headers
	headers := make([]string, 0, len(result.DimensionHeaders)+len(result.MetricHeaders))
	for _, dim := range result.DimensionHeaders {
		headers = append(headers, dim.Name)
	}
	for _, metric := range result.MetricHeaders {
		headers = append(headers, metric.Name)
	}
	
	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("failed to write CSV headers: %w", err)
	}

	// Write data rows
	for _, row := range result.Rows {
		record := make([]string, 0, len(row.DimensionValues)+len(row.MetricValues))
		
		for _, dimValue := range row.DimensionValues {
			record = append(record, dimValue.Value)
		}
		for _, metricValue := range row.MetricValues {
			record = append(record, metricValue.Value)
		}
		
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write CSV row: %w", err)
		}
	}

	return nil
}

// ExportToJSON exports query results to JSON format
func (m *Manager) ExportToJSON(ctx context.Context, queryID string, outputPath string, prettify bool) error {
	// Get the result
	result, err := m.GetResult(ctx, queryID)
	if err != nil {
		return fmt.Errorf("failed to get result: %w", err)
	}

	// Create output directory if needed
	dir := filepath.Dir(outputPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Create JSON file
	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create JSON file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if prettify {
		encoder.SetIndent("", "  ")
	}

	if err := encoder.Encode(result); err != nil {
		return fmt.Errorf("failed to write JSON: %w", err)
	}

	return nil
}

// GetResultStats returns statistics about cached results
func (m *Manager) GetResultStats(ctx context.Context, propertyID string) (*ResultStats, error) {
	// Placeholder implementation
	return &ResultStats{
		PropertyID:       propertyID,
		TotalResults:     0,
		ActiveResults:    0,
		ExpiredResults:   0,
		TotalRows:        0,
		AvgRowsPerResult: 0,
		GeneratedAt:      time.Now(),
	}, nil
}

// FormatResultTable formats query results for console display
func (m *Manager) FormatResultTable(result *query.QueryResult, maxRows int, maxWidth int) ([]string, error) {
	if len(result.Rows) == 0 {
		return []string{"No data returned"}, nil
	}

	// Build headers
	headers := make([]string, 0, len(result.DimensionHeaders)+len(result.MetricHeaders))
	for _, dim := range result.DimensionHeaders {
		headers = append(headers, dim.Name)
	}
	for _, metric := range result.MetricHeaders {
		headers = append(headers, metric.Name)
	}

	// Limit rows for display
	displayRows := result.Rows
	if maxRows > 0 && len(displayRows) > maxRows {
		displayRows = displayRows[:maxRows]
	}

	// Calculate column widths
	colWidths := make([]int, len(headers))
	for i, header := range headers {
		colWidths[i] = len(header)
	}

	// Check data for column widths
	for _, row := range displayRows {
		for i, dimValue := range row.DimensionValues {
			if i < len(colWidths) && len(dimValue.Value) > colWidths[i] {
				colWidths[i] = min(len(dimValue.Value), maxWidth)
			}
		}
		for i, metricValue := range row.MetricValues {
			colIndex := len(row.DimensionValues) + i
			if colIndex < len(colWidths) && len(metricValue.Value) > colWidths[colIndex] {
				colWidths[colIndex] = min(len(metricValue.Value), maxWidth)
			}
		}
	}

	var lines []string
	
	// Header line
	headerParts := make([]string, len(headers))
	for i, header := range headers {
		headerParts[i] = padOrTruncate(header, colWidths[i])
	}
	lines = append(lines, "| "+strings.Join(headerParts, " | ")+" |")
	
	// Separator line
	separatorParts := make([]string, len(headers))
	for i, width := range colWidths {
		separatorParts[i] = strings.Repeat("-", width)
	}
	lines = append(lines, "|"+strings.Join(separatorParts, "|")+"|")
	
	// Data lines
	for _, row := range displayRows {
		rowParts := make([]string, len(headers))
		
		// Dimension values
		for i, dimValue := range row.DimensionValues {
			if i < len(rowParts) {
				rowParts[i] = padOrTruncate(dimValue.Value, colWidths[i])
			}
		}
		
		// Metric values
		for i, metricValue := range row.MetricValues {
			colIndex := len(row.DimensionValues) + i
			if colIndex < len(rowParts) {
				// Format numeric values
				if val, err := strconv.ParseFloat(metricValue.Value, 64); err == nil {
					if val == float64(int64(val)) {
						rowParts[colIndex] = padOrTruncate(fmt.Sprintf("%.0f", val), colWidths[colIndex])
					} else {
						rowParts[colIndex] = padOrTruncate(fmt.Sprintf("%.2f", val), colWidths[colIndex])
					}
				} else {
					rowParts[colIndex] = padOrTruncate(metricValue.Value, colWidths[colIndex])
				}
			}
		}
		
		lines = append(lines, "| "+strings.Join(rowParts, " | ")+" |")
	}

	// Add summary if rows were truncated
	if len(result.Rows) > maxRows {
		lines = append(lines, "")
		lines = append(lines, fmt.Sprintf("Showing %d of %d rows", maxRows, len(result.Rows)))
	}

	return lines, nil
}

// Helper functions
func padOrTruncate(s string, width int) string {
	if len(s) > width {
		if width > 3 {
			return s[:width-3] + "..."
		}
		return s[:width]
	}
	return s + strings.Repeat(" ", width-len(s))
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}