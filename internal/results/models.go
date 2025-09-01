package results

import "time"

// ResultSummary represents a summary of a cached query result
type ResultSummary struct {
	QueryID      string     `json:"query_id"`
	PropertyID   string     `json:"property_id"`
	QueryHash    string     `json:"query_hash"`
	RowCount     int        `json:"row_count"`
	CreatedAt    time.Time  `json:"created_at"`
	LastAccessed time.Time  `json:"last_accessed"`
	ExpiresAt    *time.Time `json:"expires_at,omitempty"`
	IsExpired    bool       `json:"is_expired"`
	TableName    string     `json:"table_name,omitempty"`
	Description  string     `json:"description,omitempty"`
}

// ResultStats represents statistics about cached results for a property
type ResultStats struct {
	PropertyID        string     `json:"property_id"`
	TotalResults      int        `json:"total_results"`
	ActiveResults     int        `json:"active_results"`
	ExpiredResults    int        `json:"expired_results"`
	TotalRows         int64      `json:"total_rows"`
	AvgRowsPerResult  float64    `json:"avg_rows_per_result"`
	OldestResult      *time.Time `json:"oldest_result,omitempty"`
	NewestResult      *time.Time `json:"newest_result,omitempty"`
	GeneratedAt       time.Time  `json:"generated_at"`
}

// ExportFormat represents supported export formats
type ExportFormat string

const (
	FormatCSV  ExportFormat = "csv"
	FormatJSON ExportFormat = "json"
	FormatTSV  ExportFormat = "tsv"
	FormatXLSX ExportFormat = "xlsx"
)

// ExportOptions represents options for data export
type ExportOptions struct {
	Format       ExportFormat `json:"format"`
	OutputPath   string       `json:"output_path"`
	Prettify     bool         `json:"prettify,omitempty"`      // For JSON format
	IncludeStats bool         `json:"include_stats,omitempty"` // Include query metadata
	MaxRows      int          `json:"max_rows,omitempty"`      // Limit exported rows
}

// TableDisplayOptions represents options for formatting console output
type TableDisplayOptions struct {
	MaxRows       int  `json:"max_rows"`        // Maximum rows to display
	MaxColWidth   int  `json:"max_col_width"`   // Maximum column width
	ShowTotals    bool `json:"show_totals"`     // Show total/summary rows
	ShowMetadata  bool `json:"show_metadata"`   // Show query metadata
	NumberFormat  bool `json:"number_format"`   // Format numbers with commas
}

// DefaultDisplayOptions returns sensible defaults for table display
func DefaultDisplayOptions() TableDisplayOptions {
	return TableDisplayOptions{
		MaxRows:      50,
		MaxColWidth:  30,
		ShowTotals:   true,
		ShowMetadata: false,
		NumberFormat: true,
	}
}