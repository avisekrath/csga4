package export

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

// JSONParser handles streaming JSON files into DuckDB tables
type JSONParser struct {
	dbPath    string
	inputDir  string
	batchSize int
}

// NewJSONParser creates a new parser instance
func NewJSONParser(dbPath, inputDir string) *JSONParser {
	return &JSONParser{
		dbPath:    dbPath,
		inputDir:  inputDir,
		batchSize: 20, // Process 20 files per transaction
	}
}

// SetBatchSize updates the batch size for processing
func (p *JSONParser) SetBatchSize(size int) {
	if size > 0 {
		p.batchSize = size
	}
}

// ParseAllJSON streams all JSON files into DuckDB tables
func (p *JSONParser) ParseAllJSON(ctx context.Context) error {
	// Initialize database and schema
	if err := p.initializeDatabase(ctx); err != nil {
		return fmt.Errorf("failed to initialize database: %w", err)
	}

	// Get all JSON files
	jsonFiles, err := p.getJSONFiles()
	if err != nil {
		return fmt.Errorf("failed to get JSON files: %w", err)
	}

	fmt.Printf("Found %d JSON files to process\n", len(jsonFiles))

	// Process files in batches for memory efficiency
	for i := 0; i < len(jsonFiles); i += p.batchSize {
		end := i + p.batchSize
		if end > len(jsonFiles) {
			end = len(jsonFiles)
		}

		batch := jsonFiles[i:end]
		if err := p.processBatch(ctx, batch, i+1); err != nil {
			return fmt.Errorf("failed to process batch %d-%d: %w", i+1, end, err)
		}

		fmt.Printf("Processed files %d-%d of %d\n", i+1, end, len(jsonFiles))
	}

	// Create analysis views
	if err := p.createAnalysisViews(ctx); err != nil {
		return fmt.Errorf("failed to create analysis views: %w", err)
	}

	fmt.Println("✅ JSON parsing completed successfully")
	return nil
}

// initializeDatabase creates the database schema
func (p *JSONParser) initializeDatabase(ctx context.Context) error {
	db, err := sql.Open("duckdb", p.dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	schemas := []string{
		// Properties table
		`CREATE TABLE IF NOT EXISTS properties (
			property_id VARCHAR PRIMARY KEY,
			property_name VARCHAR NOT NULL,
			account_id VARCHAR NOT NULL,
			account_name VARCHAR NOT NULL,
			currency VARCHAR,
			timezone VARCHAR,
			industry VARCHAR,
			service_level VARCHAR,
			created_date TIMESTAMP,
			last_accessed TIMESTAMP,
			collection_timestamp TIMESTAMP,
			total_dimensions INTEGER,
			custom_dimensions_count INTEGER,
			collector_version VARCHAR,
			preset_used VARCHAR,
			collection_duration VARCHAR,
			api_call_count INTEGER
		)`,

		// Custom dimensions table - DuckDB auto-increment sequence
		`CREATE SEQUENCE IF NOT EXISTS custom_dimensions_id_seq START 1`,
		`CREATE TABLE IF NOT EXISTS custom_dimensions (
			id INTEGER PRIMARY KEY DEFAULT nextval('custom_dimensions_id_seq'),
			property_id VARCHAR NOT NULL,
			api_name VARCHAR NOT NULL,
			ui_name VARCHAR,
			description TEXT,
			scope VARCHAR NOT NULL,
			category VARCHAR,
			custom_definition BOOLEAN
		)`,

		// Clarisights integration tracking
		`CREATE TABLE IF NOT EXISTS clarisights_integration (
			property_id VARCHAR PRIMARY KEY,
			has_custom_channel_groups BOOLEAN,
			channel_group_id VARCHAR,
			channel_group_name VARCHAR
		)`,
	}

	for _, schema := range schemas {
		if _, err := db.ExecContext(ctx, schema); err != nil {
			return fmt.Errorf("failed to create schema: %w", err)
		}
	}

	return nil
}

// getJSONFiles returns all JSON files in the input directory
func (p *JSONParser) getJSONFiles() ([]string, error) {
	var files []string

	err := filepath.WalkDir(p.inputDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() && strings.HasSuffix(path, ".json") {
			files = append(files, path)
		}

		return nil
	})

	return files, err
}

// processBatch processes a batch of JSON files
func (p *JSONParser) processBatch(ctx context.Context, files []string, startNum int) error {
	db, err := sql.Open("duckdb", p.dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	// Begin transaction for batch
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Prepare statements
	propStmt, err := tx.PrepareContext(ctx, `
		INSERT OR REPLACE INTO properties (
			property_id, property_name, account_id, account_name, currency, timezone,
			industry, service_level, created_date, last_accessed, collection_timestamp,
			total_dimensions, custom_dimensions_count, collector_version, preset_used,
			collection_duration, api_call_count
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer propStmt.Close()

	dimStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO custom_dimensions (
			property_id, api_name, ui_name, description, scope, category, custom_definition
		) VALUES (?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer dimStmt.Close()

	clarisightsStmt, err := tx.PrepareContext(ctx, `
		INSERT OR REPLACE INTO clarisights_integration (
			property_id, has_custom_channel_groups, channel_group_id, channel_group_name
		) VALUES (?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer clarisightsStmt.Close()

	// Process each file in the batch
	for _, file := range files {
		if err := p.processFile(ctx, file, propStmt, dimStmt, clarisightsStmt); err != nil {
			fmt.Printf("Warning: Failed to process %s: %v\n", filepath.Base(file), err)
			continue // Continue with other files
		}
	}

	// Commit batch
	return tx.Commit()
}

// processFile processes a single JSON file
func (p *JSONParser) processFile(ctx context.Context, filePath string, propStmt, dimStmt, clarisightsStmt *sql.Stmt) error {
	// Read JSON file
	data, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	// Parse JSON
	var export PropertyExport
	if err := json.Unmarshal(data, &export); err != nil {
		return err
	}

	// Insert property info - handle potential nil pointers for time fields
	var createdDate, lastAccessed time.Time
	if export.PropertyInfo.CreatedDate != nil {
		createdDate = *export.PropertyInfo.CreatedDate
	}
	if export.PropertyInfo.LastAccessed != nil {
		lastAccessed = *export.PropertyInfo.LastAccessed
	}
	collectionTime := export.CollectionMetadata.Timestamp

	_, err = propStmt.ExecContext(ctx,
		export.PropertyInfo.PropertyID,
		export.PropertyInfo.PropertyName,
		export.PropertyInfo.AccountID,
		export.PropertyInfo.AccountName,
		export.PropertyInfo.Currency,
		export.PropertyInfo.Timezone,
		export.PropertyInfo.Industry,
		export.PropertyInfo.ServiceLevel,
		createdDate,
		lastAccessed,
		collectionTime,
		export.CollectionMetadata.TotalDimensions,
		export.CollectionMetadata.CustomDimensions,
		export.CollectionMetadata.CollectorVersion,
		export.CollectionMetadata.PresetUsed,
		export.CollectionMetadata.CollectionDuration,
		export.CollectionMetadata.ApiCallCount,
	)
	if err != nil {
		return err
	}

	// Insert custom dimensions (flattened from all scopes)
	for scope, dimensions := range export.CustomDimensions {
		for _, dim := range dimensions {
			// Determine actual scope from API name if different from map key
			actualScope := scope
			if strings.HasPrefix(dim.APIName, "customEvent:") {
				actualScope = "event"
			} else if strings.HasPrefix(dim.APIName, "customUser:") {
				actualScope = "user"
			} else if strings.HasPrefix(dim.APIName, "customItem:") {
				actualScope = "item"
			} else if strings.Contains(dim.APIName, "ChannelGroup") {
				actualScope = "session"
			}

			_, err = dimStmt.ExecContext(ctx,
				export.PropertyInfo.PropertyID,
				dim.APIName,
				dim.UIName,
				dim.Description,
				actualScope,
				dim.Category,
				dim.CustomDefinition,
			)
			if err != nil {
				return err
			}
		}
	}

	// Insert Clarisights integration info
	_, err = clarisightsStmt.ExecContext(ctx,
		export.PropertyInfo.PropertyID,
		export.ClarisightsIntegration.HasCustomChannelGroups,
		export.ClarisightsIntegration.ChannelGroupID,
		export.ClarisightsIntegration.ChannelGroupName,
	)

	return err
}

// createAnalysisViews creates useful views for data analysis
func (p *JSONParser) createAnalysisViews(ctx context.Context) error {
	db, err := sql.Open("duckdb", p.dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	views := []string{
		// Dimension summary by scope
		`CREATE OR REPLACE VIEW dimension_summary AS
		SELECT 
			scope,
			COUNT(*) as dimension_count,
			COUNT(DISTINCT property_id) as properties_using,
			COUNT(DISTINCT category) as unique_categories
		FROM custom_dimensions 
		GROUP BY scope
		ORDER BY dimension_count DESC`,

		// Property analysis with dimension counts
		`CREATE OR REPLACE VIEW property_analysis AS
		SELECT 
			p.property_id,
			p.property_name,
			p.account_name,
			p.service_level,
			p.custom_dimensions_count,
			COUNT(cd.id) as actual_dimension_count,
			c.has_custom_channel_groups,
			c.channel_group_name
		FROM properties p
		LEFT JOIN custom_dimensions cd ON p.property_id = cd.property_id
		LEFT JOIN clarisights_integration c ON p.property_id = c.property_id
		GROUP BY p.property_id, p.property_name, p.account_name, p.service_level, 
				 p.custom_dimensions_count, c.has_custom_channel_groups, c.channel_group_name
		ORDER BY p.custom_dimensions_count DESC`,

		// Account rollup analysis
		`CREATE OR REPLACE VIEW account_rollup AS
		SELECT 
			account_name,
			COUNT(DISTINCT p.property_id) as total_properties,
			SUM(custom_dimensions_count) as total_custom_dimensions,
			AVG(custom_dimensions_count) as avg_dimensions_per_property,
			COUNT(CASE WHEN service_level = 'GOOGLE_ANALYTICS_360' THEN 1 END) as ga360_properties,
			SUM(CASE WHEN c.has_custom_channel_groups THEN 1 ELSE 0 END) as clarisights_ready_properties
		FROM properties p
		LEFT JOIN clarisights_integration c ON p.property_id = c.property_id
		GROUP BY account_name
		ORDER BY total_custom_dimensions DESC`,

		// Dimension category analysis
		`CREATE OR REPLACE VIEW category_analysis AS
		SELECT 
			category,
			scope,
			COUNT(*) as usage_count,
			COUNT(DISTINCT property_id) as properties_using,
			ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
		FROM custom_dimensions 
		WHERE category IS NOT NULL
		GROUP BY category, scope
		ORDER BY usage_count DESC`,
	}

	for _, view := range views {
		if _, err := db.ExecContext(ctx, view); err != nil {
			return fmt.Errorf("failed to create view: %w", err)
		}
	}

	return nil
}