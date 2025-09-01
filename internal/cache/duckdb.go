package cache

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	
	"ga4admin/internal/config"
)

// CacheClient handles DuckDB-based caching operations
type CacheClient struct {
	db         *sql.DB
	presetName string
	cachePath  string
}

// NewCacheClient creates a new cache client for a specific preset
func NewCacheClient(presetName string) (*CacheClient, error) {
	// Create cache directory if it doesn't exist
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("failed to get user home directory: %w", err)
	}

	cacheDir := filepath.Join(homeDir, ".ga4admin", "cache")
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create cache directory: %w", err)
	}

	// Create preset-specific database file
	cachePath := filepath.Join(cacheDir, fmt.Sprintf("%s.db", presetName))
	
	// Connect to DuckDB
	db, err := sql.Open("duckdb", cachePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB connection: %w", err)
	}

	client := &CacheClient{
		db:         db,
		presetName: presetName,
		cachePath:  cachePath,
	}

	// Initialize cache tables
	if err := client.initializeTables(); err != nil {
		return nil, fmt.Errorf("failed to initialize cache tables: %w", err)
	}

	return client, nil
}

// Close closes the database connection
func (c *CacheClient) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// initializeTables creates the necessary cache tables
func (c *CacheClient) initializeTables() error {
	queries := []string{
		// Metadata cache table
		`CREATE TABLE IF NOT EXISTS metadata_cache (
			property_id VARCHAR PRIMARY KEY,
			cache_type VARCHAR NOT NULL,  -- 'dimensions', 'metrics', 'events'
			data TEXT NOT NULL,           -- JSON-encoded metadata
			created_at TIMESTAMP DEFAULT NOW(),
			expires_at TIMESTAMP NOT NULL,
			last_accessed TIMESTAMP DEFAULT NOW()
		)`,
		
		// Query results cache table
		`CREATE TABLE IF NOT EXISTS query_cache (
			query_id VARCHAR PRIMARY KEY,
			property_id VARCHAR NOT NULL,
			query_hash VARCHAR NOT NULL,     -- Hash of query parameters
			query_params TEXT NOT NULL,     -- JSON-encoded query parameters
			result_data TEXT NOT NULL,      -- JSON-encoded query results
			row_count INTEGER NOT NULL,
			created_at TIMESTAMP DEFAULT NOW(),
			expires_at TIMESTAMP,           -- NULL = never expires
			last_accessed TIMESTAMP DEFAULT NOW()
		)`,
		
		// Named tables for query results
		`CREATE TABLE IF NOT EXISTS named_tables (
			table_name VARCHAR PRIMARY KEY,
			property_id VARCHAR NOT NULL,
			query_id VARCHAR NOT NULL,
			description TEXT,
			created_at TIMESTAMP DEFAULT NOW(),
			last_accessed TIMESTAMP DEFAULT NOW(),
			FOREIGN KEY (query_id) REFERENCES query_cache(query_id)
		)`,
		
		// Cache statistics table
		`CREATE TABLE IF NOT EXISTS cache_stats (
			preset_name VARCHAR PRIMARY KEY,
			total_hits INTEGER DEFAULT 0,
			total_misses INTEGER DEFAULT 0,
			last_cleanup TIMESTAMP,
			created_at TIMESTAMP DEFAULT NOW(),
			updated_at TIMESTAMP DEFAULT NOW()
		)`,
	}

	for _, query := range queries {
		if _, err := c.db.Exec(query); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}

	// Initialize cache stats for this preset
	_, err := c.db.Exec(`
		INSERT OR IGNORE INTO cache_stats (preset_name) 
		VALUES (?)
	`, c.presetName)
	
	return err
}

// CacheMetadata stores GA4 metadata with TTL
func (c *CacheClient) CacheMetadata(ctx context.Context, propertyID, cacheType string, data interface{}, ttlHours int) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	expiresAt := time.Now().Add(time.Duration(ttlHours) * time.Hour)
	
	_, err = c.db.ExecContext(ctx, `
		INSERT OR REPLACE INTO metadata_cache 
		(property_id, cache_type, data, expires_at) 
		VALUES (?, ?, ?, ?)
	`, propertyID, cacheType, string(jsonData), expiresAt)

	return err
}

// GetCachedMetadata retrieves cached metadata if valid
func (c *CacheClient) GetCachedMetadata(ctx context.Context, propertyID, cacheType string, result interface{}) (bool, error) {
	var data string
	var expiresAt time.Time

	err := c.db.QueryRowContext(ctx, `
		SELECT data, expires_at 
		FROM metadata_cache 
		WHERE property_id = ? AND cache_type = ?
	`, propertyID, cacheType).Scan(&data, &expiresAt)

	if err != nil {
		if err == sql.ErrNoRows {
			c.incrementMisses()
			return false, nil // Cache miss
		}
		return false, fmt.Errorf("failed to query cache: %w", err)
	}

	// Check if cache has expired
	if time.Now().After(expiresAt) {
		c.incrementMisses()
		// Clean up expired entry
		c.db.ExecContext(ctx, `
			DELETE FROM metadata_cache 
			WHERE property_id = ? AND cache_type = ?
		`, propertyID, cacheType)
		return false, nil
	}

	// Update last accessed time
	c.db.ExecContext(ctx, `
		UPDATE metadata_cache 
		SET last_accessed = NOW() 
		WHERE property_id = ? AND cache_type = ?
	`, propertyID, cacheType)

	// Unmarshal and return
	if err := json.Unmarshal([]byte(data), result); err != nil {
		return false, fmt.Errorf("failed to unmarshal cached data: %w", err)
	}

	c.incrementHits()
	return true, nil
}

// CacheQuery stores query results with optional TTL
func (c *CacheClient) CacheQuery(ctx context.Context, queryID, propertyID, queryHash string, queryParams, resultData interface{}, rowCount int, ttlHours *int) error {
	jsonParams, err := json.Marshal(queryParams)
	if err != nil {
		return fmt.Errorf("failed to marshal query params: %w", err)
	}

	jsonData, err := json.Marshal(resultData)
	if err != nil {
		return fmt.Errorf("failed to marshal result data: %w", err)
	}

	var expiresAt *time.Time
	if ttlHours != nil {
		expires := time.Now().Add(time.Duration(*ttlHours) * time.Hour)
		expiresAt = &expires
	}

	_, err = c.db.ExecContext(ctx, `
		INSERT OR REPLACE INTO query_cache 
		(query_id, property_id, query_hash, query_params, result_data, row_count, expires_at) 
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, queryID, propertyID, queryHash, string(jsonParams), string(jsonData), rowCount, expiresAt)

	return err
}

// GetCachedQuery retrieves cached query results if valid
func (c *CacheClient) GetCachedQuery(ctx context.Context, queryHash string, queryParams, resultData interface{}) (bool, error) {
	var data string
	var expiresAt *time.Time
	var rowCount int

	err := c.db.QueryRowContext(ctx, `
		SELECT result_data, row_count, expires_at
		FROM query_cache 
		WHERE query_hash = ?
	`, queryHash).Scan(&data, &rowCount, &expiresAt)

	if err != nil {
		if err == sql.ErrNoRows {
			c.incrementMisses()
			return false, nil
		}
		return false, fmt.Errorf("failed to query cache: %w", err)
	}

	// Check expiration
	if expiresAt != nil && time.Now().After(*expiresAt) {
		c.incrementMisses()
		// Clean up expired entry
		c.db.ExecContext(ctx, `DELETE FROM query_cache WHERE query_hash = ?`, queryHash)
		return false, nil
	}

	// Update last accessed
	c.db.ExecContext(ctx, `
		UPDATE query_cache 
		SET last_accessed = NOW() 
		WHERE query_hash = ?
	`, queryHash)

	// Unmarshal result
	if err := json.Unmarshal([]byte(data), resultData); err != nil {
		return false, fmt.Errorf("failed to unmarshal cached data: %w", err)
	}

	c.incrementHits()
	return true, nil
}

// CreateNamedTable creates a named reference to query results
func (c *CacheClient) CreateNamedTable(ctx context.Context, tableName, propertyID, queryID, description string) error {
	_, err := c.db.ExecContext(ctx, `
		INSERT OR REPLACE INTO named_tables 
		(table_name, property_id, query_id, description) 
		VALUES (?, ?, ?, ?)
	`, tableName, propertyID, queryID, description)

	return err
}

// ListNamedTables returns all named tables for a property
func (c *CacheClient) ListNamedTables(ctx context.Context, propertyID string) ([]config.NamedTable, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT nt.table_name, nt.description, nt.created_at, nt.last_accessed,
		       qc.row_count, qc.created_at as query_created
		FROM named_tables nt
		JOIN query_cache qc ON nt.query_id = qc.query_id
		WHERE nt.property_id = ?
		ORDER BY nt.created_at DESC
	`, propertyID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []config.NamedTable
	for rows.Next() {
		var table config.NamedTable
		err := rows.Scan(
			&table.Name, &table.Description, &table.CreatedAt, &table.LastAccessed,
			&table.RowCount, &table.QueryCreatedAt,
		)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}

	return tables, nil
}

// GetCacheStats returns cache performance statistics
func (c *CacheClient) GetCacheStats(ctx context.Context) (*config.CacheStats, error) {
	var stats config.CacheStats
	err := c.db.QueryRowContext(ctx, `
		SELECT total_hits, total_misses, last_cleanup, created_at, updated_at
		FROM cache_stats 
		WHERE preset_name = ?
	`, c.presetName).Scan(
		&stats.TotalHits, &stats.TotalMisses, &stats.LastCleanup,
		&stats.CreatedAt, &stats.UpdatedAt,
	)

	if err != nil {
		return nil, err
	}

	// Calculate hit rate
	total := stats.TotalHits + stats.TotalMisses
	if total > 0 {
		stats.HitRate = float64(stats.TotalHits) / float64(total) * 100
	}

	// Get storage stats
	var dbSize int64
	err = c.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM metadata_cache
		UNION ALL SELECT COUNT(*) FROM query_cache
	`).Scan(&dbSize)

	stats.EntriesCount = int(dbSize)
	
	return &stats, nil
}

// CleanupExpiredEntries removes expired cache entries
func (c *CacheClient) CleanupExpiredEntries(ctx context.Context) (int, error) {
	// Clean metadata cache
	result1, err := c.db.ExecContext(ctx, `
		DELETE FROM metadata_cache 
		WHERE expires_at < NOW()
	`)
	if err != nil {
		return 0, err
	}

	deleted1, _ := result1.RowsAffected()

	// Clean query cache
	result2, err := c.db.ExecContext(ctx, `
		DELETE FROM query_cache 
		WHERE expires_at IS NOT NULL AND expires_at < NOW()
	`)
	if err != nil {
		return int(deleted1), err
	}

	deleted2, _ := result2.RowsAffected()

	// Update cleanup timestamp
	_, err = c.db.ExecContext(ctx, `
		UPDATE cache_stats 
		SET last_cleanup = NOW(), updated_at = NOW() 
		WHERE preset_name = ?
	`, c.presetName)

	return int(deleted1 + deleted2), err
}

// Helper methods for cache statistics
func (c *CacheClient) incrementHits() {
	c.db.Exec(`
		UPDATE cache_stats 
		SET total_hits = total_hits + 1, updated_at = NOW() 
		WHERE preset_name = ?
	`, c.presetName)
}

func (c *CacheClient) incrementMisses() {
	c.db.Exec(`
		UPDATE cache_stats 
		SET total_misses = total_misses + 1, updated_at = NOW() 
		WHERE preset_name = ?
	`, c.presetName)
}