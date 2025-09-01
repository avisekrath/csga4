# GA4 Admin Tool

A powerful command-line interface for exploring Google Analytics 4 properties, discovering dimensions and metrics, executing queries, and generating Clarisights configurations. Built in Go for performance and reliability.

## Overview

GA4 Admin Tool streamlines GA4 data exploration and analysis, reducing investigation time from hours to minutes through intelligent caching, comprehensive metadata discovery, and automated export capabilities.

### Key Features

- **🚀 Fast GA4 Exploration**: Discover accounts, properties, dimensions, and metrics with caching
- **📊 Interactive Query Building**: Execute GA4 reports with advanced filtering and result management  
- **💾 Smart Caching**: DuckDB-powered caching with 70% performance improvements
- **🔧 Multi-Preset Support**: Manage multiple customer environments with isolated authentication
- **📈 JSON Analysis**: Stream and analyze custom dimension exports with business intelligence views
- **⚡ High Performance**: Single binary, minimal dependencies, enterprise-scale tested

## Installation

### Prerequisites

- Go 1.21+ (for building from source)
- Google Analytics 4 access with appropriate permissions
- OAuth2 client credentials (Google Cloud Console)

### Build from Source

```bash
git clone <repository-url>
cd ga4admin
go mod tidy
go build -o ga4admin cmd/ga4admin/main.go
```

### Verify Installation

```bash
./ga4admin --help
./ga4admin config --help
```

## Quick Start

### 1. Configure OAuth Credentials

Set up global OAuth client credentials (shared across all presets):

```bash
# Configure global OAuth credentials
ga4admin config set --client-id <your-client-id> --client-secret <your-client-secret>

# Verify configuration
ga4admin config show
```

### 2. Create Your First Preset

Create a preset with your GA4 refresh token:

```bash
# Create a new preset
ga4admin preset create customer-name --refresh-token <refresh-token>

# List available presets
ga4admin preset list

# Set active preset
ga4admin preset use customer-name
```

### 3. Explore GA4 Data

```bash
# Discover all accessible accounts
ga4admin accounts list

# View accounts and properties in tree format
ga4admin accounts tree

# List properties in specific account
ga4admin properties list --account <account-id>

# Get detailed property information
ga4admin properties show <property-id>
```

### 4. Analyze Metadata

```bash
# Explore available dimensions
ga4admin metadata dimensions --property <property-id>

# Focus on custom dimensions only
ga4admin metadata dimensions --property <property-id> --custom-only

# Analyze metrics
ga4admin metadata metrics --property <property-id>

# Event analysis with volume data
ga4admin metadata events --property <property-id> --days 30
```

### 5. Execute Queries

```bash
# Direct query execution
ga4admin query run --property <property-id> \
  --dimensions sessionSource,deviceCategory \
  --metrics activeUsers,sessions

# Interactive query builder
ga4admin query build --property <property-id>

# View cached results
ga4admin results list --property <property-id>

# Export results to CSV
ga4admin results export <result-id> output.csv --format csv
```

## Architecture

### Command Structure

```
ga4admin/
├── config      # Global OAuth credential management
├── preset      # Multi-customer environment management  
├── accounts    # GA4 account discovery
├── properties  # Property listing and details
├── metadata    # Dimensions, metrics, events exploration
├── query       # Query building and execution
├── results     # Result management and export
├── cache       # Cache performance and cleanup
└── export      # JSON parsing and analysis tools
```

### Authentication System

**Two-Tier Architecture:**
- **Global Config** (`~/.ga4admin/config.yaml`): OAuth client credentials shared across all users
- **Per-Preset** (`~/.ga4admin/presets/*.yaml`): Individual refresh tokens with isolated caches

This allows teams to use shared OAuth applications while maintaining separate customer contexts.

### Caching System

**DuckDB-Powered Performance:**
- **Metadata Caching**: 24-hour TTL for dimensions, metrics (rarely change)
- **Event Analysis**: 1-hour TTL for dynamic event volume data  
- **Query Results**: Persistent storage with explicit management
- **Per-Preset Isolation**: Individual cache databases prevent data mixing
- **Performance**: Demonstrated 70% speed improvements with cache hits

### File Locations

```
~/.ga4admin/
├── config.yaml              # Global OAuth credentials
├── presets/
│   ├── customer1.yaml       # Customer 1 refresh token
│   └── customer2.yaml       # Customer 2 refresh token
└── cache/
    ├── customer1.db         # Customer 1 cached data
    └── customer2.db         # Customer 2 cached data
```

## Command Reference

### Configuration Management

#### `ga4admin config`
Manage global OAuth client credentials used across all presets.

```bash
# Set OAuth credentials
ga4admin config set --client-id <id> --client-secret <secret>

# View current configuration
ga4admin config show
```

### Preset Management

#### `ga4admin preset`
Create, manage, and switch between customer environment presets.

```bash
# Create new preset with refresh token
ga4admin preset create <name> --refresh-token <token>

# List all available presets
ga4admin preset list

# Switch active preset
ga4admin preset use <name>

# Delete preset and associated cache
ga4admin preset delete <name>
```

### Account Discovery

#### `ga4admin accounts`
Discover and browse GA4 accounts accessible by current preset.

```bash
# List all accounts with metadata
ga4admin accounts list

# Tree view showing accounts and properties
ga4admin accounts tree
```

### Property Exploration

#### `ga4admin properties`
Explore properties within GA4 accounts.

```bash
# List properties for specific account
ga4admin properties list --account <account-id>

# Show detailed property information
ga4admin properties show <property-id>
```

**Property Details Include:**
- Service level (GA360 vs Standard GA4)
- Timezone, currency, creation date
- Industry category and property settings
- Cache status and last accessed time

### Metadata Discovery

#### `ga4admin metadata`
Comprehensive metadata exploration for dimensions, metrics, and events.

##### Dimensions
```bash
# List all available dimensions
ga4admin metadata dimensions --property <property-id>

# Focus on custom dimensions only
ga4admin metadata dimensions --property <property-id> --custom-only

# Filter by category
ga4admin metadata dimensions --property <property-id> --category "Event"

# Limit results
ga4admin metadata dimensions --property <property-id> --limit 50
```

##### Metrics
```bash
# List all available metrics
ga4admin metadata metrics --property <property-id>

# Filter by category
ga4admin metadata metrics --property <property-id> --category "Revenue"
```

##### Events Analysis
```bash
# Analyze events over last 30 days
ga4admin metadata events --property <property-id> --days 30

# Focus on revenue events only
ga4admin metadata events --property <property-id> --days 7 --revenue-only
```

### Query Execution

#### `ga4admin query`
Build and execute GA4 reporting queries with advanced filtering.

```bash
# Direct query execution
ga4admin query run --property <property-id> \
  --dimensions sessionSource,deviceCategory \
  --metrics activeUsers,sessions \
  --date-range 30

# With filters
ga4admin query run --property <property-id> \
  --dimensions sessionSource \
  --metrics sessions \
  --filters "deviceCategory==mobile"

# Interactive query builder
ga4admin query build --property <property-id>

# List cached queries
ga4admin query list --property <property-id>
```

**Supported Filters:**
- String operations: `EXACT`, `CONTAINS`, `BEGINS_WITH`, `ENDS_WITH`
- Numeric operations: `EQUAL`, `GREATER_THAN`, `LESS_THAN`
- Multiple filters with AND logic

### Result Management

#### `ga4admin results`
Manage, view, and export query results.

```bash
# List all cached results
ga4admin results list --property <property-id>

# Show detailed result with formatted table
ga4admin results show <result-id> --max-rows 100

# Export to CSV
ga4admin results export <result-id> output.csv --format csv

# Export to JSON with pretty formatting
ga4admin results export <result-id> output.json --format json --prettify

# Result statistics
ga4admin results stats --property <property-id>
```

### Cache Management

#### `ga4admin cache`
Monitor and manage caching performance.

```bash
# View cache statistics and hit rates
ga4admin cache stats

# Clean expired cache entries
ga4admin cache cleanup --expired

# Clean all cache data (use with caution)
ga4admin cache cleanup --all
```

### Data Export & Analysis

#### `ga4admin export`
Advanced data parsing and analysis tools.

```bash
# Parse JSON property exports into DuckDB for analysis
ga4admin export parse-json \
  --input-dir ./exports/properties \
  --output-db ./analysis.db \
  --batch-size 10
```

**JSON Parser Features:**
- **Memory-efficient streaming**: Process large JSON exports without loading all into memory
- **Structured storage**: Create properties, custom_dimensions, and clarisights_integration tables  
- **Business intelligence**: Pre-built analysis views for immediate insights
- **Batch processing**: Configurable transaction sizes for optimal performance

**Analysis Views Created:**
- `dimension_summary`: Scope distribution and usage patterns
- `property_analysis`: Custom dimension counts with Clarisights readiness
- `account_rollup`: Account-level aggregation and statistics  
- `category_analysis`: Dimension category usage with percentages

## Common Workflows

### Initial Customer Setup

```bash
# 1. Configure OAuth (one-time setup)
ga4admin config set --client-id <id> --client-secret <secret>

# 2. Create customer preset
ga4admin preset create acme-corp --refresh-token <token>

# 3. Discover their GA4 environment
ga4admin accounts tree

# 4. Analyze target property
ga4admin properties show <property-id>
ga4admin metadata dimensions --property <property-id> --custom-only
```

### Custom Dimension Analysis

```bash
# 1. Export custom dimensions (assuming JSON files exist)
ga4admin export parse-json --input-dir ./customer-exports --output-db ./analysis.db

# 2. Query analysis database
duckdb ./analysis.db -c "SELECT * FROM dimension_summary;"
duckdb ./analysis.db -c "SELECT * FROM property_analysis LIMIT 10;"
duckdb ./analysis.db -c "SELECT * FROM account_rollup;"
```

### Query Performance Analysis

```bash
# 1. Execute sample queries
ga4admin query run --property <property-id> --dimensions sessionSource --metrics sessions

# 2. Check cache performance  
ga4admin cache stats

# 3. Analyze results
ga4admin results stats --property <property-id>
```

### Multi-Property Comparison

```bash
# 1. Switch between customer environments
ga4admin preset use customer-a
ga4admin metadata dimensions --property <prop-id-a> --custom-only

ga4admin preset use customer-b  
ga4admin metadata dimensions --property <prop-id-b> --custom-only

# 2. Compare cache performance
ga4admin cache stats
```

## Performance Optimizations

### Caching Benefits

- **70% faster metadata retrieval** with cache hits
- **Persistent query results** eliminate re-execution
- **Smart TTL management** balances freshness with performance
- **Per-preset isolation** prevents data contamination

### Best Practices

1. **Use appropriate cache TTLs**: Metadata changes rarely, events change hourly
2. **Monitor cache hit rates**: Aim for >90% hit rates on repeated operations
3. **Clean expired entries**: Regular cleanup maintains optimal performance
4. **Batch operations**: Use tree view and list commands for bulk discovery

### Enterprise Scale Testing

- **Properties**: Successfully tested with 100+ properties per account
- **Dimensions**: Handles 380+ dimensions with full metadata caching  
- **Concurrent Access**: Multiple preset switching without performance degradation
- **Cache Size**: Supports multi-GB cache databases with fast query response

## Troubleshooting

### Authentication Issues

```bash
# Check OAuth configuration
ga4admin config show

# Verify preset token validity  
ga4admin preset list

# Test authentication with account listing
ga4admin accounts list
```

### Cache Problems

```bash
# Check cache statistics
ga4admin cache stats

# Clear corrupted cache
ga4admin cache cleanup --all

# Rebuild cache with fresh API calls
ga4admin metadata dimensions --property <id> --force-refresh
```

### Performance Issues

```bash
# Monitor API rate limits
ga4admin --verbose accounts list

# Check cache hit rates
ga4admin cache stats

# Clean up expired entries
ga4admin cache cleanup --expired
```

## Technical Details

### Dependencies

**Core Libraries:**
```go
github.com/spf13/cobra          // CLI framework
github.com/marcboeker/go-duckdb  // High-performance caching  
golang.org/x/oauth2             // Google OAuth2 authentication
gopkg.in/yaml.v3                // Configuration management
```

**API Integration:**
- Google Analytics Data API v1
- Google Analytics Admin API v1  
- Automatic OAuth2 token refresh
- Intelligent rate limiting and backoff

### Package Architecture

```
internal/
├── api/           # GA4 API client (auth, admin, data)
├── cache/         # DuckDB caching system
├── config/        # Configuration models and management
├── export/        # JSON parsing and analysis tools
├── preset/        # Multi-preset environment management
├── query/         # Query building and execution
└── results/       # Result storage and export
```

### Data Models

**Properties:** Service level, metadata, cache status tracking  
**Accounts:** Hierarchical account/property relationships  
**Dimensions/Metrics:** Full metadata with categories and descriptions
**Query Results:** Cached results with execution metadata
**Cache Statistics:** Hit rates, entry counts, storage efficiency

## Roadmap

### Planned Features

- **🎨 Terminal UI (TUI)**: Interactive interface with Bubbletea framework

### Integration Extensions

- **Custom Connectors**: Plugin system for proprietary analytics platforms  
- **API Validation**: Test exported configurations against target platforms

## Contributing

### Development Setup

```bash
# Clone and build
git clone <repository-url>  
cd ga4admin
go mod tidy
go build -o ga4admin cmd/ga4admin/main.go

# Run tests
go test ./...

# Format code
go fmt ./...
```

### Code Standards

- Follow Go conventions and idioms
- Maintain backward compatibility for CLI interfaces
- Add comprehensive error handling for API interactions

GA4 Admin Tool transforms manual GA4 exploration into an efficient, cached, command-line workflow that accelerates customer implementation and reduces analysis time from hours to minutes.
