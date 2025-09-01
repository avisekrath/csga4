package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"ga4admin/internal/api"
	"ga4admin/internal/cache"
	"ga4admin/internal/config"
	"ga4admin/internal/export"
	"ga4admin/internal/preset"
	"ga4admin/internal/query"
	"ga4admin/internal/results"
)

var (
	version = "0.1.0"
	rootCmd = &cobra.Command{
		Use:   "ga4admin",
		Short: "GA4 Admin Tool for exploring and exporting Google Analytics 4 data",
		Long: `GA4 Admin Tool is a CLI for exploring GA4 properties, discovering dimensions/metrics,
running queries, and generating Clarisights configurations.

Examples:
  ga4admin config set --client-id <id> --client-secret <secret>
  ga4admin preset create tmobile --refresh-token <token>
  ga4admin accounts list
  ga4admin properties list --account <id>
  ga4admin metadata dimensions --property <id>`,
		Version: version,
	}

	configCmd = &cobra.Command{
		Use:   "config",
		Short: "Manage global OAuth configuration",
		Long:  "Configure global OAuth client credentials used across all presets",
	}

	presetCmd = &cobra.Command{
		Use:   "preset",
		Short: "Manage GA4 account presets",
		Long:  "Create, list, delete, and switch between GA4 account presets",
	}

	accountsCmd = &cobra.Command{
		Use:   "accounts",
		Short: "List GA4 accounts",
		Long:  "List all Google Analytics 4 accounts accessible by the current preset",
	}

	propertiesCmd = &cobra.Command{
		Use:   "properties",  
		Short: "List GA4 properties",
		Long:  "List Google Analytics 4 properties within a specific account",
	}

	metadataCmd = &cobra.Command{
		Use:   "metadata",
		Short: "Explore GA4 metadata",
		Long:  "Discover dimensions, metrics, and events available in a GA4 property",
	}

	queryCmd = &cobra.Command{
		Use:   "query",
		Short: "Execute GA4 queries",
		Long:  "Build and execute GA4 reporting queries, save results to cache",
	}

	resultsCmd = &cobra.Command{
		Use:   "results",
		Short: "Manage query results",
		Long:  "List, export, and manage cached GA4 query results",
	}

	cacheCmd = &cobra.Command{
		Use:   "cache",
		Short: "Manage data cache",
		Long:  "Manage metadata and query result caching",
	}

	exportCmd = &cobra.Command{
		Use:   "export",
		Short: "Export configurations",
		Long:  "Export Clarisights configurations and data extracts",
	}
)

func init() {
	// Global flags
	rootCmd.PersistentFlags().String("preset", "", "GA4 preset to use (overrides active preset)")
	rootCmd.PersistentFlags().Bool("verbose", false, "Enable verbose logging")

	// Config subcommands
	configSetCmd := &cobra.Command{
		Use:   "set",
		Short: "Set global OAuth credentials",
		Long:  "Configure global OAuth client credentials used across all presets",
		Run:   configSetCmdHandler,
	}
	configSetCmd.Flags().String("client-id", "", "Google OAuth client ID (required)")
	configSetCmd.Flags().String("client-secret", "", "Google OAuth client secret (required)")
	configSetCmd.MarkFlagRequired("client-id")
	configSetCmd.MarkFlagRequired("client-secret")
	
	configShowCmd := &cobra.Command{
		Use:   "show", 
		Short: "Show current configuration",
		Long:  "Display the current global configuration and active preset",
		Run:   configShowCmdHandler,
	}

	configCmd.AddCommand(configSetCmd, configShowCmd)

	// Preset subcommands
	presetCreateCmd := &cobra.Command{
		Use:   "create [name]",
		Short: "Create a new preset",
		Long:  "Create a new GA4 preset with refresh token for API access",
		Args:  cobra.ExactArgs(1),
		Run:   presetCreateCmdHandler,
	}
	presetCreateCmd.Flags().String("refresh-token", "", "Google OAuth refresh token (required)")
	presetCreateCmd.Flags().String("user-email", "", "User email for identification (optional)")
	presetCreateCmd.Flags().Bool("no-validate", false, "Skip refresh token validation (advanced users only)")
	presetCreateCmd.MarkFlagRequired("refresh-token")

	presetListCmd := &cobra.Command{
		Use:   "list",
		Short: "List all presets",
		Long:  "List all available GA4 presets with metadata",
		Run:   presetListCmdHandler,
	}

	presetDeleteCmd := &cobra.Command{
		Use:   "delete [name]",
		Short: "Delete a preset",
		Long:  "Delete a GA4 preset and all associated data",
		Args:  cobra.ExactArgs(1), 
		Run:   presetDeleteCmdHandler,
	}

	presetUseCmd := &cobra.Command{
		Use:   "use [name]",
		Short: "Set active preset",
		Long:  "Set the active GA4 preset for API operations",
		Args:  cobra.ExactArgs(1),
		Run:   presetUseCmdHandler,
	}

	presetCmd.AddCommand(presetCreateCmd, presetListCmd, presetDeleteCmd, presetUseCmd)

	// Accounts subcommands
	accountsCmd.AddCommand(&cobra.Command{
		Use:   "list",
		Short: "List all accounts",
		Run:   accountsListCmd,
	})
	accountsCmd.AddCommand(&cobra.Command{
		Use:   "tree",
		Short: "Show accounts with properties in tree view",
		Run:   accountsTreeCmd,
	})

	// Properties subcommands
	propertiesListSubCmd := &cobra.Command{
		Use:   "list",
		Short: "List properties for account",
		Run:   propertiesListCmd,
	}
	propertiesListSubCmd.Flags().String("account", "", "Account ID to list properties for (required)")
	propertiesListSubCmd.MarkFlagRequired("account")
	propertiesCmd.AddCommand(propertiesListSubCmd)
	propertiesCmd.AddCommand(&cobra.Command{
		Use:   "show [property-id]",
		Short: "Show property details",
		Args:  cobra.ExactArgs(1),
		Run:   propertiesShowCmd,
	})

	// Metadata subcommands
	metadataDimensionsSubCmd := &cobra.Command{
		Use:   "dimensions",
		Short: "List available dimensions",
		Run:   metadataDimensionsCmd,
	}
	metadataDimensionsSubCmd.Flags().String("property", "", "Property ID to get dimensions for (required)")
	metadataDimensionsSubCmd.Flags().Bool("custom-only", false, "Show only custom dimensions")
	metadataDimensionsSubCmd.Flags().String("category", "", "Filter by dimension category")
	metadataDimensionsSubCmd.MarkFlagRequired("property")
	
	metadataMetricsSubCmd := &cobra.Command{
		Use:   "metrics", 
		Short: "List available metrics",
		Run:   metadataMetricsCmd,
	}
	metadataMetricsSubCmd.Flags().String("property", "", "Property ID to get metrics for (required)")
	metadataMetricsSubCmd.Flags().Bool("custom-only", false, "Show only custom metrics")
	metadataMetricsSubCmd.Flags().String("category", "", "Filter by metric category")
	metadataMetricsSubCmd.Flags().String("type", "", "Filter by metric type")
	metadataMetricsSubCmd.MarkFlagRequired("property")
	
	metadataEventsSubCmd := &cobra.Command{
		Use:   "events",
		Short: "Analyze events with volumes",
		Run:   metadataEventsCmd,
	}
	metadataEventsSubCmd.Flags().String("property", "", "Property ID to analyze events for (required)")
	metadataEventsSubCmd.Flags().Int("days", 30, "Number of days to analyze (default: 30)")
	metadataEventsSubCmd.Flags().Int("limit", 50, "Number of top events to show (default: 50)")
	metadataEventsSubCmd.MarkFlagRequired("property")

	metadataCmd.AddCommand(metadataDimensionsSubCmd, metadataMetricsSubCmd, metadataEventsSubCmd)

	// Query subcommands
	queryRunSubCmd := &cobra.Command{
		Use:   "run",
		Short: "Execute a GA4 query",
		Run:   queryRunCmd,
	}
	queryRunSubCmd.Flags().String("property", "", "Property ID to query (required)")
	queryRunSubCmd.Flags().StringSlice("dimensions", []string{}, "Dimension names (comma-separated)")
	queryRunSubCmd.Flags().StringSlice("metrics", []string{}, "Metric names (comma-separated)")
	queryRunSubCmd.Flags().String("start-date", "30daysAgo", "Start date (YYYY-MM-DD or relative)")
	queryRunSubCmd.Flags().String("end-date", "yesterday", "End date (YYYY-MM-DD or relative)")
	queryRunSubCmd.Flags().Int64("limit", 10000, "Maximum rows to return")
	queryRunSubCmd.Flags().StringSlice("filters", []string{}, "Filters in format 'field:type:operation:value'")
	queryRunSubCmd.Flags().String("order-by", "", "Order by field (prefix with - for descending)")
	queryRunSubCmd.Flags().String("name", "", "Save query with this name")
	queryRunSubCmd.Flags().Bool("no-cache", false, "Skip cache and force fresh query")
	queryRunSubCmd.MarkFlagRequired("property")

	queryBuildSubCmd := &cobra.Command{
		Use:   "build",
		Short: "Interactive query builder",
		Run:   queryBuildCmd,
	}
	queryBuildSubCmd.Flags().String("property", "", "Property ID to query (required)")
	queryBuildSubCmd.MarkFlagRequired("property")

	queryListSubCmd := &cobra.Command{
		Use:   "list",
		Short: "List cached queries",
		Run:   queryListCmd,
	}
	queryListSubCmd.Flags().String("property", "", "Filter by property ID")
	queryListSubCmd.Flags().Int("limit", 20, "Maximum results to show")

	queryCmd.AddCommand(queryRunSubCmd, queryBuildSubCmd, queryListSubCmd)

	// Results subcommands
	resultsListSubCmd := &cobra.Command{
		Use:   "list",
		Short: "List cached query results",
		Run:   resultsListCmd,
	}
	resultsListSubCmd.Flags().String("property", "", "Filter by property ID")
	resultsListSubCmd.Flags().Int("limit", 20, "Maximum results to show")

	resultsShowSubCmd := &cobra.Command{
		Use:   "show [result-id]",
		Short: "Show query result details",
		Args:  cobra.ExactArgs(1),
		Run:   resultsShowCmd,
	}
	resultsShowSubCmd.Flags().Int("max-rows", 50, "Maximum rows to display")
	resultsShowSubCmd.Flags().Int("max-width", 30, "Maximum column width")
	resultsShowSubCmd.Flags().Bool("show-totals", true, "Show totals/summary rows")

	resultsExportSubCmd := &cobra.Command{
		Use:   "export [result-id] [output-file]",
		Short: "Export query results to file",
		Args:  cobra.ExactArgs(2),
		Run:   resultsExportCmd,
	}
	resultsExportSubCmd.Flags().String("format", "csv", "Export format (csv, json)")
	resultsExportSubCmd.Flags().Bool("prettify", false, "Prettify JSON output")

	resultsStatsSubCmd := &cobra.Command{
		Use:   "stats",
		Short: "Show result statistics",
		Run:   resultsStatsCmd,
	}
	resultsStatsSubCmd.Flags().String("property", "", "Property ID to analyze")

	resultsCmd.AddCommand(resultsListSubCmd, resultsShowSubCmd, resultsExportSubCmd, resultsStatsSubCmd)

	// Cache subcommands
	cacheStatsSubCmd := &cobra.Command{
		Use:   "stats",
		Short: "Show cache statistics",
		Run:   cacheStatsCmd,
	}

	cacheCleanupSubCmd := &cobra.Command{
		Use:   "cleanup",
		Short: "Clean up expired cache entries",
		Run:   cacheCleanupCmd,
	}
	cacheCleanupSubCmd.Flags().Bool("expired", true, "Clean only expired entries")
	cacheCleanupSubCmd.Flags().Bool("all", false, "Clean all cache entries (use with caution)")

	cacheCmd.AddCommand(cacheStatsSubCmd, cacheCleanupSubCmd)

	// Export subcommands
	exportParseSubCmd := &cobra.Command{
		Use:   "parse-json",
		Short: "Parse JSON files into DuckDB tables",
		Long:  "Stream JSON export files into structured DuckDB tables for efficient querying",
		Run:   exportParseCmd,
	}
	exportParseSubCmd.Flags().String("input-dir", "UniversalMusic/properties", "Directory containing JSON files")
	exportParseSubCmd.Flags().String("output-db", "UniversalMusic/universal_music_parsed.db", "Output DuckDB database path")
	exportParseSubCmd.Flags().Int("batch-size", 20, "Number of files to process per transaction")

	exportCmd.AddCommand(exportParseSubCmd)

	// Test command (hidden) for OAuth validation
	testCmd := &cobra.Command{
		Use:    "test-auth",
		Short:  "Test OAuth2 authentication",
		Hidden: true,
		Run:    testAuthCmdHandler,
	}

	// Add all commands to root
	rootCmd.AddCommand(configCmd, presetCmd, accountsCmd, propertiesCmd, metadataCmd, queryCmd, resultsCmd, cacheCmd, exportCmd, testCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// Command implementations
func configSetCmdHandler(cmd *cobra.Command, args []string) {
	clientID, _ := cmd.Flags().GetString("client-id")
	clientSecret, _ := cmd.Flags().GetString("client-secret")

	fmt.Println("🔧 Setting global OAuth configuration...")

	// Validate inputs
	if strings.TrimSpace(clientID) == "" {
		fmt.Fprintf(os.Stderr, "Error: client-id cannot be empty\n")
		os.Exit(1)
	}
	if strings.TrimSpace(clientSecret) == "" {
		fmt.Fprintf(os.Stderr, "Error: client-secret cannot be empty\n")
		os.Exit(1)
	}

	// Save credentials
	if err := config.SetClientCredentials(clientID, clientSecret); err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to save configuration: %v\n", err)
		os.Exit(1)
	}

	// Get config path for display
	configPath, _ := config.GetConfigPath()
	fmt.Printf("✅ OAuth credentials saved successfully\n")
	fmt.Printf("📁 Config file: %s\n", configPath)
	fmt.Println("🚀 You can now create presets with refresh tokens")
}

func configShowCmdHandler(cmd *cobra.Command, args []string) {
	fmt.Println("📋 Current GA4 Admin Configuration:")
	fmt.Println()

	// Load configuration
	appConfig, err := config.LoadConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to load configuration: %v\n", err)
		os.Exit(1)
	}

	// Display config path
	configPath, _ := config.GetConfigPath()
	fmt.Printf("📁 Config Location: %s\n", configPath)
	fmt.Println()

	// Display OAuth credentials status
	if appConfig.ClientID != "" {
		fmt.Printf("🔑 OAuth Client ID: %s...%s (configured)\n", 
			appConfig.ClientID[:min(12, len(appConfig.ClientID))], 
			appConfig.ClientID[max(0, len(appConfig.ClientID)-4):])
		fmt.Printf("🔐 OAuth Client Secret: [HIDDEN] (configured)\n")
	} else {
		fmt.Println("❌ OAuth Client ID: Not configured")
		fmt.Println("❌ OAuth Client Secret: Not configured")
		fmt.Println()
		fmt.Println("💡 Run 'ga4admin config set --client-id <id> --client-secret <secret>' to configure")
	}

	// Display active preset
	if appConfig.ActivePreset != "" {
		fmt.Printf("🎯 Active Preset: %s\n", appConfig.ActivePreset)
	} else {
		fmt.Println("📝 Active Preset: None")
	}

	// Display timestamps
	fmt.Println()
	fmt.Printf("📅 Created: %s\n", appConfig.CreatedAt.Format("2006-01-02 15:04:05"))
	fmt.Printf("🔄 Updated: %s\n", appConfig.UpdatedAt.Format("2006-01-02 15:04:05"))
}

// Helper functions
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func presetCreateCmdHandler(cmd *cobra.Command, args []string) {
	presetName := args[0]
	refreshToken, _ := cmd.Flags().GetString("refresh-token")
	userEmail, _ := cmd.Flags().GetString("user-email")
	noValidate, _ := cmd.Flags().GetBool("no-validate")

	fmt.Printf("➕ Creating preset '%s'...\n", presetName)

	// Validate OAuth credentials are configured
	hasCredentials, err := config.HasClientCredentials()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to check OAuth configuration: %v\n", err)
		os.Exit(1)
	}
	if !hasCredentials {
		fmt.Fprintf(os.Stderr, "Error: OAuth client credentials not configured\n")
		fmt.Fprintf(os.Stderr, "💡 Run 'ga4admin config set --client-id <id> --client-secret <secret>' first\n")
		os.Exit(1)
	}

	// Validate refresh token (unless --no-validate is specified)
	if !noValidate {
		fmt.Println("🔍 Validating refresh token...")
		
		// Create auth client for validation
		authClient, err := api.NewAuthClient()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: Failed to create auth client for validation: %v\n", err)
			os.Exit(1)
		}

		// Test the refresh token
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := authClient.ValidateRefreshToken(ctx, refreshToken); err != nil {
			fmt.Fprintf(os.Stderr, "Error: Refresh token validation failed: %v\n", err)
			fmt.Fprintf(os.Stderr, "\n💡 Common issues:\n")
			fmt.Fprintf(os.Stderr, "   - Token has expired or been revoked\n")
			fmt.Fprintf(os.Stderr, "   - Token doesn't have required GA4 permissions\n")
			fmt.Fprintf(os.Stderr, "   - Network connectivity issues\n")
			fmt.Fprintf(os.Stderr, "\n🔧 To skip validation: add --no-validate flag\n")
			os.Exit(1)
		}

		fmt.Println("✅ Refresh token is valid!")
	} else {
		fmt.Println("⚠️  Skipping token validation (--no-validate specified)")
	}

	// Create the preset
	if err := preset.CreatePreset(presetName, refreshToken, userEmail); err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to create preset: %v\n", err)
		os.Exit(1)
	}

	// Get preset path for display
	presetPath, _ := preset.GetPresetPath(presetName)
	fmt.Printf("✅ Preset '%s' created successfully\n", presetName)
	fmt.Printf("📁 Preset file: %s\n", presetPath)
	if userEmail != "" {
		fmt.Printf("👤 User email: %s\n", userEmail)
	}
	
	if noValidate {
		fmt.Println("⚠️  Remember: Token was not validated - test with API commands")
	}
	fmt.Println("🚀 You can now use 'ga4admin preset use " + presetName + "' to activate it")
}

func presetListCmdHandler(cmd *cobra.Command, args []string) {
	fmt.Println("📝 Available GA4 Presets:")
	fmt.Println()

	// Get active preset name
	activePresetName, err := config.GetActivePreset()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to get active preset: %v\n", err)
		os.Exit(1)
	}

	// Load all presets
	presets, err := preset.ListPresets()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to list presets: %v\n", err)
		os.Exit(1)
	}

	if len(presets) == 0 {
		fmt.Println("❌ No presets found")
		fmt.Println()
		fmt.Println("💡 Create your first preset with:")
		fmt.Println("   ga4admin preset create <name> --refresh-token <token>")
		return
	}

	// Display presets
	for i, p := range presets {
		// Active preset indicator
		activeIndicator := "  "
		if p.Name == activePresetName {
			activeIndicator = "▶️ "
		}

		fmt.Printf("%s📋 %s\n", activeIndicator, p.Name)
		
		// User email if available
		if p.UserEmail != "" {
			fmt.Printf("   👤 %s\n", p.UserEmail)
		}

		// Account count
		accountCount := len(p.Accounts)
		if accountCount > 0 {
			fmt.Printf("   🏢 %d account(s)\n", accountCount)
		}

		// Timestamps
		fmt.Printf("   📅 Created: %s\n", p.CreatedAt.Format("2006-01-02 15:04"))
		fmt.Printf("   🔄 Last used: %s\n", p.LastUsed.Format("2006-01-02 15:04"))

		// Add spacing between presets
		if i < len(presets)-1 {
			fmt.Println()
		}
	}

	fmt.Println()
	fmt.Println("💡 Use 'ga4admin preset use <name>' to set active preset")
}

func presetDeleteCmdHandler(cmd *cobra.Command, args []string) {
	presetName := args[0]

	// Check if preset exists
	exists, err := preset.PresetExists(presetName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to check preset: %v\n", err)
		os.Exit(1)
	}
	if !exists {
		fmt.Fprintf(os.Stderr, "Error: Preset '%s' does not exist\n", presetName)
		os.Exit(1)
	}

	// Confirmation prompt
	fmt.Printf("⚠️  Are you sure you want to delete preset '%s'? (y/N): ", presetName)
	var response string
	fmt.Scanln(&response)
	
	response = strings.ToLower(strings.TrimSpace(response))
	if response != "y" && response != "yes" {
		fmt.Println("❌ Deletion cancelled")
		return
	}

	// Delete the preset
	if err := preset.DeletePreset(presetName); err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to delete preset: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("✅ Preset '%s' deleted successfully\n", presetName)
}

func presetUseCmdHandler(cmd *cobra.Command, args []string) {
	presetName := args[0]

	// Set active preset
	if err := preset.SetActivePreset(presetName); err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to set active preset: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("✅ Activated preset '%s'\n", presetName)
	fmt.Println("🚀 You can now use GA4 API commands")
}

func accountsListCmd(cmd *cobra.Command, args []string) {
	fmt.Println("🏢 Listing GA4 accounts...")

	accounts, err := getAccountsWithClient()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if len(accounts) == 0 {
		fmt.Println("❌ No GA4 accounts found")
		fmt.Println("💡 Ensure the refresh token has GA4 read permissions")
		return
	}

	// Display accounts
	fmt.Printf("📊 Found %d account(s):\n\n", len(accounts))
	for i, account := range accounts {
		fmt.Printf("🏢 %s (ID: %s)\n", account.DisplayName, account.ID)
		fmt.Printf("   🌍 Region: %s\n", account.RegionCode)
		fmt.Printf("   📅 Created: %s\n", account.CreateTime.Format("2006-01-02"))
		
		if i < len(accounts)-1 {
			fmt.Println()
		}
	}

	fmt.Println("\n💡 Use 'ga4admin accounts tree' for hierarchical view")
	fmt.Println("💡 Use 'ga4admin properties list --account <id>' to see properties")
}

func accountsTreeCmd(cmd *cobra.Command, args []string) {
	fmt.Println("🌳 GA4 Account & Property Tree:")
	fmt.Println()

	// Get accounts
	accounts, err := getAccountsWithClient()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if len(accounts) == 0 {
		fmt.Println("❌ No GA4 accounts found")
		fmt.Println("💡 Ensure the refresh token has GA4 read permissions")
		return
	}

	// Create Admin API client
	adminClient, err := api.NewAdminClient()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to create Admin API client: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Display accounts with properties in tree format
	for accountIndex, account := range accounts {
		// Account level
		isLastAccount := accountIndex == len(accounts)-1
		accountPrefix := "├── "
		childPrefix := "│   "
		if isLastAccount {
			accountPrefix = "└── "
			childPrefix = "    "
		}

		fmt.Printf("%s🏢 %s (ID: %s)\n", accountPrefix, account.DisplayName, account.ID)
		fmt.Printf("%s   🌍 %s • 📅 %s\n", childPrefix, account.RegionCode, account.CreateTime.Format("2006-01-02"))
		
		// Get properties for this account
		fmt.Printf("%s   🔍 Loading properties...\n", childPrefix)
		properties, err := adminClient.ListProperties(ctx, account.ID)
		if err != nil {
			fmt.Printf("%s   ❌ Error loading properties: %v\n", childPrefix, err)
			continue
		}

		if len(properties) == 0 {
			fmt.Printf("%s   📭 No properties found\n", childPrefix)
		} else {
			fmt.Printf("%s   📊 %d propert(y/ies):\n", childPrefix, len(properties))
			
			// Display properties in simple list
			for propIndex, property := range properties {
				isLastProp := propIndex == len(properties)-1
				propPrefix := "├── "
				if isLastProp {
					propPrefix = "└── "
				}
				
				// Service level indicator
				serviceIcon := "📊"
				if property.ServiceLevel == "GOOGLE_ANALYTICS_360" {
					serviceIcon = "🎯" // Premium/360
				}
				
				fmt.Printf("%s   %s%s %s (ID: %s)\n", 
					childPrefix, propPrefix, serviceIcon, property.DisplayName, property.ID)
				fmt.Printf("%s      💰 %s • 🌍 %s • 📅 %s\n", 
					childPrefix, property.CurrencyCode, property.TimeZone, property.CreateTime.Format("2006-01-02"))
			}
		}
		
		if !isLastAccount {
			fmt.Println()
		}
	}
	
	fmt.Println()
	fmt.Printf("🎯 Total: %d account(s) discovered\n", len(accounts))
	fmt.Println("💡 Use 'ga4admin properties show <property-id>' for detailed property information")
}

// Helper function to get accounts with proper error handling
func getAccountsWithClient() ([]config.Account, error) {
	// Get active preset
	activePreset, err := preset.GetActivePreset()
	if err != nil {
		return nil, err
	}

	if activePreset == nil {
		return nil, fmt.Errorf("no active preset - run 'ga4admin preset use <name>' first")
	}

	// Create Admin API client
	adminClient, err := api.NewAdminClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create Admin API client: %w", err)
	}

	// List accounts
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	accounts, err := adminClient.ListAccounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list accounts: %w", err)
	}

	return accounts, nil
}

func propertiesListCmd(cmd *cobra.Command, args []string) {
	accountID, _ := cmd.Flags().GetString("account")
	fmt.Printf("🏠 Listing GA4 properties for account %s...\n", accountID)

	// Get active preset
	activePreset, err := preset.GetActivePreset()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if activePreset == nil {
		fmt.Fprintf(os.Stderr, "Error: No active preset - run 'ga4admin preset use <name>' first\n")
		os.Exit(1)
	}

	// Create Admin API client
	adminClient, err := api.NewAdminClient()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to create Admin API client: %v\n", err)
		os.Exit(1)
	}

	// List properties
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	properties, err := adminClient.ListProperties(ctx, accountID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to list properties: %v\n", err)
		os.Exit(1)
	}

	if len(properties) == 0 {
		fmt.Printf("❌ No properties found for account %s\n", accountID)
		fmt.Println("💡 Ensure the account ID is correct and accessible")
		return
	}

	// Display properties
	fmt.Printf("🏠 Found %d propert(y/ies):\n\n", len(properties))
	for i, property := range properties {
		fmt.Printf("📊 %s (ID: %s)\n", property.DisplayName, property.ID)
		fmt.Printf("   💰 Currency: %s\n", property.CurrencyCode)
		fmt.Printf("   🌍 Timezone: %s\n", property.TimeZone)
		fmt.Printf("   🏭 Industry: %s\n", property.IndustryCategory)
		fmt.Printf("   📈 Service Level: %s\n", property.ServiceLevel)
		fmt.Printf("   📅 Created: %s\n", property.CreateTime.Format("2006-01-02"))
		
		if i < len(properties)-1 {
			fmt.Println()
		}
	}

	fmt.Println("\n💡 Use 'ga4admin properties show <property-id>' for detailed information")
}

func propertiesShowCmd(cmd *cobra.Command, args []string) {
	propertyID := args[0]
	fmt.Printf("📊 Property details for %s...\n", propertyID)

	// Get active preset
	activePreset, err := preset.GetActivePreset()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if activePreset == nil {
		fmt.Fprintf(os.Stderr, "Error: No active preset - run 'ga4admin preset use <name>' first\n")
		os.Exit(1)
	}

	// Create Admin API client
	adminClient, err := api.NewAdminClient()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to create Admin API client: %v\n", err)
		os.Exit(1)
	}

	// Get property details
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	property, err := adminClient.GetProperty(ctx, propertyID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to get property details: %v\n", err)
		os.Exit(1)
	}

	// Display property details
	fmt.Printf("📊 %s (ID: %s)\n\n", property.DisplayName, property.ID)
	
	fmt.Println("🔧 Configuration:")
	fmt.Printf("   💰 Currency Code: %s\n", property.CurrencyCode)
	fmt.Printf("   🌍 Timezone: %s\n", property.TimeZone)
	fmt.Printf("   🏭 Industry Category: %s\n", property.IndustryCategory)
	fmt.Printf("   📈 Service Level: %s\n", property.ServiceLevel)
	fmt.Println()
	
	fmt.Println("📅 Timeline:")
	fmt.Printf("   🆕 Created: %s\n", property.CreateTime.Format("2006-01-02 15:04:05"))
	fmt.Printf("   🔄 Last Accessed: %s\n", property.LastAccessed.Format("2006-01-02 15:04:05"))
	fmt.Println()
	
	fmt.Println("💡 Next steps:")
	fmt.Printf("   • ga4admin metadata dimensions --property %s\n", propertyID)
	fmt.Printf("   • ga4admin metadata metrics --property %s\n", propertyID)
	fmt.Printf("   • ga4admin metadata events --property %s\n", propertyID)
}

func metadataDimensionsCmd(cmd *cobra.Command, args []string) {
	propertyID, _ := cmd.Flags().GetString("property")
	customOnly, _ := cmd.Flags().GetBool("custom-only")
	category, _ := cmd.Flags().GetString("category")

	fmt.Printf("📏 Discovering dimensions for property %s...\n", propertyID)

	// Get active preset
	activePreset, err := preset.GetActivePreset()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if activePreset == nil {
		fmt.Fprintf(os.Stderr, "Error: No active preset - run 'ga4admin preset use <name>' first\n")
		os.Exit(1)
	}

	// Create Data API client with cache
	dataClient, err := createDataClientWithCache()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to create Data API client: %v\n", err)
		os.Exit(1)
	}
	defer dataClient.Close()

	// Get metadata
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	metadata, err := dataClient.GetMetadata(ctx, propertyID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to get metadata: %v\n", err)
		os.Exit(1)
	}

	// Filter and display dimensions
	filteredDimensions := make([]api.DimensionMetadata, 0)
	for _, dim := range metadata.Dimensions {
		// Apply filters
		if customOnly && !dim.CustomDefinition {
			continue
		}
		if category != "" && dim.Category != category {
			continue
		}
		filteredDimensions = append(filteredDimensions, dim)
	}

	if len(filteredDimensions) == 0 {
		fmt.Println("❌ No dimensions found matching your criteria")
		return
	}

	// Display results
	fmt.Printf("📊 Found %d dimension(s):\n\n", len(filteredDimensions))
	
	// Group by category
	categories := make(map[string][]api.DimensionMetadata)
	for _, dim := range filteredDimensions {
		cat := dim.Category
		if cat == "" {
			cat = "Other"
		}
		categories[cat] = append(categories[cat], dim)
	}

	for category, dims := range categories {
		fmt.Printf("🏷️  %s (%d)\n", category, len(dims))
		for _, dim := range dims {
			customIndicator := ""
			if dim.CustomDefinition {
				customIndicator = " 🔧"
			}
			
			fmt.Printf("   • %s%s\n", dim.APIName, customIndicator)
			fmt.Printf("     UI Name: %s\n", dim.UIName)
			if dim.Description != "" {
				fmt.Printf("     %s\n", dim.Description)
			}
		}
		fmt.Println()
	}

	fmt.Printf("💡 Total: %d dimensions (%d custom)\n", 
		len(metadata.Dimensions), countCustom(metadata.Dimensions))
	fmt.Printf("💡 Use 'ga4admin metadata metrics --property %s' to see available metrics\n", propertyID)
}

func metadataMetricsCmd(cmd *cobra.Command, args []string) {
	propertyID, _ := cmd.Flags().GetString("property")
	customOnly, _ := cmd.Flags().GetBool("custom-only")
	category, _ := cmd.Flags().GetString("category")
	metricType, _ := cmd.Flags().GetString("type")

	fmt.Printf("📈 Discovering metrics for property %s...\n", propertyID)

	// Get active preset
	activePreset, err := preset.GetActivePreset()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if activePreset == nil {
		fmt.Fprintf(os.Stderr, "Error: No active preset - run 'ga4admin preset use <name>' first\n")
		os.Exit(1)
	}

	// Create Data API client with cache
	dataClient, err := createDataClientWithCache()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to create Data API client: %v\n", err)
		os.Exit(1)
	}
	defer dataClient.Close()

	// Get metadata
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	metadata, err := dataClient.GetMetadata(ctx, propertyID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to get metadata: %v\n", err)
		os.Exit(1)
	}

	// Filter and display metrics
	filteredMetrics := make([]api.MetricMetadata, 0)
	for _, metric := range metadata.Metrics {
		// Apply filters
		if customOnly && !metric.CustomDefinition {
			continue
		}
		if category != "" && metric.Category != category {
			continue
		}
		if metricType != "" && metric.Type != metricType {
			continue
		}
		filteredMetrics = append(filteredMetrics, metric)
	}

	if len(filteredMetrics) == 0 {
		fmt.Println("❌ No metrics found matching your criteria")
		return
	}

	// Display results
	fmt.Printf("📊 Found %d metric(s):\n\n", len(filteredMetrics))
	
	// Group by category
	categories := make(map[string][]api.MetricMetadata)
	for _, metric := range filteredMetrics {
		cat := metric.Category
		if cat == "" {
			cat = "Other"
		}
		categories[cat] = append(categories[cat], metric)
	}

	for category, metrics := range categories {
		fmt.Printf("🏷️  %s (%d)\n", category, len(metrics))
		for _, metric := range metrics {
			customIndicator := ""
			if metric.CustomDefinition {
				customIndicator = " 🔧"
			}
			
			typeIndicator := ""
			if metric.Type != "" {
				typeIndicator = fmt.Sprintf(" [%s]", metric.Type)
			}
			
			fmt.Printf("   • %s%s%s\n", metric.APIName, typeIndicator, customIndicator)
			fmt.Printf("     UI Name: %s\n", metric.UIName)
			if metric.Description != "" {
				fmt.Printf("     %s\n", metric.Description)
			}
		}
		fmt.Println()
	}

	fmt.Printf("💡 Total: %d metrics (%d custom)\n", 
		len(metadata.Metrics), countCustomMetrics(metadata.Metrics))
	fmt.Printf("💡 Use 'ga4admin metadata events --property %s' to analyze event volumes\n", propertyID)
}

func metadataEventsCmd(cmd *cobra.Command, args []string) {
	propertyID, _ := cmd.Flags().GetString("property")
	days, _ := cmd.Flags().GetInt("days")
	limit, _ := cmd.Flags().GetInt("limit")

	fmt.Printf("📅 Analyzing events for property %s (%d days)...\n", propertyID, days)

	// Get active preset
	activePreset, err := preset.GetActivePreset()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if activePreset == nil {
		fmt.Fprintf(os.Stderr, "Error: No active preset - run 'ga4admin preset use <name>' first\n")
		os.Exit(1)
	}

	// Create Data API client with cache
	dataClient, err := createDataClientWithCache()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to create Data API client: %v\n", err)
		os.Exit(1)
	}
	defer dataClient.Close()

	// Analyze events
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	analysis, err := dataClient.AnalyzeEvents(ctx, propertyID, days)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to analyze events: %v\n", err)
		os.Exit(1)
	}

	// Display results
	if analysis.TotalEvents == 0 {
		fmt.Printf("❌ No events found in the last %d days\n", days)
		fmt.Println("💡 This might indicate no data collection or a very new property")
		return
	}

	fmt.Printf("📊 Event Analysis Results:\n\n")
	fmt.Printf("📈 Total Events: %d unique event types\n", analysis.TotalEvents)
	fmt.Printf("🔢 Total Event Count: %s\n", formatNumber(analysis.TotalEventCount))
	fmt.Printf("👥 Total Active Users: %s\n", formatNumber(analysis.TotalActiveUsers))
	fmt.Printf("🎯 Events per User: %.1f\n", float64(analysis.TotalEventCount)/float64(analysis.TotalActiveUsers))
	fmt.Println()

	// Show top events (limited by user preference)
	displayLimit := limit
	if displayLimit > len(analysis.Events) {
		displayLimit = len(analysis.Events)
	}

	fmt.Printf("🔥 Top %d Events:\n\n", displayLimit)
	for i, event := range analysis.Events[:displayLimit] {
		rank := i + 1
		percentage := (float64(event.EventCount) / float64(analysis.TotalEventCount)) * 100
		
		fmt.Printf("%2d. %s\n", rank, event.EventName)
		fmt.Printf("    📊 %s events (%.1f%% of total)\n", formatNumber(event.EventCount), percentage)
		fmt.Printf("    👥 %s users (%.1f events/user)\n", formatNumber(event.ActiveUsers), event.EventsPerUser)
		
		// Identify potential conversion events
		if isLikelyConversionEvent(event.EventName) {
			fmt.Printf("    🎯 Likely conversion event\n")
		}
		fmt.Println()
	}

	fmt.Printf("💡 Analyzed %d days of data (updated %s)\n", days, analysis.AnalyzedAt.Format("2006-01-02 15:04"))
	fmt.Printf("💡 Use 'ga4admin metadata dimensions --property %s' to see available dimensions\n", propertyID)
}

// Helper functions
func countCustom(dimensions []api.DimensionMetadata) int {
	count := 0
	for _, dim := range dimensions {
		if dim.CustomDefinition {
			count++
		}
	}
	return count
}

func countCustomMetrics(metrics []api.MetricMetadata) int {
	count := 0
	for _, metric := range metrics {
		if metric.CustomDefinition {
			count++
		}
	}
	return count
}

func formatNumber(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	} else if n < 1000000 {
		return fmt.Sprintf("%.1fK", float64(n)/1000)
	} else if n < 1000000000 {
		return fmt.Sprintf("%.1fM", float64(n)/1000000)
	}
	return fmt.Sprintf("%.1fB", float64(n)/1000000000)
}

func isLikelyConversionEvent(eventName string) bool {
	conversionKeywords := []string{
		"purchase", "conversion", "complete", "submit", "signup", "register", 
		"subscribe", "download", "checkout", "payment", "order", "buy",
		"generate_lead", "sign_up", "login", "add_payment_info",
	}
	
	eventLower := strings.ToLower(eventName)
	for _, keyword := range conversionKeywords {
		if strings.Contains(eventLower, keyword) {
			return true
		}
	}
	return false
}

func testAuthCmdHandler(cmd *cobra.Command, args []string) {
	fmt.Println("🔐 Testing OAuth2 Authentication...")
	
	// Create auth client
	authClient, err := api.NewAuthClient()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to create auth client: %v\n", err)
		os.Exit(1)
	}
	
	// Get active preset info
	activePreset, err := preset.GetActivePreset()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	
	if activePreset == nil {
		fmt.Fprintf(os.Stderr, "Error: No active preset - run 'ga4admin preset use <name>' first\n")
		os.Exit(1)
	}
	
	fmt.Printf("📋 Active Preset: %s\n", activePreset.Name)
	if activePreset.UserEmail != "" {
		fmt.Printf("👤 User: %s\n", activePreset.UserEmail)
	}
	
	// Test token refresh
	fmt.Println("🔄 Testing token refresh...")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	token, err := authClient.GetAccessToken(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Token refresh failed: %v\n", err)
		os.Exit(1)
	}
	
	fmt.Printf("✅ Token refresh successful!\n")
	fmt.Printf("🎯 Access Token: %s...%s\n", token.AccessToken[:20], token.AccessToken[len(token.AccessToken)-4:])
	fmt.Printf("⏰ Expires: %s\n", token.Expiry.Format("2006-01-02 15:04:05"))
	fmt.Printf("⏳ Valid for: %s\n", time.Until(token.Expiry).Round(time.Second))
	
	// Test HTTP client
	fmt.Println("🌐 Testing authenticated HTTP client...")
	httpClient, err := authClient.AuthenticatedHTTPClient(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to create HTTP client: %v\n", err)
		os.Exit(1)
	}
	
	// Make a test request to GA4 Admin API accounts endpoint
	resp, err := httpClient.Get("https://analyticsadmin.googleapis.com/v1alpha/accounts")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Test API call failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	
	fmt.Printf("🚀 Test API call successful!\n")
	fmt.Printf("📊 Status: %s\n", resp.Status)
	
	if resp.StatusCode == 200 {
		fmt.Println("✨ OAuth2 authentication is working correctly!")
		fmt.Println("🎉 Ready for GA4 API integration")
	} else {
		fmt.Printf("⚠️  Unexpected status code: %d\n", resp.StatusCode)
		fmt.Println("💡 This might indicate permission issues")
	}
	
	// Show token cache info
	tokenInfo := authClient.GetTokenInfo()
	fmt.Println("\n📈 Token Cache Info:")
	for key, value := range tokenInfo {
		fmt.Printf("  %s: %v\n", key, value)
	}
}

// Helper function to create a cache-enabled data client
func createDataClientWithCache() (*api.DataClient, error) {
	// Get active preset name for cache
	activePreset, err := preset.GetActivePreset()
	if err != nil {
		return nil, fmt.Errorf("failed to get active preset: %w", err)
	}
	if activePreset == nil {
		return nil, fmt.Errorf("no active preset - run 'ga4admin preset use <name>' first")
	}

	// Create cache client
	cacheClient, err := cache.NewCacheClient(activePreset.Name)
	if err != nil {
		// Fall back to non-cached client if cache fails
		fmt.Fprintf(os.Stderr, "Warning: Failed to create cache client, using non-cached mode: %v\n", err)
		return api.NewDataClient()
	}

	// Create data client with cache
	return api.NewDataClientWithCache(cacheClient)
}

// Query command handlers

func queryRunCmd(cmd *cobra.Command, args []string) {
	propertyID, _ := cmd.Flags().GetString("property")
	dimensions, _ := cmd.Flags().GetStringSlice("dimensions")
	metrics, _ := cmd.Flags().GetStringSlice("metrics")
	startDate, _ := cmd.Flags().GetString("start-date")
	endDate, _ := cmd.Flags().GetString("end-date")
	limit, _ := cmd.Flags().GetInt64("limit")
	filterStrings, _ := cmd.Flags().GetStringSlice("filters")
	orderBy, _ := cmd.Flags().GetString("order-by")
	queryName, _ := cmd.Flags().GetString("name")
	// noCache, _ := cmd.Flags().GetBool("no-cache") // TODO: Implement cache skipping

	fmt.Printf("🚀 Executing GA4 query for property %s...\n", propertyID)

	// Validate basic requirements
	if len(dimensions) == 0 && len(metrics) == 0 {
		fmt.Fprintf(os.Stderr, "Error: At least one dimension or metric is required\n")
		fmt.Fprintf(os.Stderr, "Example: --dimensions sessionSource,sessionMedium --metrics activeUsers,sessions\n")
		os.Exit(1)
	}

	// Create data client
	dataClient, err := createDataClientWithCache()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to create data client: %v\n", err)
		os.Exit(1)
	}
	defer dataClient.Close()

	// Build query configuration
	config := &query.QueryConfig{
		PropertyID: propertyID,
		Name:       queryName,
		Dimensions: dimensions,
		Metrics:    metrics,
		StartDate:  startDate,
		EndDate:    endDate,
		Limit:      limit,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}

	// Parse filters if provided
	if len(filterStrings) > 0 {
		filters, err := parseFilters(filterStrings)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: Invalid filter format: %v\n", err)
			fmt.Fprintf(os.Stderr, "Filter format: field:type:operation:value\n")
			fmt.Fprintf(os.Stderr, "Example: sessionSource:string:EXACT:google\n")
			os.Exit(1)
		}
		config.Filters = filters
	}

	// Parse order by if provided
	if orderBy != "" {
		orderConfig, err := parseOrderBy(orderBy, config)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: Invalid order-by format: %v\n", err)
			os.Exit(1)
		}
		config.OrderBy = []query.OrderByConfig{*orderConfig}
	}

	// Execute query
	executor := query.NewExecutor(dataClient)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	result, err := executor.Execute(ctx, config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Query execution failed: %v\n", err)
		os.Exit(1)
	}

	// Display results
	fmt.Printf("✅ Query completed successfully!\n")
	fmt.Printf("📊 Returned %d rows in %s\n", result.RowCount, result.ExecutionTime)
	if result.FromCache {
		fmt.Printf("⚡ Results served from cache\n")
	}
	fmt.Println()

	// Show result table
	if result.RowCount > 0 {
		// Create results manager for formatting
		cacheClient, _ := cache.NewCacheClient("temp") // For formatting only
		resultsManager := results.NewManager(cacheClient)
		
		lines, err := resultsManager.FormatResultTable(result, 20, 30)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error formatting results: %v\n", err)
		} else {
			for _, line := range lines {
				fmt.Println(line)
			}
		}
		cacheClient.Close()
	}

	fmt.Println()
	fmt.Printf("💡 Query ID: %s\n", result.QueryID)
	fmt.Printf("💡 Use 'ga4admin results show %s' to see full results\n", result.QueryID)
	fmt.Printf("💡 Use 'ga4admin results export %s output.csv' to export data\n", result.QueryID)
}

func queryBuildCmd(cmd *cobra.Command, args []string) {
	propertyID, _ := cmd.Flags().GetString("property")
	
	fmt.Printf("🔧 Starting interactive query builder for property %s\n", propertyID)

	// Create data client
	dataClient, err := createDataClientWithCache()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to create data client: %v\n", err)
		os.Exit(1)
	}
	defer dataClient.Close()

	// Create query builder
	builder := query.NewQueryBuilder(dataClient, propertyID)

	// Build query interactively
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second) // 5 minutes
	defer cancel()

	config, err := builder.BuildInteractively(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Query building failed: %v\n", err)
		os.Exit(1)
	}

	// Validate the query
	if err := builder.ValidateQuery(config); err != nil {
		fmt.Fprintf(os.Stderr, "Error: Query validation failed: %v\n", err)
		os.Exit(1)
	}

	// Ask if user wants to execute now
	fmt.Println("\n🎯 Query Configuration Complete!")
	fmt.Printf("📊 Property: %s\n", config.PropertyID)
	fmt.Printf("📏 Dimensions: %s\n", strings.Join(config.Dimensions, ", "))
	fmt.Printf("📈 Metrics: %s\n", strings.Join(config.Metrics, ", "))
	fmt.Printf("📅 Date Range: %s to %s\n", config.StartDate, config.EndDate)
	fmt.Printf("🔢 Limit: %d rows\n", config.Limit)
	if len(config.Filters) > 0 {
		fmt.Printf("🔍 Filters: %d applied\n", len(config.Filters))
	}

	fmt.Print("\nExecute this query now? (y/N): ")
	var execute string
	fmt.Scanln(&execute)

	if strings.ToLower(strings.TrimSpace(execute)) == "y" {
		fmt.Println("\n🚀 Executing query...")
		
		executor := query.NewExecutor(dataClient)
		result, err := executor.Execute(ctx, config)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: Query execution failed: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("✅ Query completed! Returned %d rows in %s\n", result.RowCount, result.ExecutionTime)
		fmt.Printf("💡 Query ID: %s\n", result.QueryID)
	} else {
		fmt.Println("Query configuration saved but not executed.")
	}
}

func queryListCmd(cmd *cobra.Command, args []string) {
	propertyFilter, _ := cmd.Flags().GetString("property")
	limit, _ := cmd.Flags().GetInt("limit")

	fmt.Println("📋 Cached Queries:")
	fmt.Println()

	// Get active preset for cache access
	activePreset, err := preset.GetActivePreset()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	if activePreset == nil {
		fmt.Fprintf(os.Stderr, "Error: No active preset\n")
		os.Exit(1)
	}

	// Create cache client and results manager
	cacheClient, err := cache.NewCacheClient(activePreset.Name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to create cache client: %v\n", err)
		os.Exit(1)
	}
	defer cacheClient.Close()

	resultsManager := results.NewManager(cacheClient)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var resultsList []results.ResultSummary
	if propertyFilter != "" {
		resultsList, err = resultsManager.ListResults(ctx, propertyFilter, limit)
	} else {
		// TODO: List results for all properties
		fmt.Fprintf(os.Stderr, "Error: Property filter is required for now\n")
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to list results: %v\n", err)
		os.Exit(1)
	}

	if len(resultsList) == 0 {
		fmt.Println("❌ No cached queries found")
		fmt.Println("💡 Run 'ga4admin query run' to create your first query")
		return
	}

	// Display results
	for i, summary := range resultsList {
		fmt.Printf("🔍 %s\n", summary.QueryID)
		fmt.Printf("   📊 %d rows • 📅 %s\n", summary.RowCount, summary.CreatedAt.Format("2006-01-02 15:04"))
		if summary.TableName != "" {
			fmt.Printf("   🏷️  %s\n", summary.TableName)
		}
		if summary.IsExpired {
			fmt.Printf("   ⏰ Expired\n")
		}
		
		if i < len(resultsList)-1 {
			fmt.Println()
		}
	}

	fmt.Printf("\n💡 Showing %d of cached queries\n", len(resultsList))
	fmt.Printf("💡 Use 'ga4admin results show <query-id>' to see details\n")
}

// Results command handlers

func resultsListCmd(cmd *cobra.Command, args []string) {
	propertyFilter, _ := cmd.Flags().GetString("property")
	limit, _ := cmd.Flags().GetInt("limit")

	fmt.Println("📊 Cached Query Results:")
	fmt.Println()

	if propertyFilter == "" {
		fmt.Fprintf(os.Stderr, "Error: --property flag is required\n")
		os.Exit(1)
	}

	// Get active preset for cache access
	activePreset, err := preset.GetActivePreset()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	if activePreset == nil {
		fmt.Fprintf(os.Stderr, "Error: No active preset\n")
		os.Exit(1)
	}

	// Create cache client and results manager
	cacheClient, err := cache.NewCacheClient(activePreset.Name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to create cache client: %v\n", err)
		os.Exit(1)
	}
	defer cacheClient.Close()

	resultsManager := results.NewManager(cacheClient)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resultsList, err := resultsManager.ListResults(ctx, propertyFilter, limit)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to list results: %v\n", err)
		os.Exit(1)
	}

	if len(resultsList) == 0 {
		fmt.Printf("❌ No cached results found for property %s\n", propertyFilter)
		fmt.Println("💡 Run 'ga4admin query run' to create results")
		return
	}

	// Display results
	for i, summary := range resultsList {
		statusIcon := "✅"
		if summary.IsExpired {
			statusIcon = "⏰"
		}

		fmt.Printf("%s %s\n", statusIcon, summary.QueryID)
		fmt.Printf("   📊 %d rows • 📅 %s • 🔄 %s\n", 
			summary.RowCount, 
			summary.CreatedAt.Format("2006-01-02 15:04"),
			summary.LastAccessed.Format("2006-01-02 15:04"))
		
		if summary.TableName != "" {
			fmt.Printf("   🏷️  %s: %s\n", summary.TableName, summary.Description)
		}
		
		if i < len(resultsList)-1 {
			fmt.Println()
		}
	}

	fmt.Printf("\n💡 Total: %d cached results\n", len(resultsList))
	fmt.Printf("💡 Use 'ga4admin results show <query-id>' for detailed view\n")
}

func resultsShowCmd(cmd *cobra.Command, args []string) {
	queryID := args[0]
	maxRows, _ := cmd.Flags().GetInt("max-rows")
	maxWidth, _ := cmd.Flags().GetInt("max-width")
	showTotals, _ := cmd.Flags().GetBool("show-totals")

	fmt.Printf("📊 Query Result: %s\n", queryID)

	// Get active preset for cache access
	activePreset, err := preset.GetActivePreset()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	if activePreset == nil {
		fmt.Fprintf(os.Stderr, "Error: No active preset\n")
		os.Exit(1)
	}

	// Create cache client and results manager
	cacheClient, err := cache.NewCacheClient(activePreset.Name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to create cache client: %v\n", err)
		os.Exit(1)
	}
	defer cacheClient.Close()

	resultsManager := results.NewManager(cacheClient)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := resultsManager.GetResult(ctx, queryID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to get result: %v\n", err)
		os.Exit(1)
	}

	// Show metadata
	fmt.Printf("📈 Property: %s\n", result.PropertyID)
	fmt.Printf("📅 Executed: %s (%s)\n", result.ExecutedAt.Format("2006-01-02 15:04:05"), result.ExecutionTime)
	fmt.Printf("📊 Rows: %d\n", result.RowCount)
	if result.FromCache {
		fmt.Printf("⚡ From cache\n")
	}
	
	// Show query configuration
	if result.QueryConfig != nil {
		fmt.Printf("📏 Dimensions: %s\n", strings.Join(result.QueryConfig.Dimensions, ", "))
		fmt.Printf("📈 Metrics: %s\n", strings.Join(result.QueryConfig.Metrics, ", "))
		fmt.Printf("📅 Date range: %s to %s\n", result.QueryConfig.StartDate, result.QueryConfig.EndDate)
	}
	fmt.Println()

	// Show data table
	if result.RowCount > 0 {
		lines, err := resultsManager.FormatResultTable(result, maxRows, maxWidth)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error formatting table: %v\n", err)
		} else {
			for _, line := range lines {
				fmt.Println(line)
			}
		}

		// Show totals if requested and available
		if showTotals && len(result.Totals) > 0 {
			fmt.Println("\n📊 Totals:")
			for _, total := range result.Totals {
				fmt.Printf("   ")
				for _, value := range total.MetricValues {
					fmt.Printf("%-15s ", value.Value)
				}
				fmt.Println()
			}
		}
	}

	fmt.Printf("\n💡 Export: ga4admin results export %s output.csv\n", queryID)
}

func resultsExportCmd(cmd *cobra.Command, args []string) {
	queryID := args[0]
	outputFile := args[1]
	format, _ := cmd.Flags().GetString("format")
	prettify, _ := cmd.Flags().GetBool("prettify")

	fmt.Printf("📤 Exporting result %s to %s (%s format)...\n", queryID, outputFile, format)

	// Get active preset for cache access
	activePreset, err := preset.GetActivePreset()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	if activePreset == nil {
		fmt.Fprintf(os.Stderr, "Error: No active preset\n")
		os.Exit(1)
	}

	// Create cache client and results manager
	cacheClient, err := cache.NewCacheClient(activePreset.Name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to create cache client: %v\n", err)
		os.Exit(1)
	}
	defer cacheClient.Close()

	resultsManager := results.NewManager(cacheClient)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Export based on format
	switch strings.ToLower(format) {
	case "csv":
		err = resultsManager.ExportToCSV(ctx, queryID, outputFile)
	case "json":
		err = resultsManager.ExportToJSON(ctx, queryID, outputFile, prettify)
	default:
		fmt.Fprintf(os.Stderr, "Error: Unsupported format '%s'. Supported: csv, json\n", format)
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Export failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("✅ Export completed successfully!\n")
	fmt.Printf("📁 File: %s\n", outputFile)
}

func resultsStatsCmd(cmd *cobra.Command, args []string) {
	propertyID, _ := cmd.Flags().GetString("property")
	
	if propertyID == "" {
		fmt.Fprintf(os.Stderr, "Error: --property flag is required\n")
		os.Exit(1)
	}

	fmt.Printf("📈 Result Statistics for Property %s\n", propertyID)

	// Get active preset for cache access
	activePreset, err := preset.GetActivePreset()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	if activePreset == nil {
		fmt.Fprintf(os.Stderr, "Error: No active preset\n")
		os.Exit(1)
	}

	// Create cache client and results manager
	cacheClient, err := cache.NewCacheClient(activePreset.Name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to create cache client: %v\n", err)
		os.Exit(1)
	}
	defer cacheClient.Close()

	resultsManager := results.NewManager(cacheClient)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stats, err := resultsManager.GetResultStats(ctx, propertyID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to get stats: %v\n", err)
		os.Exit(1)
	}

	// Display statistics
	fmt.Printf("📊 Total Results: %d\n", stats.TotalResults)
	fmt.Printf("✅ Active: %d • ⏰ Expired: %d\n", stats.ActiveResults, stats.ExpiredResults)
	fmt.Printf("📈 Total Rows: %s\n", formatNumber(stats.TotalRows))
	fmt.Printf("📊 Average Rows/Result: %.1f\n", stats.AvgRowsPerResult)
	
	if stats.OldestResult != nil {
		fmt.Printf("⏰ Date Range: %s to %s\n", 
			stats.OldestResult.Format("2006-01-02"),
			stats.NewestResult.Format("2006-01-02"))
	}
	
	fmt.Printf("📅 Generated: %s\n", stats.GeneratedAt.Format("2006-01-02 15:04:05"))
}

// Cache command handlers

func cacheStatsCmd(cmd *cobra.Command, args []string) {
	fmt.Println("💾 Cache Statistics:")

	// Get active preset for cache access
	activePreset, err := preset.GetActivePreset()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	if activePreset == nil {
		fmt.Fprintf(os.Stderr, "Error: No active preset\n")
		os.Exit(1)
	}

	// Create cache client
	cacheClient, err := cache.NewCacheClient(activePreset.Name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to create cache client: %v\n", err)
		os.Exit(1)
	}
	defer cacheClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stats, err := cacheClient.GetCacheStats(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to get cache stats: %v\n", err)
		os.Exit(1)
	}

	// Display cache statistics
	fmt.Printf("🎯 Preset: %s\n", activePreset.Name)
	fmt.Printf("✅ Cache Hits: %d\n", stats.TotalHits)
	fmt.Printf("❌ Cache Misses: %d\n", stats.TotalMisses)
	fmt.Printf("📊 Hit Rate: %.1f%%\n", stats.HitRate)
	fmt.Printf("📁 Cache Entries: %d\n", stats.EntriesCount)
	fmt.Printf("📅 Created: %s\n", stats.CreatedAt.Format("2006-01-02 15:04:05"))
	fmt.Printf("🔄 Last Updated: %s\n", stats.UpdatedAt.Format("2006-01-02 15:04:05"))
	
	if stats.LastCleanup != nil {
		fmt.Printf("🧹 Last Cleanup: %s\n", stats.LastCleanup.Format("2006-01-02 15:04:05"))
	}
}

func cacheCleanupCmd(cmd *cobra.Command, args []string) {
	expiredOnly, _ := cmd.Flags().GetBool("expired")
	cleanAll, _ := cmd.Flags().GetBool("all")

	if cleanAll {
		fmt.Print("⚠️  Are you sure you want to clear ALL cache entries? This cannot be undone. (y/N): ")
		var confirm string
		fmt.Scanln(&confirm)
		if strings.ToLower(strings.TrimSpace(confirm)) != "y" {
			fmt.Println("❌ Cache cleanup cancelled")
			return
		}
	}

	fmt.Println("🧹 Cleaning up cache...")

	// Get active preset for cache access
	activePreset, err := preset.GetActivePreset()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	if activePreset == nil {
		fmt.Fprintf(os.Stderr, "Error: No active preset\n")
		os.Exit(1)
	}

	// Create cache client
	cacheClient, err := cache.NewCacheClient(activePreset.Name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to create cache client: %v\n", err)
		os.Exit(1)
	}
	defer cacheClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if expiredOnly || !cleanAll {
		// Clean only expired entries
		deleted, err := cacheClient.CleanupExpiredEntries(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: Cleanup failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("✅ Cleaned up %d expired cache entries\n", deleted)
	} else {
		// TODO: Implement full cache clearing if needed
		fmt.Println("❌ Full cache clearing not yet implemented")
		os.Exit(1)
	}
}

// Helper functions for query parsing

func parseFilters(filterStrings []string) ([]query.FilterConfig, error) {
	filters := make([]query.FilterConfig, 0, len(filterStrings))
	
	for _, filterStr := range filterStrings {
		parts := strings.Split(filterStr, ":")
		if len(parts) != 4 {
			return nil, fmt.Errorf("filter must have format 'field:type:operation:value', got: %s", filterStr)
		}

		filter := query.FilterConfig{
			FieldName: strings.TrimSpace(parts[0]),
			Type:      strings.ToLower(strings.TrimSpace(parts[1])),
		}

		operation := strings.TrimSpace(parts[2])
		value := strings.TrimSpace(parts[3])

		switch filter.Type {
		case "string":
			filter.StringMatchType = operation
			filter.StringValue = value
		case "numeric":
			filter.NumericOperation = operation
			if numValue, err := strconv.ParseFloat(value, 64); err == nil {
				filter.NumericValue = numValue
			} else {
				return nil, fmt.Errorf("invalid numeric value: %s", value)
			}
		default:
			return nil, fmt.Errorf("unsupported filter type: %s", filter.Type)
		}

		filters = append(filters, filter)
	}

	return filters, nil
}

func parseOrderBy(orderByStr string, config *query.QueryConfig) (*query.OrderByConfig, error) {
	orderBy := &query.OrderByConfig{}
	
	// Check for descending order (prefix with -)
	if strings.HasPrefix(orderByStr, "-") {
		orderBy.Descending = true
		orderByStr = orderByStr[1:]
	}

	orderBy.FieldName = strings.TrimSpace(orderByStr)

	// Determine field type
	for _, dim := range config.Dimensions {
		if dim == orderBy.FieldName {
			orderBy.FieldType = "dimension"
			return orderBy, nil
		}
	}

	for _, metric := range config.Metrics {
		if metric == orderBy.FieldName {
			orderBy.FieldType = "metric"
			return orderBy, nil
		}
	}

	return nil, fmt.Errorf("field '%s' not found in dimensions or metrics", orderBy.FieldName)
}

func exportParseCmd(cmd *cobra.Command, args []string) {
	inputDir, _ := cmd.Flags().GetString("input-dir")
	outputDB, _ := cmd.Flags().GetString("output-db")
	batchSize, _ := cmd.Flags().GetInt("batch-size")

	fmt.Printf("📦 Parsing JSON files from %s into DuckDB\n", inputDir)
	fmt.Printf("🎯 Output database: %s\n", outputDB)
	fmt.Printf("⚡ Batch size: %d files\n", batchSize)

	// Create parser
	parser := export.NewJSONParser(outputDB, inputDir)
	parser.SetBatchSize(batchSize)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	// Start parsing
	start := time.Now()
	if err := parser.ParseAllJSON(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to parse JSON files: %v\n", err)
		os.Exit(1)
	}

	duration := time.Since(start)
	fmt.Printf("\n✅ Parsing completed in %v\n", duration)
	fmt.Printf("🗄️  Database ready for analysis: %s\n", outputDB)
	fmt.Println("\n💡 Try these analysis queries:")
	fmt.Println("   duckdb", outputDB, "-c \"SELECT * FROM dimension_summary;\"")
	fmt.Println("   duckdb", outputDB, "-c \"SELECT * FROM property_analysis LIMIT 10;\"")
	fmt.Println("   duckdb", outputDB, "-c \"SELECT * FROM account_rollup;\"")
}