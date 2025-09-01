package config

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	ConfigDirName  = ".ga4admin"
	ConfigFileName = "config.yaml"
)

// GetConfigDir returns the path to the config directory (~/.ga4admin)
func GetConfigDir() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("failed to get user home directory: %w", err)
	}
	return filepath.Join(homeDir, ConfigDirName), nil
}

// GetConfigPath returns the full path to the config file
func GetConfigPath() (string, error) {
	configDir, err := GetConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(configDir, ConfigFileName), nil
}

// EnsureConfigDir creates the config directory if it doesn't exist
func EnsureConfigDir() error {
	configDir, err := GetConfigDir()
	if err != nil {
		return err
	}
	
	// Create directory with proper permissions (user read/write/execute only)
	return os.MkdirAll(configDir, 0700)
}

// LoadConfig reads the global configuration from ~/.ga4admin/config.yaml
func LoadConfig() (*AppConfig, error) {
	configPath, err := GetConfigPath()
	if err != nil {
		return nil, err
	}

	// Check if config file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		// Return empty config if file doesn't exist
		return &AppConfig{
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}, nil
	}

	// Read config file
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Parse YAML
	var config AppConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &config, nil
}

// SaveConfig writes the global configuration to ~/.ga4admin/config.yaml
func SaveConfig(config *AppConfig) error {
	if err := EnsureConfigDir(); err != nil {
		return err
	}

	configPath, err := GetConfigPath()
	if err != nil {
		return err
	}

	// Update timestamp
	config.UpdatedAt = time.Now()
	if config.CreatedAt.IsZero() {
		config.CreatedAt = time.Now()
	}

	// Marshal to YAML
	data, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal config to YAML: %w", err)
	}

	// Write to file with proper permissions (user read/write only)
	if err := os.WriteFile(configPath, data, 0600); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// SetClientCredentials sets the OAuth client ID and secret in global config
func SetClientCredentials(clientID, clientSecret string) error {
	config, err := LoadConfig()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	config.ClientID = clientID
	config.ClientSecret = clientSecret

	if err := SaveConfig(config); err != nil {
		return fmt.Errorf("failed to save config: %w", err)
	}

	return nil
}

// GetClientCredentials returns the OAuth client ID and secret
func GetClientCredentials() (clientID, clientSecret string, err error) {
	config, err := LoadConfig()
	if err != nil {
		return "", "", fmt.Errorf("failed to load config: %w", err)
	}

	return config.ClientID, config.ClientSecret, nil
}

// HasClientCredentials checks if OAuth credentials are configured
func HasClientCredentials() (bool, error) {
	clientID, clientSecret, err := GetClientCredentials()
	if err != nil {
		return false, err
	}

	return clientID != "" && clientSecret != "", nil
}

// SetActivePreset sets the active preset name
func SetActivePreset(presetName string) error {
	config, err := LoadConfig()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	config.ActivePreset = presetName

	if err := SaveConfig(config); err != nil {
		return fmt.Errorf("failed to save config: %w", err)
	}

	return nil
}

// GetActivePreset returns the currently active preset name
func GetActivePreset() (string, error) {
	config, err := LoadConfig()
	if err != nil {
		return "", fmt.Errorf("failed to load config: %w", err)
	}

	return config.ActivePreset, nil
}