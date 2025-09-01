package preset

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
	"ga4admin/internal/config"
)

const (
	PresetsDirName = "presets"
	PresetFileExt  = ".yaml"
)

var (
	// Valid preset names: alphanumeric, underscores, hyphens only
	validPresetName = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
)

// GetPresetsDir returns the path to the presets directory (~/.ga4admin/presets)
func GetPresetsDir() (string, error) {
	configDir, err := config.GetConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(configDir, PresetsDirName), nil
}

// GetPresetPath returns the full path to a preset file
func GetPresetPath(presetName string) (string, error) {
	if !IsValidPresetName(presetName) {
		return "", fmt.Errorf("invalid preset name: must contain only letters, numbers, underscores, and hyphens")
	}
	
	presetsDir, err := GetPresetsDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(presetsDir, presetName+PresetFileExt), nil
}

// EnsurePresetsDir creates the presets directory if it doesn't exist
func EnsurePresetsDir() error {
	presetsDir, err := GetPresetsDir()
	if err != nil {
		return err
	}
	
	// Create directory with proper permissions (user read/write/execute only)
	return os.MkdirAll(presetsDir, 0700)
}

// IsValidPresetName validates a preset name
func IsValidPresetName(name string) bool {
	if name == "" || len(name) > 50 {
		return false
	}
	return validPresetName.MatchString(name)
}

// PresetExists checks if a preset file exists
func PresetExists(presetName string) (bool, error) {
	presetPath, err := GetPresetPath(presetName)
	if err != nil {
		return false, err
	}
	
	_, err = os.Stat(presetPath)
	if os.IsNotExist(err) {
		return false, nil
	}
	return err == nil, err
}

// LoadPreset reads a preset from file
func LoadPreset(presetName string) (*config.Preset, error) {
	presetPath, err := GetPresetPath(presetName)
	if err != nil {
		return nil, err
	}

	// Check if preset file exists
	if _, err := os.Stat(presetPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("preset '%s' does not exist", presetName)
	}

	// Read preset file
	data, err := os.ReadFile(presetPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read preset file: %w", err)
	}

	// Parse YAML
	var preset config.Preset
	if err := yaml.Unmarshal(data, &preset); err != nil {
		return nil, fmt.Errorf("failed to parse preset file: %w", err)
	}

	// Update last used timestamp
	preset.LastUsed = time.Now()
	if err := SavePreset(&preset); err != nil {
		// Don't fail loading if we can't update timestamp
		// This is a non-critical operation
	}

	return &preset, nil
}

// SavePreset writes a preset to file
func SavePreset(preset *config.Preset) error {
	if !IsValidPresetName(preset.Name) {
		return fmt.Errorf("invalid preset name: %s", preset.Name)
	}

	if err := EnsurePresetsDir(); err != nil {
		return err
	}

	presetPath, err := GetPresetPath(preset.Name)
	if err != nil {
		return err
	}

	// Set creation time if not already set
	if preset.CreatedAt.IsZero() {
		preset.CreatedAt = time.Now()
	}

	// Marshal to YAML
	data, err := yaml.Marshal(preset)
	if err != nil {
		return fmt.Errorf("failed to marshal preset to YAML: %w", err)
	}

	// Write to file with proper permissions (user read/write only for security)
	if err := os.WriteFile(presetPath, data, 0600); err != nil {
		return fmt.Errorf("failed to write preset file: %w", err)
	}

	return nil
}

// DeletePreset removes a preset file
func DeletePreset(presetName string) error {
	if !IsValidPresetName(presetName) {
		return fmt.Errorf("invalid preset name: %s", presetName)
	}

	presetPath, err := GetPresetPath(presetName)
	if err != nil {
		return err
	}

	// Check if preset exists
	exists, err := PresetExists(presetName)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("preset '%s' does not exist", presetName)
	}

	// Remove the file
	if err := os.Remove(presetPath); err != nil {
		return fmt.Errorf("failed to delete preset file: %w", err)
	}

	// If this was the active preset, clear it from global config
	activePreset, err := config.GetActivePreset()
	if err == nil && activePreset == presetName {
		config.SetActivePreset("")
	}

	return nil
}

// ListPresets returns all available presets
func ListPresets() ([]config.Preset, error) {
	presetsDir, err := GetPresetsDir()
	if err != nil {
		return nil, err
	}

	// Check if presets directory exists
	if _, err := os.Stat(presetsDir); os.IsNotExist(err) {
		return []config.Preset{}, nil // Return empty list if no presets directory
	}

	// Read directory contents
	entries, err := os.ReadDir(presetsDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read presets directory: %w", err)
	}

	var presets []config.Preset
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), PresetFileExt) {
			continue
		}

		// Extract preset name from filename
		presetName := strings.TrimSuffix(entry.Name(), PresetFileExt)
		
		// Load preset (this will update last used timestamp)
		preset, err := LoadPreset(presetName)
		if err != nil {
			// Skip corrupted preset files but don't fail the entire operation
			continue
		}

		presets = append(presets, *preset)
	}

	return presets, nil
}

// CreatePreset creates a new preset with validation
func CreatePreset(name, refreshToken, userEmail string) error {
	if !IsValidPresetName(name) {
		return fmt.Errorf("invalid preset name: must contain only letters, numbers, underscores, and hyphens (max 50 chars)")
	}

	if strings.TrimSpace(refreshToken) == "" {
		return fmt.Errorf("refresh token is required")
	}

	// Check if preset already exists
	exists, err := PresetExists(name)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("preset '%s' already exists", name)
	}

	// Create new preset
	preset := &config.Preset{
		Name:         name,
		RefreshToken: strings.TrimSpace(refreshToken),
		UserEmail:    strings.TrimSpace(userEmail),
		CreatedAt:    time.Now(),
		LastUsed:     time.Now(),
		Accounts:     []config.Account{}, // Initialize empty accounts slice
	}

	// Save preset
	if err := SavePreset(preset); err != nil {
		return fmt.Errorf("failed to create preset: %w", err)
	}

	return nil
}

// SetActivePreset sets a preset as the active one in global config
func SetActivePreset(presetName string) error {
	if presetName != "" {
		// Validate preset exists
		exists, err := PresetExists(presetName)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("preset '%s' does not exist", presetName)
		}
	}

	// Update global config
	return config.SetActivePreset(presetName)
}

// GetActivePreset returns the active preset, if any
func GetActivePreset() (*config.Preset, error) {
	activePresetName, err := config.GetActivePreset()
	if err != nil {
		return nil, err
	}
	
	if activePresetName == "" {
		return nil, nil // No active preset
	}

	// Load and return the active preset
	return LoadPreset(activePresetName)
}