package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds the application configuration
type Config struct {
	Source      KeyDBConfig     `yaml:"source"`
	Destination KeyDBConfig     `yaml:"destination"`
	Migration   MigrationConfig `yaml:"migration"`
	Logging     LoggingConfig   `yaml:"logging"`
}

// KeyDBConfig holds KeyDB connection configuration
type KeyDBConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Password string `yaml:"password"`
	Database int    `yaml:"database"`
	PoolSize int    `yaml:"pool_size"`
}

// MigrationConfig holds migration-specific configuration
type MigrationConfig struct {
	BatchSize  int           `yaml:"batch_size"`
	Workers    int           `yaml:"workers"`
	Timeout    time.Duration `yaml:"timeout"`
	RetryCount int           `yaml:"retry_count"`
	RetryDelay time.Duration `yaml:"retry_delay"`
}

// LoggingConfig holds logging configuration
type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// Load loads configuration from environment variables
func Load() (*Config, error) {
	config := &Config{
		Source: KeyDBConfig{
			Host:     getEnvOrDefault("SOURCE_KEYDB_HOST", "localhost"),
			Port:     getEnvIntOrDefault("SOURCE_KEYDB_PORT", 6379),
			Password: os.Getenv("SOURCE_KEYDB_PASSWORD"),
			Database: getEnvIntOrDefault("SOURCE_KEYDB_DB", 0),
			PoolSize: getEnvIntOrDefault("SOURCE_KEYDB_POOL_SIZE", 20),
		},
		Destination: KeyDBConfig{
			Host:     getEnvOrDefault("DEST_KEYDB_HOST", "localhost"),
			Port:     getEnvIntOrDefault("DEST_KEYDB_PORT", 6379),
			Password: os.Getenv("DEST_KEYDB_PASSWORD"),
			Database: getEnvIntOrDefault("DEST_KEYDB_DB", 0),
			PoolSize: getEnvIntOrDefault("DEST_KEYDB_POOL_SIZE", 20),
		},
		Migration: MigrationConfig{
			BatchSize:  getEnvIntOrDefault("MIGRATION_BATCH_SIZE", 1000),
			Workers:    getEnvIntOrDefault("MIGRATION_WORKERS", 10),
			Timeout:    getDurationOrDefault("MIGRATION_TIMEOUT", 30*time.Minute),
			RetryCount: getEnvIntOrDefault("MIGRATION_RETRY_COUNT", 3),
			RetryDelay: getDurationOrDefault("MIGRATION_RETRY_DELAY", 1*time.Second),
		},
		Logging: LoggingConfig{
			Level:  getEnvOrDefault("LOG_LEVEL", "info"),
			Format: getEnvOrDefault("LOG_FORMAT", "json"),
		},
	}

	return config, config.validate()
}

// validate validates the configuration
func (c *Config) validate() error {
	if c.Source.Host == "" {
		return fmt.Errorf("source host cannot be empty")
	}
	if c.Destination.Host == "" {
		return fmt.Errorf("destination host cannot be empty")
	}
	if c.Migration.Workers <= 0 {
		return fmt.Errorf("number of workers must be positive")
	}
	if c.Migration.BatchSize <= 0 {
		return fmt.Errorf("batch size must be positive")
	}
	return nil
}

// Utility functions
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvIntOrDefault(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getDurationOrDefault(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}
