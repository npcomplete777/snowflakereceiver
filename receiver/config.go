package snowflakereceiver

import (
    "errors"
    "fmt"
    "time"
    
    "go.opentelemetry.io/collector/component"
    "go.opentelemetry.io/collector/config/confighttp"
)

type Config struct {
    confighttp.ClientConfig `mapstructure:",squash"`
    
    // Snowflake connection details
    Account   string `mapstructure:"account"`
    User      string `mapstructure:"user"`
    Password  string `mapstructure:"password"`
    Warehouse string `mapstructure:"warehouse"`
    Database  string `mapstructure:"database"`
    Schema    string `mapstructure:"schema"`
    Role      string `mapstructure:"role"`
    
    // Collection interval
    CollectionInterval string `mapstructure:"collection_interval"`
    
    // Metric category toggles
    Metrics MetricsConfig `mapstructure:"metrics"`
}

type MetricsConfig struct {
    // INFORMATION_SCHEMA metrics (REAL-TIME - no latency!)
    CurrentQueries    bool `mapstructure:"current_queries"`      // Current query activity (last 5min)
    WarehouseLoad     bool `mapstructure:"warehouse_load"`       // Warehouse queue depth & load
    
    // ACCOUNT_USAGE metrics (45min-3hr latency)
    QueryHistory         bool `mapstructure:"query_history"`          // Historical query performance
    CreditUsage          bool `mapstructure:"credit_usage"`           // Warehouse credit consumption
    StorageMetrics       bool `mapstructure:"storage_metrics"`        // Account-level storage
    LoginHistory         bool `mapstructure:"login_history"`          // Authentication attempts
    DataPipeline         bool `mapstructure:"data_pipeline"`          // Snowpipe usage
    DatabaseStorage      bool `mapstructure:"database_storage"`       // Per-database storage
    TaskHistory          bool `mapstructure:"task_history"`           // Task execution history
    ReplicationUsage     bool `mapstructure:"replication_usage"`      // Database replication
    AutoClusteringHistory bool `mapstructure:"auto_clustering_history"` // Auto-clustering activity
}

func (cfg *Config) Validate() error {
    if cfg.Account == "" {
        return errors.New("account is required")
    }
    if cfg.User == "" {
        return errors.New("user is required")
    }
    if cfg.Password == "" {
        return errors.New("password is required")
    }
    if cfg.Warehouse == "" {
        return errors.New("warehouse is required")
    }
    if cfg.Database == "" {
        return errors.New("database is required")
    }
    
    // Validate collection interval
    if cfg.CollectionInterval != "" {
        if _, err := time.ParseDuration(cfg.CollectionInterval); err != nil {
            return fmt.Errorf("invalid collection_interval: %w", err)
        }
    }
    
    return nil
}

func (cfg *Config) GetCollectionInterval() time.Duration {
    if cfg.CollectionInterval != "" {
        d, _ := time.ParseDuration(cfg.CollectionInterval)
        return d
    }
    return 2 * time.Minute // default
}

func createDefaultConfig() component.Config {
    return &Config{
        ClientConfig:       confighttp.NewDefaultClientConfig(),
        CollectionInterval: "2m",
        Schema:            "ACCOUNT_USAGE",
        Role:              "ACCOUNTADMIN",
        Metrics: MetricsConfig{
            // Real-time metrics (INFORMATION_SCHEMA)
            CurrentQueries: true,
            WarehouseLoad:  true,
            // Historical metrics (ACCOUNT_USAGE)
            QueryHistory:          true,
            CreditUsage:           true,
            StorageMetrics:        true,
            LoginHistory:          true,
            DataPipeline:          true,
            DatabaseStorage:       true,
            TaskHistory:           true,
            ReplicationUsage:      true,
            AutoClusteringHistory: true,
        },
    }
}
