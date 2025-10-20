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
    
    // Collection intervals per tier
    CollectionIntervals CollectionIntervals `mapstructure:"collection_intervals"`
    
    // Metric category toggles
    Metrics MetricsConfig `mapstructure:"metrics"`
}

type CollectionIntervals struct {
    Historical string `mapstructure:"historical"`  // ACCOUNT_USAGE queries
}

type MetricsConfig struct {
    // Historical tier (ACCOUNT_USAGE)
    QueryHistory      bool `mapstructure:"query_history"`
    CreditUsage       bool `mapstructure:"credit_usage"`
    StorageMetrics    bool `mapstructure:"storage_metrics"`
    LoginHistory      bool `mapstructure:"login_history"`
    DataPipeline      bool `mapstructure:"data_pipeline"`
    DatabaseStorage   bool `mapstructure:"database_storage"`
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
    
    // Validate collection intervals
    if cfg.CollectionIntervals.Historical != "" {
        if _, err := time.ParseDuration(cfg.CollectionIntervals.Historical); err != nil {
            return fmt.Errorf("invalid historical collection_interval: %w", err)
        }
    }
    
    return nil
}

func (cfg *Config) GetHistoricalInterval() time.Duration {
    if cfg.CollectionIntervals.Historical != "" {
        d, _ := time.ParseDuration(cfg.CollectionIntervals.Historical)
        return d
    }
    return 5 * time.Minute // default
}

func createDefaultConfig() component.Config {
    return &Config{
        ClientConfig: confighttp.NewDefaultClientConfig(),
        Schema:      "ACCOUNT_USAGE",
        Role:        "ACCOUNTADMIN",
        CollectionIntervals: CollectionIntervals{
            Historical: "5m",
        },
        Metrics: MetricsConfig{
            QueryHistory:    true,
            CreditUsage:     true,
            StorageMetrics:  true,
            LoginHistory:    true,
            DataPipeline:    true,
            DatabaseStorage: true,
        },
    }
}
