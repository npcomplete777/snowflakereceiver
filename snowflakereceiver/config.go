package snowflakereceiver

import (
    "fmt"
    "time"
    
    "go.opentelemetry.io/collector/component"
)

// Config represents the receiver configuration
type Config struct {
    User           string                `mapstructure:"user"`
    Password       string                `mapstructure:"password"`
    Account        string                `mapstructure:"account"`
    Warehouse      string                `mapstructure:"warehouse"`
    Database       string                `mapstructure:"database"`
    Schema         string                `mapstructure:"schema"`
    
    // Security and reliability settings
    QueryTimeout       string `mapstructure:"query_timeout"`        // Default: "30s"
    MaxRowsPerQuery    int    `mapstructure:"max_rows_per_query"`   // Default: 10000
    RateLimitQPS       int    `mapstructure:"rate_limit_qps"`       // Default: 10
    MaxRetries         int    `mapstructure:"max_retries"`          // Default: 3
    RetryInitialDelay  string `mapstructure:"retry_initial_delay"`  // Default: "1s"
    RetryMaxDelay      string `mapstructure:"retry_max_delay"`      // Default: "30s"
    
    Metrics        MetricsConfig         `mapstructure:"metrics"`
    EventTables    EventTablesConfig     `mapstructure:"event_tables"`
    Organization   OrganizationConfig    `mapstructure:"organization"`
    CustomQueries  CustomQueriesConfig   `mapstructure:"custom_queries"`
}

// Metrics configuration with per-metric intervals
type MetricsConfig struct {
    CurrentQueries        MetricCategoryConfig `mapstructure:"current_queries"`
    WarehouseLoad         MetricCategoryConfig `mapstructure:"warehouse_load"`
    QueryHistory          MetricCategoryConfig `mapstructure:"query_history"`
    CreditUsage           MetricCategoryConfig `mapstructure:"credit_usage"`
    StorageMetrics        MetricCategoryConfig `mapstructure:"storage_metrics"`
    LoginHistory          MetricCategoryConfig `mapstructure:"login_history"`
    DataPipeline          MetricCategoryConfig `mapstructure:"data_pipeline"`
    DatabaseStorage       MetricCategoryConfig `mapstructure:"database_storage"`
    TaskHistory           MetricCategoryConfig `mapstructure:"task_history"`
    ReplicationUsage      MetricCategoryConfig `mapstructure:"replication_usage"`
    AutoClusteringHistory MetricCategoryConfig `mapstructure:"auto_clustering_history"`
}

type MetricCategoryConfig struct {
    Enabled  bool   `mapstructure:"enabled"`
    Interval string `mapstructure:"interval"`
}

func (c *MetricCategoryConfig) GetInterval(defaultInterval time.Duration) time.Duration {
    if c.Interval == "" {
        return defaultInterval
    }
    duration, err := time.ParseDuration(c.Interval)
    if err != nil {
        return defaultInterval
    }
    return duration
}

// Event Tables configuration
type EventTablesConfig struct {
    Enabled        bool                 `mapstructure:"enabled"`
    TableName      string               `mapstructure:"table_name"`
    QueryLogs      MetricCategoryConfig `mapstructure:"query_logs"`
    TaskLogs       MetricCategoryConfig `mapstructure:"task_logs"`
    FunctionLogs   MetricCategoryConfig `mapstructure:"function_logs"`
    ProcedureLogs  MetricCategoryConfig `mapstructure:"procedure_logs"`
}

// Organization metrics configuration with column mapping
type OrganizationConfig struct {
    Enabled           bool                       `mapstructure:"enabled"`
    OrgCreditUsage    OrgCreditUsageConfig       `mapstructure:"org_credit_usage"`
    OrgStorageUsage   OrgStorageUsageConfig      `mapstructure:"org_storage_usage"`
    OrgDataTransfer   OrgDataTransferConfig      `mapstructure:"org_data_transfer"`
    OrgContractUsage  OrgContractUsageConfig     `mapstructure:"org_contract_usage"`
}

type OrgCreditUsageConfig struct {
    Enabled  bool                    `mapstructure:"enabled"`
    Interval string                  `mapstructure:"interval"`
    Columns  OrgCreditUsageColumns   `mapstructure:"columns"`
}

type OrgCreditUsageColumns struct {
    OrganizationName string `mapstructure:"organization_name"`
    AccountName      string `mapstructure:"account_name"`
    ServiceType      string `mapstructure:"service_type"`
    CreditsUsed      string `mapstructure:"credits_used"`
    UsageDate        string `mapstructure:"usage_date"`
}

func (c *OrgCreditUsageConfig) GetInterval(defaultInterval time.Duration) time.Duration {
    if c.Interval == "" {
        return defaultInterval
    }
    duration, err := time.ParseDuration(c.Interval)
    if err != nil {
        return defaultInterval
    }
    return duration
}

// Default column names for org credit usage
func (c *OrgCreditUsageColumns) GetOrganizationName() string {
    if c.OrganizationName != "" {
        return c.OrganizationName
    }
    return "ORGANIZATION_NAME"
}

func (c *OrgCreditUsageColumns) GetAccountName() string {
    if c.AccountName != "" {
        return c.AccountName
    }
    return "ACCOUNT_NAME"
}

func (c *OrgCreditUsageColumns) GetServiceType() string {
    if c.ServiceType != "" {
        return c.ServiceType
    }
    return "SERVICE_TYPE"
}

func (c *OrgCreditUsageColumns) GetCreditsUsed() string {
    if c.CreditsUsed != "" {
        return c.CreditsUsed
    }
    return "CREDITS"
}

func (c *OrgCreditUsageColumns) GetUsageDate() string {
    if c.UsageDate != "" {
        return c.UsageDate
    }
    return "USAGE_DATE"
}

type OrgStorageUsageConfig struct {
    Enabled  bool                      `mapstructure:"enabled"`
    Interval string                    `mapstructure:"interval"`
    Columns  OrgStorageUsageColumns    `mapstructure:"columns"`
}

type OrgStorageUsageColumns struct {
    OrganizationName string `mapstructure:"organization_name"`
    AccountName      string `mapstructure:"account_name"`
    StorageBytes     string `mapstructure:"storage_bytes"`
    StageBytes       string `mapstructure:"stage_bytes"`
    FailsafeBytes    string `mapstructure:"failsafe_bytes"`
    UsageDate        string `mapstructure:"usage_date"`
}

func (c *OrgStorageUsageConfig) GetInterval(defaultInterval time.Duration) time.Duration {
    if c.Interval == "" {
        return defaultInterval
    }
    duration, err := time.ParseDuration(c.Interval)
    if err != nil {
        return defaultInterval
    }
    return duration
}

func (c *OrgStorageUsageColumns) GetOrganizationName() string {
    if c.OrganizationName != "" {
        return c.OrganizationName
    }
    return "ORGANIZATION_NAME"
}

func (c *OrgStorageUsageColumns) GetAccountName() string {
    if c.AccountName != "" {
        return c.AccountName
    }
    return "ACCOUNT_NAME"
}

func (c *OrgStorageUsageColumns) GetStorageBytes() string {
    if c.StorageBytes != "" {
        return c.StorageBytes
    }
    return "AVERAGE_STORAGE_BYTES"
}

func (c *OrgStorageUsageColumns) GetStageBytes() string {
    if c.StageBytes != "" {
        return c.StageBytes
    }
    return "AVERAGE_STAGE_BYTES"
}

func (c *OrgStorageUsageColumns) GetFailsafeBytes() string {
    if c.FailsafeBytes != "" {
        return c.FailsafeBytes
    }
    return "AVERAGE_FAILSAFE_BYTES"
}

func (c *OrgStorageUsageColumns) GetUsageDate() string {
    if c.UsageDate != "" {
        return c.UsageDate
    }
    return "USAGE_DATE"
}

type OrgDataTransferConfig struct {
    Enabled  bool                      `mapstructure:"enabled"`
    Interval string                    `mapstructure:"interval"`
    Columns  OrgDataTransferColumns    `mapstructure:"columns"`
}

type OrgDataTransferColumns struct {
    OrganizationName  string `mapstructure:"organization_name"`
    SourceAccountName string `mapstructure:"source_account_name"`
    TargetAccountName string `mapstructure:"target_account_name"`
    SourceRegion      string `mapstructure:"source_region"`
    TargetRegion      string `mapstructure:"target_region"`
    BytesTransferred  string `mapstructure:"bytes_transferred"`
    TransferDate      string `mapstructure:"transfer_date"`
}

func (c *OrgDataTransferConfig) GetInterval(defaultInterval time.Duration) time.Duration {
    if c.Interval == "" {
        return defaultInterval
    }
    duration, err := time.ParseDuration(c.Interval)
    if err != nil {
        return defaultInterval
    }
    return duration
}

func (c *OrgDataTransferColumns) GetOrganizationName() string {
    if c.OrganizationName != "" {
        return c.OrganizationName
    }
    return "ORGANIZATION_NAME"
}

func (c *OrgDataTransferColumns) GetSourceAccountName() string {
    if c.SourceAccountName != "" {
        return c.SourceAccountName
    }
    return "SOURCE_ACCOUNT_NAME"
}

func (c *OrgDataTransferColumns) GetTargetAccountName() string {
    if c.TargetAccountName != "" {
        return c.TargetAccountName
    }
    return "TARGET_ACCOUNT_NAME"
}

func (c *OrgDataTransferColumns) GetSourceRegion() string {
    if c.SourceRegion != "" {
        return c.SourceRegion
    }
    return "SOURCE_REGION"
}

func (c *OrgDataTransferColumns) GetTargetRegion() string {
    if c.TargetRegion != "" {
        return c.TargetRegion
    }
    return "TARGET_REGION"
}

func (c *OrgDataTransferColumns) GetBytesTransferred() string {
    if c.BytesTransferred != "" {
        return c.BytesTransferred
    }
    return "BYTES_TRANSFERRED"
}

func (c *OrgDataTransferColumns) GetTransferDate() string {
    if c.TransferDate != "" {
        return c.TransferDate
    }
    return "TRANSFER_DATE"
}

type OrgContractUsageConfig struct {
    Enabled  bool                       `mapstructure:"enabled"`
    Interval string                     `mapstructure:"interval"`
    Columns  OrgContractUsageColumns    `mapstructure:"columns"`
}

type OrgContractUsageColumns struct {
    OrganizationName string `mapstructure:"organization_name"`
    ContractNumber   string `mapstructure:"contract_number"`
    CreditsUsed      string `mapstructure:"credits_used"`
    CreditsBilled    string `mapstructure:"credits_billed"`
    UsageDate        string `mapstructure:"usage_date"`
}

func (c *OrgContractUsageConfig) GetInterval(defaultInterval time.Duration) time.Duration {
    if c.Interval == "" {
        return defaultInterval
    }
    duration, err := time.ParseDuration(c.Interval)
    if err != nil {
        return defaultInterval
    }
    return duration
}

func (c *OrgContractUsageColumns) GetOrganizationName() string {
    if c.OrganizationName != "" {
        return c.OrganizationName
    }
    return "ORGANIZATION_NAME"
}

func (c *OrgContractUsageColumns) GetContractNumber() string {
    if c.ContractNumber != "" {
        return c.ContractNumber
    }
    return "CONTRACT_NUMBER"
}

func (c *OrgContractUsageColumns) GetCreditsUsed() string {
    if c.CreditsUsed != "" {
        return c.CreditsUsed
    }
    return "CREDITS_USED"
}

func (c *OrgContractUsageColumns) GetCreditsBilled() string {
    if c.CreditsBilled != "" {
        return c.CreditsBilled
    }
    return "CREDITS_BILLED"
}

func (c *OrgContractUsageColumns) GetUsageDate() string {
    if c.UsageDate != "" {
        return c.UsageDate
    }
    return "USAGE_DATE"
}

// Custom queries configuration
type CustomQueriesConfig struct {
    Enabled bool          `mapstructure:"enabled"`
    Queries []CustomQuery `mapstructure:"queries"`
}

type CustomQuery struct {
    Name         string   `mapstructure:"name"`
    Interval     string   `mapstructure:"interval"`
    MetricType   string   `mapstructure:"metric_type"`
    ValueColumn  string   `mapstructure:"value_column"`
    LabelColumns []string `mapstructure:"label_columns"`
    SQL          string   `mapstructure:"sql"`
}

func (q *CustomQuery) GetInterval(defaultInterval time.Duration) time.Duration {
    if q.Interval == "" {
        return defaultInterval
    }
    duration, err := time.ParseDuration(q.Interval)
    if err != nil {
        return defaultInterval
    }
    return duration
}

// GetQueryTimeout returns the parsed query timeout duration
func (cfg *Config) GetQueryTimeout() time.Duration {
    if cfg.QueryTimeout == "" {
        return 30 * time.Second
    }
    duration, err := time.ParseDuration(cfg.QueryTimeout)
    if err != nil {
        return 30 * time.Second
    }
    return duration
}

// GetMaxRowsPerQuery returns the max rows per query
func (cfg *Config) GetMaxRowsPerQuery() int {
    if cfg.MaxRowsPerQuery <= 0 {
        return 10000
    }
    return cfg.MaxRowsPerQuery
}

// GetRateLimitQPS returns the rate limit in queries per second
func (cfg *Config) GetRateLimitQPS() int {
    if cfg.RateLimitQPS <= 0 {
        return 10
    }
    return cfg.RateLimitQPS
}

// GetMaxRetries returns the maximum number of retries
func (cfg *Config) GetMaxRetries() int {
    if cfg.MaxRetries <= 0 {
        return 3
    }
    return cfg.MaxRetries
}

// GetRetryInitialDelay returns the initial retry delay
func (cfg *Config) GetRetryInitialDelay() time.Duration {
    if cfg.RetryInitialDelay == "" {
        return 1 * time.Second
    }
    duration, err := time.ParseDuration(cfg.RetryInitialDelay)
    if err != nil {
        return 1 * time.Second
    }
    return duration
}

// GetRetryMaxDelay returns the maximum retry delay
func (cfg *Config) GetRetryMaxDelay() time.Duration {
    if cfg.RetryMaxDelay == "" {
        return 30 * time.Second
    }
    duration, err := time.ParseDuration(cfg.RetryMaxDelay)
    if err != nil {
        return 30 * time.Second
    }
    return duration
}

// GetBaseInterval returns the minimum interval across all enabled metrics
func (cfg *Config) GetBaseInterval() time.Duration {
    minInterval := 30 * time.Second
    
    allMetrics := []MetricCategoryConfig{
        cfg.Metrics.CurrentQueries,
        cfg.Metrics.WarehouseLoad,
        cfg.Metrics.QueryHistory,
        cfg.Metrics.CreditUsage,
        cfg.Metrics.StorageMetrics,
        cfg.Metrics.LoginHistory,
        cfg.Metrics.DataPipeline,
        cfg.Metrics.DatabaseStorage,
        cfg.Metrics.TaskHistory,
        cfg.Metrics.ReplicationUsage,
        cfg.Metrics.AutoClusteringHistory,
    }
    
    if cfg.EventTables.Enabled {
        allMetrics = append(allMetrics,
            cfg.EventTables.QueryLogs,
            cfg.EventTables.TaskLogs,
            cfg.EventTables.FunctionLogs,
            cfg.EventTables.ProcedureLogs,
        )
    }
    
    if cfg.Organization.Enabled {
        allMetrics = append(allMetrics,
            cfg.Organization.OrgCreditUsage.MetricCategoryConfig(),
            cfg.Organization.OrgStorageUsage.MetricCategoryConfig(),
            cfg.Organization.OrgDataTransfer.MetricCategoryConfig(),
            cfg.Organization.OrgContractUsage.MetricCategoryConfig(),
        )
    }
    
    for _, metric := range allMetrics {
        if metric.Enabled && metric.Interval != "" {
            interval := metric.GetInterval(minInterval)
            if interval < minInterval {
                minInterval = interval
            }
        }
    }
    
    return minInterval
}

// Helper methods to convert org configs to MetricCategoryConfig
func (c *OrgCreditUsageConfig) MetricCategoryConfig() MetricCategoryConfig {
    return MetricCategoryConfig{Enabled: c.Enabled, Interval: c.Interval}
}

func (c *OrgStorageUsageConfig) MetricCategoryConfig() MetricCategoryConfig {
    return MetricCategoryConfig{Enabled: c.Enabled, Interval: c.Interval}
}

func (c *OrgDataTransferConfig) MetricCategoryConfig() MetricCategoryConfig {
    return MetricCategoryConfig{Enabled: c.Enabled, Interval: c.Interval}
}

func (c *OrgContractUsageConfig) MetricCategoryConfig() MetricCategoryConfig {
    return MetricCategoryConfig{Enabled: c.Enabled, Interval: c.Interval}
}

// createDefaultConfig creates the default configuration
func createDefaultConfig() component.Config {
    return &Config{
        Database: "SNOWFLAKE",
        Schema:   "ACCOUNT_USAGE",
        
        // Security and reliability defaults
        QueryTimeout:      "30s",
        MaxRowsPerQuery:   10000,
        RateLimitQPS:      10,
        MaxRetries:        3,
        RetryInitialDelay: "1s",
        RetryMaxDelay:     "30s",
        
        Metrics: MetricsConfig{
            CurrentQueries:        MetricCategoryConfig{Enabled: true, Interval: "1m"},
            WarehouseLoad:         MetricCategoryConfig{Enabled: true, Interval: "1m"},
            QueryHistory:          MetricCategoryConfig{Enabled: true, Interval: "5m"},
            CreditUsage:           MetricCategoryConfig{Enabled: true, Interval: "5m"},
            StorageMetrics:        MetricCategoryConfig{Enabled: true, Interval: "30m"},
            LoginHistory:          MetricCategoryConfig{Enabled: true, Interval: "10m"},
            DataPipeline:          MetricCategoryConfig{Enabled: true, Interval: "10m"},
            DatabaseStorage:       MetricCategoryConfig{Enabled: true, Interval: "30m"},
            TaskHistory:           MetricCategoryConfig{Enabled: true, Interval: "10m"},
            ReplicationUsage:      MetricCategoryConfig{Enabled: true, Interval: "15m"},
            AutoClusteringHistory: MetricCategoryConfig{Enabled: true, Interval: "15m"},
        },
        EventTables: EventTablesConfig{
            Enabled:       false,
            QueryLogs:     MetricCategoryConfig{Enabled: true, Interval: "30s"},
            TaskLogs:      MetricCategoryConfig{Enabled: true, Interval: "30s"},
            FunctionLogs:  MetricCategoryConfig{Enabled: true, Interval: "30s"},
            ProcedureLogs: MetricCategoryConfig{Enabled: true, Interval: "30s"},
        },
        Organization: OrganizationConfig{
            Enabled: false,
            OrgCreditUsage: OrgCreditUsageConfig{
                Enabled:  true,
                Interval: "1h",
                Columns:  OrgCreditUsageColumns{},
            },
            OrgStorageUsage: OrgStorageUsageConfig{
                Enabled:  true,
                Interval: "1h",
                Columns:  OrgStorageUsageColumns{},
            },
            OrgDataTransfer: OrgDataTransferConfig{
                Enabled:  true,
                Interval: "1h",
                Columns:  OrgDataTransferColumns{},
            },
            OrgContractUsage: OrgContractUsageConfig{
                Enabled:  true,
                Interval: "12h",
                Columns:  OrgContractUsageColumns{},
            },
        },
        CustomQueries: CustomQueriesConfig{
            Enabled: false,
            Queries: []CustomQuery{},
        },
    }
}

// Validate validates the configuration
func (cfg *Config) Validate() error {
    if cfg.User == "" {
        return fmt.Errorf("user is required")
    }
    if cfg.Password == "" {
        return fmt.Errorf("password is required")
    }
    if cfg.Account == "" {
        return fmt.Errorf("account is required")
    }
    if cfg.Warehouse == "" {
        return fmt.Errorf("warehouse is required")
    }
    
    // Validate timeout values
    if cfg.QueryTimeout != "" {
        if _, err := time.ParseDuration(cfg.QueryTimeout); err != nil {
            return fmt.Errorf("invalid query_timeout: %w", err)
        }
    }
    
    if cfg.RetryInitialDelay != "" {
        if _, err := time.ParseDuration(cfg.RetryInitialDelay); err != nil {
            return fmt.Errorf("invalid retry_initial_delay: %w", err)
        }
    }
    
    if cfg.RetryMaxDelay != "" {
        if _, err := time.ParseDuration(cfg.RetryMaxDelay); err != nil {
            return fmt.Errorf("invalid retry_max_delay: %w", err)
        }
    }
    
    // Validate numeric ranges
    if cfg.MaxRowsPerQuery < 0 {
        return fmt.Errorf("max_rows_per_query must be positive")
    }
    
    if cfg.RateLimitQPS < 0 {
        return fmt.Errorf("rate_limit_qps must be positive")
    }
    
    if cfg.MaxRetries < 0 {
        return fmt.Errorf("max_retries must be non-negative")
    }
    
    return nil
}
