// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package snowflakereceiver

import (
    "fmt"
    "time"
)

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
    
    // Cardinality protection limits
    MaxUsersCardinality     int `mapstructure:"max_users_cardinality"`     // Default: 500
    MaxSchemasCardinality   int `mapstructure:"max_schemas_cardinality"`   // Default: 200
    MaxDatabasesCardinality int `mapstructure:"max_databases_cardinality"` // Default: 100
    MaxRolesCardinality     int `mapstructure:"max_roles_cardinality"`     // Default: 200
    
    Metrics        MetricsConfig         `mapstructure:"metrics"`
    EventTables    EventTablesConfig     `mapstructure:"event_tables"`
    Organization   OrganizationConfig    `mapstructure:"organization"`
    CustomQueries  CustomQueriesConfig   `mapstructure:"custom_queries"`
}

// SanitizedDSN returns a DSN string with the password redacted for safe logging
func (cfg *Config) SanitizedDSN() string {
    return fmt.Sprintf("%s:***@%s/%s/%s?warehouse=%s",
        cfg.User,
        cfg.Account,
        cfg.Database,
        cfg.Schema,
        cfg.Warehouse,
    )
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
    
    // Column name configuration for organization tables
    OrganizationNameColumn string `mapstructure:"organization_name_column"` // Default: "ORGANIZATION_NAME"
    AccountNameColumn      string `mapstructure:"account_name_column"`      // Default: "ACCOUNT_NAME"
    
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

func (c OrgCreditUsageColumns) GetOrganizationName() string {
    if c.OrganizationName != "" {
        return c.OrganizationName
    }
    return "ORGANIZATION_NAME"
}

func (c OrgCreditUsageColumns) GetAccountName() string {
    if c.AccountName != "" {
        return c.AccountName
    }
    return "ACCOUNT_NAME"
}

func (c OrgCreditUsageColumns) GetServiceType() string {
    if c.ServiceType != "" {
        return c.ServiceType
    }
    return "SERVICE_TYPE"
}

func (c OrgCreditUsageColumns) GetCreditsUsed() string {
    if c.CreditsUsed != "" {
        return c.CreditsUsed
    }
    return "CREDITS"
}

func (c OrgCreditUsageColumns) GetUsageDate() string {
    if c.UsageDate != "" {
        return c.UsageDate
    }
    return "USAGE_DATE"
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

type OrgStorageUsageConfig struct {
    Enabled  bool                     `mapstructure:"enabled"`
    Interval string                   `mapstructure:"interval"`
    Columns  OrgStorageUsageColumns   `mapstructure:"columns"`
}

type OrgStorageUsageColumns struct {
    OrganizationName string `mapstructure:"organization_name"`
    AccountName      string `mapstructure:"account_name"`
    StorageBytes     string `mapstructure:"storage_bytes"`
    StageBytes       string `mapstructure:"stage_bytes"`
    FailsafeBytes    string `mapstructure:"failsafe_bytes"`
    UsageDate        string `mapstructure:"usage_date"`
}

func (c OrgStorageUsageColumns) GetOrganizationName() string {
    if c.OrganizationName != "" {
        return c.OrganizationName
    }
    return "ORGANIZATION_NAME"
}

func (c OrgStorageUsageColumns) GetAccountName() string {
    if c.AccountName != "" {
        return c.AccountName
    }
    return "ACCOUNT_NAME"
}

func (c OrgStorageUsageColumns) GetStorageBytes() string {
    if c.StorageBytes != "" {
        return c.StorageBytes
    }
    return "AVERAGE_STORAGE_BYTES"
}

func (c OrgStorageUsageColumns) GetStageBytes() string {
    if c.StageBytes != "" {
        return c.StageBytes
    }
    return "AVERAGE_STAGE_BYTES"
}

func (c OrgStorageUsageColumns) GetFailsafeBytes() string {
    if c.FailsafeBytes != "" {
        return c.FailsafeBytes
    }
    return "AVERAGE_FAILSAFE_BYTES"
}

func (c OrgStorageUsageColumns) GetUsageDate() string {
    if c.UsageDate != "" {
        return c.UsageDate
    }
    return "USAGE_DATE"
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

type OrgDataTransferConfig struct {
    Enabled  bool                     `mapstructure:"enabled"`
    Interval string                   `mapstructure:"interval"`
    Columns  OrgDataTransferColumns   `mapstructure:"columns"`
}

type OrgDataTransferColumns struct {
    OrganizationName string `mapstructure:"organization_name"`
    SourceAccountName string `mapstructure:"source_account_name"`
    TargetAccountName string `mapstructure:"target_account_name"`
    SourceRegion     string `mapstructure:"source_region"`
    TargetRegion     string `mapstructure:"target_region"`
    BytesTransferred string `mapstructure:"bytes_transferred"`
    TransferDate     string `mapstructure:"transfer_date"`
}

func (c OrgDataTransferColumns) GetOrganizationName() string {
    if c.OrganizationName != "" {
        return c.OrganizationName
    }
    return "ORGANIZATION_NAME"
}

func (c OrgDataTransferColumns) GetSourceAccountName() string {
    if c.SourceAccountName != "" {
        return c.SourceAccountName
    }
    return "SOURCE_ACCOUNT_NAME"
}

func (c OrgDataTransferColumns) GetTargetAccountName() string {
    if c.TargetAccountName != "" {
        return c.TargetAccountName
    }
    return "TARGET_ACCOUNT_NAME"
}

func (c OrgDataTransferColumns) GetSourceRegion() string {
    if c.SourceRegion != "" {
        return c.SourceRegion
    }
    return "SOURCE_REGION"
}

func (c OrgDataTransferColumns) GetTargetRegion() string {
    if c.TargetRegion != "" {
        return c.TargetRegion
    }
    return "TARGET_REGION"
}

func (c OrgDataTransferColumns) GetBytesTransferred() string {
    if c.BytesTransferred != "" {
        return c.BytesTransferred
    }
    return "BYTES_TRANSFERRED"
}

func (c OrgDataTransferColumns) GetTransferDate() string {
    if c.TransferDate != "" {
        return c.TransferDate
    }
    return "TRANSFER_DATE"
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

type OrgContractUsageConfig struct {
    Enabled  bool                      `mapstructure:"enabled"`
    Interval string                    `mapstructure:"interval"`
    Columns  OrgContractUsageColumns   `mapstructure:"columns"`
}

type OrgContractUsageColumns struct {
    OrganizationName string `mapstructure:"organization_name"`
    ContractNumber   string `mapstructure:"contract_number"`
    CreditsUsed      string `mapstructure:"credits_used"`
    CreditsBilled    string `mapstructure:"credits_billed"`
    UsageDate        string `mapstructure:"usage_date"`
}

func (c OrgContractUsageColumns) GetOrganizationName() string {
    if c.OrganizationName != "" {
        return c.OrganizationName
    }
    return "ORGANIZATION_NAME"
}

func (c OrgContractUsageColumns) GetContractNumber() string {
    if c.ContractNumber != "" {
        return c.ContractNumber
    }
    return "CONTRACT_NUMBER"
}

func (c OrgContractUsageColumns) GetCreditsUsed() string {
    if c.CreditsUsed != "" {
        return c.CreditsUsed
    }
    return "CREDITS_USED"
}

func (c OrgContractUsageColumns) GetCreditsBilled() string {
    if c.CreditsBilled != "" {
        return c.CreditsBilled
    }
    return "CREDITS_BILLED"
}

func (c OrgContractUsageColumns) GetUsageDate() string {
    if c.UsageDate != "" {
        return c.UsageDate
    }
    return "USAGE_DATE"
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

// Custom Queries configuration
type CustomQueriesConfig struct {
    Enabled bool          `mapstructure:"enabled"`
    Queries []CustomQuery `mapstructure:"queries"`
}

type CustomQuery struct {
    Name         string   `mapstructure:"name"`
    Interval     string   `mapstructure:"interval"`
    MetricType   string   `mapstructure:"metric_type"`    // gauge, counter, histogram
    ValueColumn  string   `mapstructure:"value_column"`   // Column containing metric value
    LabelColumns []string `mapstructure:"label_columns"`  // Columns to use as labels
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

// Validate ensures the configuration is valid
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
    return nil
}

// Helper methods with defaults

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

func (cfg *Config) GetMaxRowsPerQuery() int {
    if cfg.MaxRowsPerQuery <= 0 {
        return 10000
    }
    return cfg.MaxRowsPerQuery
}

func (cfg *Config) GetRateLimitQPS() int {
    if cfg.RateLimitQPS <= 0 {
        return 10
    }
    return cfg.RateLimitQPS
}

func (cfg *Config) GetMaxRetries() int {
    if cfg.MaxRetries < 0 {
        return 3
    }
    return cfg.MaxRetries
}

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

// GetBaseInterval returns the shortest interval across all enabled metrics
func (cfg *Config) GetBaseInterval() time.Duration {
    intervals := []time.Duration{}
    
    if cfg.Metrics.CurrentQueries.Enabled {
        intervals = append(intervals, cfg.Metrics.CurrentQueries.GetInterval(1*time.Minute))
    }
    if cfg.Metrics.WarehouseLoad.Enabled {
        intervals = append(intervals, cfg.Metrics.WarehouseLoad.GetInterval(1*time.Minute))
    }
    if cfg.Metrics.QueryHistory.Enabled {
        intervals = append(intervals, cfg.Metrics.QueryHistory.GetInterval(5*time.Minute))
    }
    if cfg.Metrics.CreditUsage.Enabled {
        intervals = append(intervals, cfg.Metrics.CreditUsage.GetInterval(5*time.Minute))
    }
    if cfg.Metrics.StorageMetrics.Enabled {
        intervals = append(intervals, cfg.Metrics.StorageMetrics.GetInterval(30*time.Minute))
    }
    if cfg.Metrics.LoginHistory.Enabled {
        intervals = append(intervals, cfg.Metrics.LoginHistory.GetInterval(10*time.Minute))
    }
    if cfg.Metrics.DataPipeline.Enabled {
        intervals = append(intervals, cfg.Metrics.DataPipeline.GetInterval(10*time.Minute))
    }
    if cfg.Metrics.DatabaseStorage.Enabled {
        intervals = append(intervals, cfg.Metrics.DatabaseStorage.GetInterval(30*time.Minute))
    }
    if cfg.Metrics.TaskHistory.Enabled {
        intervals = append(intervals, cfg.Metrics.TaskHistory.GetInterval(10*time.Minute))
    }
    if cfg.Metrics.ReplicationUsage.Enabled {
        intervals = append(intervals, cfg.Metrics.ReplicationUsage.GetInterval(15*time.Minute))
    }
    if cfg.Metrics.AutoClusteringHistory.Enabled {
        intervals = append(intervals, cfg.Metrics.AutoClusteringHistory.GetInterval(15*time.Minute))
    }
    
    // Event tables (real-time!)
    if cfg.EventTables.Enabled {
        if cfg.EventTables.QueryLogs.Enabled {
            intervals = append(intervals, cfg.EventTables.QueryLogs.GetInterval(30*time.Second))
        }
        if cfg.EventTables.TaskLogs.Enabled {
            intervals = append(intervals, cfg.EventTables.TaskLogs.GetInterval(30*time.Second))
        }
    }
    
    // Find minimum interval
    if len(intervals) == 0 {
        return 1 * time.Minute // Default if nothing enabled
    }
    
    minInterval := intervals[0]
    for _, interval := range intervals[1:] {
        if interval < minInterval {
            minInterval = interval
        }
    }
    
    return minInterval
}
