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
    
    // DEPRECATED: Single interval (kept for backward compatibility)
    CollectionInterval string `mapstructure:"collection_interval"`
    
    // Metric category toggles with per-metric intervals
    Metrics       MetricsConfig       `mapstructure:"metrics"`
    EventTables   EventTablesConfig   `mapstructure:"event_tables"`
    Organization  OrganizationConfig  `mapstructure:"organization"`
    CustomQueries CustomQueriesConfig `mapstructure:"custom_queries"`
}

type MetricsConfig struct {
    // INFORMATION_SCHEMA metrics (REAL-TIME - no latency!)
    CurrentQueries    MetricConfig `mapstructure:"current_queries"`
    WarehouseLoad     MetricConfig `mapstructure:"warehouse_load"`
    
    // ACCOUNT_USAGE metrics (45min-3hr latency)
    QueryHistory          MetricConfig `mapstructure:"query_history"`
    CreditUsage           MetricConfig `mapstructure:"credit_usage"`
    StorageMetrics        MetricConfig `mapstructure:"storage_metrics"`
    LoginHistory          MetricConfig `mapstructure:"login_history"`
    DataPipeline          MetricConfig `mapstructure:"data_pipeline"`
    DatabaseStorage       MetricConfig `mapstructure:"database_storage"`
    TaskHistory           MetricConfig `mapstructure:"task_history"`
    ReplicationUsage      MetricConfig `mapstructure:"replication_usage"`
    AutoClusteringHistory MetricConfig `mapstructure:"auto_clustering_history"`
}

// Event Tables - NEAR REAL-TIME (seconds latency!)
type EventTablesConfig struct {
    Enabled bool `mapstructure:"enabled"`
    
    // Event table name (user must create this)
    TableName string `mapstructure:"table_name"`
    
    // Per-event type intervals
    QueryLogs      MetricConfig `mapstructure:"query_logs"`
    TaskLogs       MetricConfig `mapstructure:"task_logs"`
    FunctionLogs   MetricConfig `mapstructure:"function_logs"`
    ProcedureLogs  MetricConfig `mapstructure:"procedure_logs"`
}

// Organization-level metrics (multi-account)
type OrganizationConfig struct {
    Enabled bool `mapstructure:"enabled"`
    
    // Organization metrics with intervals
    OrgCreditUsage   MetricConfig `mapstructure:"org_credit_usage"`
    OrgStorageUsage  MetricConfig `mapstructure:"org_storage_usage"`
    OrgDataTransfer  MetricConfig `mapstructure:"org_data_transfer"`
    OrgContractUsage MetricConfig `mapstructure:"org_contract_usage"`
}

// Custom SQL queries
type CustomQueriesConfig struct {
    Enabled bool          `mapstructure:"enabled"`
    Queries []CustomQuery `mapstructure:"queries"`
}

type CustomQuery struct {
    Name        string `mapstructure:"name"`
    SQL         string `mapstructure:"sql"`
    Interval    string `mapstructure:"interval"`
    MetricType  string `mapstructure:"metric_type"`  // gauge, counter, histogram
    ValueColumn string `mapstructure:"value_column"`
    LabelColumns []string `mapstructure:"label_columns"`
}

type MetricConfig struct {
    Enabled  bool   `mapstructure:"enabled"`
    Interval string `mapstructure:"interval"`
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
    
    // Validate Event Tables config
    if cfg.EventTables.Enabled && cfg.EventTables.TableName == "" {
        return errors.New("event_tables.table_name is required when event_tables.enabled is true")
    }
    
    // Validate custom queries
    if cfg.CustomQueries.Enabled {
        for i, query := range cfg.CustomQueries.Queries {
            if query.Name == "" {
                return fmt.Errorf("custom_queries[%d].name is required", i)
            }
            if query.SQL == "" {
                return fmt.Errorf("custom_queries[%d].sql is required", i)
            }
            if query.ValueColumn == "" {
                return fmt.Errorf("custom_queries[%d].value_column is required", i)
            }
            if query.MetricType != "" && query.MetricType != "gauge" && query.MetricType != "counter" && query.MetricType != "histogram" {
                return fmt.Errorf("custom_queries[%d].metric_type must be gauge, counter, or histogram", i)
            }
        }
    }
    
    // Validate intervals for enabled metrics
    allMetrics := make(map[string]MetricConfig)
    
    // Standard metrics
    allMetrics["current_queries"] = cfg.Metrics.CurrentQueries
    allMetrics["warehouse_load"] = cfg.Metrics.WarehouseLoad
    allMetrics["query_history"] = cfg.Metrics.QueryHistory
    allMetrics["credit_usage"] = cfg.Metrics.CreditUsage
    allMetrics["storage_metrics"] = cfg.Metrics.StorageMetrics
    allMetrics["login_history"] = cfg.Metrics.LoginHistory
    allMetrics["data_pipeline"] = cfg.Metrics.DataPipeline
    allMetrics["database_storage"] = cfg.Metrics.DatabaseStorage
    allMetrics["task_history"] = cfg.Metrics.TaskHistory
    allMetrics["replication_usage"] = cfg.Metrics.ReplicationUsage
    allMetrics["auto_clustering_history"] = cfg.Metrics.AutoClusteringHistory
    
    // Event Tables metrics
    if cfg.EventTables.Enabled {
        allMetrics["event_query_logs"] = cfg.EventTables.QueryLogs
        allMetrics["event_task_logs"] = cfg.EventTables.TaskLogs
        allMetrics["event_function_logs"] = cfg.EventTables.FunctionLogs
        allMetrics["event_procedure_logs"] = cfg.EventTables.ProcedureLogs
    }
    
    // Organization metrics
    if cfg.Organization.Enabled {
        allMetrics["org_credit_usage"] = cfg.Organization.OrgCreditUsage
        allMetrics["org_storage_usage"] = cfg.Organization.OrgStorageUsage
        allMetrics["org_data_transfer"] = cfg.Organization.OrgDataTransfer
        allMetrics["org_contract_usage"] = cfg.Organization.OrgContractUsage
    }
    
    for name, metric := range allMetrics {
        if metric.Enabled && metric.Interval != "" {
            if _, err := time.ParseDuration(metric.Interval); err != nil {
                return fmt.Errorf("invalid interval for %s: %w", name, err)
            }
        }
    }
    
    return nil
}

func (mc *MetricConfig) GetInterval(defaultInterval time.Duration) time.Duration {
    if mc.Interval != "" {
        d, err := time.ParseDuration(mc.Interval)
        if err == nil {
            return d
        }
    }
    return defaultInterval
}

func (cfg *Config) GetBaseInterval() time.Duration {
    minInterval := 30 * time.Second // Event Tables can be very fast
    
    allMetrics := []MetricConfig{
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
            cfg.Organization.OrgCreditUsage,
            cfg.Organization.OrgStorageUsage,
            cfg.Organization.OrgDataTransfer,
            cfg.Organization.OrgContractUsage,
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

func createDefaultConfig() component.Config {
    return &Config{
        ClientConfig: confighttp.NewDefaultClientConfig(),
        Schema:       "ACCOUNT_USAGE",
        Role:         "ACCOUNTADMIN",
        Metrics: MetricsConfig{
            CurrentQueries: MetricConfig{Enabled: true, Interval: "1m"},
            WarehouseLoad:  MetricConfig{Enabled: true, Interval: "1m"},
            QueryHistory:   MetricConfig{Enabled: true, Interval: "5m"},
            CreditUsage:    MetricConfig{Enabled: true, Interval: "5m"},
            StorageMetrics: MetricConfig{Enabled: true, Interval: "30m"},
            LoginHistory:   MetricConfig{Enabled: true, Interval: "10m"},
            DataPipeline:   MetricConfig{Enabled: true, Interval: "10m"},
            DatabaseStorage: MetricConfig{Enabled: true, Interval: "30m"},
            TaskHistory:    MetricConfig{Enabled: true, Interval: "10m"},
            ReplicationUsage: MetricConfig{Enabled: true, Interval: "15m"},
            AutoClusteringHistory: MetricConfig{Enabled: true, Interval: "15m"},
        },
        EventTables: EventTablesConfig{
            Enabled:   false, // Must be explicitly enabled
            TableName: "",
            QueryLogs:      MetricConfig{Enabled: true, Interval: "30s"},
            TaskLogs:       MetricConfig{Enabled: true, Interval: "30s"},
            FunctionLogs:   MetricConfig{Enabled: true, Interval: "30s"},
            ProcedureLogs:  MetricConfig{Enabled: true, Interval: "30s"},
        },
        Organization: OrganizationConfig{
            Enabled: false, // Must be explicitly enabled
            OrgCreditUsage:   MetricConfig{Enabled: true, Interval: "1h"},
            OrgStorageUsage:  MetricConfig{Enabled: true, Interval: "1h"},
            OrgDataTransfer:  MetricConfig{Enabled: true, Interval: "1h"},
            OrgContractUsage: MetricConfig{Enabled: true, Interval: "12h"},
        },
        CustomQueries: CustomQueriesConfig{
            Enabled: false,
            Queries: []CustomQuery{},
        },
    }
}
