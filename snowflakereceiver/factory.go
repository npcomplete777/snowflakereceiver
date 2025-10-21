// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package snowflakereceiver

import (
    "context"

    "go.opentelemetry.io/collector/component"
    "go.opentelemetry.io/collector/consumer"
    "go.opentelemetry.io/collector/receiver"
    "go.opentelemetry.io/collector/scraper"
    "go.opentelemetry.io/collector/scraper/scraperhelper"
)

var (
    typeVal = component.MustNewType("snowflake")
)

// NewFactory creates a factory for Snowflake receiver
func NewFactory() receiver.Factory {
    return receiver.NewFactory(
        typeVal,
        createDefaultConfig,
        receiver.WithMetrics(createMetricsReceiver, component.StabilityLevelAlpha),
    )
}

// createDefaultConfig creates the default configuration
func createDefaultConfig() component.Config {
    return &Config{
        Database:        "SNOWFLAKE",
        Schema:          "ACCOUNT_USAGE",
        QueryTimeout:    "30s",
        MaxRowsPerQuery: 10000,
        RateLimitQPS:    10,
        MaxRetries:      3,
        RetryInitialDelay: "1s",
        RetryMaxDelay:   "30s",
        
        // Default cardinality limits
        MaxUsersCardinality:     500,
        MaxSchemasCardinality:   200,
        MaxDatabasesCardinality: 100,
        MaxRolesCardinality:     200,
        
        Metrics: MetricsConfig{
            CurrentQueries:        MetricCategoryConfig{Enabled: true, Interval: "1m"},
            WarehouseLoad:         MetricCategoryConfig{Enabled: true, Interval: "1m"},
            QueryHistory:          MetricCategoryConfig{Enabled: true, Interval: "5m"},
            CreditUsage:           MetricCategoryConfig{Enabled: true, Interval: "5m"},
            StorageMetrics:        MetricCategoryConfig{Enabled: true, Interval: "30m"},
            LoginHistory:          MetricCategoryConfig{Enabled: false, Interval: "10m"},
            DataPipeline:          MetricCategoryConfig{Enabled: false, Interval: "10m"},
            DatabaseStorage:       MetricCategoryConfig{Enabled: true, Interval: "30m"},
            TaskHistory:           MetricCategoryConfig{Enabled: false, Interval: "10m"},
            ReplicationUsage:      MetricCategoryConfig{Enabled: false, Interval: "15m"},
            AutoClusteringHistory: MetricCategoryConfig{Enabled: false, Interval: "15m"},
        },
        EventTables: EventTablesConfig{
            Enabled:   false,
            TableName: "SNOWFLAKE.ACCOUNT_USAGE.EVENT_TABLE",
            QueryLogs: MetricCategoryConfig{Enabled: false, Interval: "30s"},
            TaskLogs:  MetricCategoryConfig{Enabled: false, Interval: "30s"},
        },
        Organization: OrganizationConfig{
            Enabled:                false,
            OrganizationNameColumn: "ORGANIZATION_NAME",
            AccountNameColumn:      "ACCOUNT_NAME",
            OrgCreditUsage:    OrgCreditUsageConfig{Enabled: false, Interval: "1h"},
            OrgStorageUsage:   OrgStorageUsageConfig{Enabled: false, Interval: "1h"},
            OrgDataTransfer:   OrgDataTransferConfig{Enabled: false, Interval: "1h"},
            OrgContractUsage:  OrgContractUsageConfig{Enabled: false, Interval: "1h"},
        },
        CustomQueries: CustomQueriesConfig{
            Enabled: false,
            Queries: []CustomQuery{},
        },
    }
}

// createMetricsReceiver creates a metrics receiver
func createMetricsReceiver(
    ctx context.Context,
    settings receiver.Settings,
    cfg component.Config,
    consumer consumer.Metrics,
) (receiver.Metrics, error) {
    snowflakeCfg := cfg.(*Config)
    
    // Calculate the shortest interval across all metrics
    interval := snowflakeCfg.GetBaseInterval()
    
    // Create the scraper
    s, err := newSnowflakeScraper(settings, snowflakeCfg)
    if err != nil {
        return nil, err
    }
    
    // Configure scraping
    scraperCfg := &scraperhelper.ControllerConfig{
        CollectionInterval: interval,
        InitialDelay:       interval,
    }
    
    // Wrap scraper with shutdown support
    sc, err := scraper.NewMetrics(
        s.scrape,
        scraper.WithShutdown(s.Shutdown),
    )
    if err != nil {
        return nil, err
    }
    
    // Create controller that will call scrape() periodically
    return scraperhelper.NewMetricsController(
        scraperCfg,
        settings,
        consumer,
        scraperhelper.AddScraper(typeVal, sc),
    )
}
