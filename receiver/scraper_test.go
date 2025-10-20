package snowflakereceiver

import (
    "database/sql"
    "testing"
    "time"
    
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
    "go.opentelemetry.io/collector/component"
    "go.opentelemetry.io/collector/pdata/pcommon"
    "go.opentelemetry.io/collector/pdata/pmetric"
    "go.opentelemetry.io/collector/receiver"
    "go.uber.org/zap"
)

func newTestScraper(t *testing.T, config *Config) *snowflakeScraper {
    settings := receiver.Settings{
        ID: component.MustNewID("snowflake"),
        TelemetrySettings: component.TelemetrySettings{
            Logger: zap.NewNop(),
        },
    }
    
    scraper, err := newSnowflakeScraper(settings, config)
    require.NoError(t, err)
    
    return scraper
}

func TestNewSnowflakeScraper(t *testing.T) {
    config := &Config{
        User:      "test",
        Password:  "test",
        Account:   "test-account",
        Warehouse: "TEST_WH",
        Database:  "SNOWFLAKE",
        Schema:    "ACCOUNT_USAGE",
    }
    
    scraper := newTestScraper(t, config)
    
    assert.NotNil(t, scraper)
    assert.NotNil(t, scraper.logger)
    assert.NotNil(t, scraper.client)
    assert.NotNil(t, scraper.lastRun)
}

func TestScraper_ShouldScrape(t *testing.T) {
    config := &Config{
        User:      "test",
        Password:  "test",
        Account:   "test-account",
        Warehouse: "TEST_WH",
    }
    
    scraper := newTestScraper(t, config)
    
    // First scrape - no last run
    assert.True(t, scraper.shouldScrape("test_metric", 1*time.Minute))
    
    // Mark as scraped
    scraper.markScraped("test_metric")
    
    // Immediately after - should not scrape
    assert.False(t, scraper.shouldScrape("test_metric", 1*time.Minute))
    
    // Set last run to past
    scraper.lastRun["test_metric"] = time.Now().Add(-2 * time.Minute)
    
    // Should scrape now
    assert.True(t, scraper.shouldScrape("test_metric", 1*time.Minute))
}

func TestScraper_MarkScraped(t *testing.T) {
    config := &Config{
        User:      "test",
        Password:  "test",
        Account:   "test-account",
        Warehouse: "TEST_WH",
    }
    
    scraper := newTestScraper(t, config)
    
    before := time.Now()
    scraper.markScraped("test_metric")
    after := time.Now()
    
    lastRun, exists := scraper.lastRun["test_metric"]
    require.True(t, exists)
    assert.True(t, lastRun.After(before) || lastRun.Equal(before))
    assert.True(t, lastRun.Before(after) || lastRun.Equal(after))
}

func TestScraper_AddCurrentQueryMetrics(t *testing.T) {
    config := &Config{
        User:      "test",
        Password:  "test",
        Account:   "test-account",
        Warehouse: "TEST_WH",
        Database:  "SNOWFLAKE",
    }
    
    scraper := newTestScraper(t, config)
    
    queries := []currentQueryRow{
        {
            warehouseName:    sql.NullString{String: "WH1", Valid: true},
            queryType:        sql.NullString{String: "SELECT", Valid: true},
            executionStatus:  sql.NullString{String: "SUCCESS", Valid: true},
            queryCount:       10,
            avgExecutionTime: sql.NullFloat64{Float64: 1500.0, Valid: true},
            avgBytesScanned:  sql.NullFloat64{Float64: 1024000.0, Valid: true},
        },
    }
    
    md := pmetric.NewMetrics()
    scopeMetrics := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
    now := pcommon.NewTimestampFromTime(time.Now())
    
    scraper.addCurrentQueryMetrics(scopeMetrics, queries, now)
    
    metrics := scopeMetrics.Metrics()
    assert.Greater(t, metrics.Len(), 0)
    
    metric := metrics.At(0)
    assert.Equal(t, "snowflake.queries.current.count", metric.Name())
}

func TestScraper_AddCreditMetrics(t *testing.T) {
    config := &Config{
        User:      "test",
        Password:  "test",
        Account:   "test-account",
        Warehouse: "TEST_WH",
    }
    
    scraper := newTestScraper(t, config)
    
    credits := []creditUsageRow{
        {
            warehouseName:       sql.NullString{String: "WH1", Valid: true},
            totalCredits:        10.5,
            computeCredits:      sql.NullFloat64{Float64: 8.0, Valid: true},
            cloudServiceCredits: sql.NullFloat64{Float64: 2.5, Valid: true},
        },
    }
    
    md := pmetric.NewMetrics()
    scopeMetrics := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
    now := pcommon.NewTimestampFromTime(time.Now())
    
    scraper.addCreditMetrics(scopeMetrics, credits, now)
    
    metrics := scopeMetrics.Metrics()
    assert.Equal(t, 3, metrics.Len())
    assert.Equal(t, "snowflake.warehouse.credits.usage.total", metrics.At(0).Name())
}

func TestScraper_AddStorageMetrics(t *testing.T) {
    config := &Config{
        User:      "test",
        Password:  "test",
        Account:   "test-account",
        Warehouse: "TEST_WH",
    }
    
    scraper := newTestScraper(t, config)
    
    storage := []storageUsageRow{
        {
            totalStorageBytes: sql.NullFloat64{Float64: 1024000000.0, Valid: true},
            stageBytes:        sql.NullFloat64{Float64: 102400000.0, Valid: true},
            failsafeBytes:     sql.NullFloat64{Float64: 10240000.0, Valid: true},
        },
    }
    
    md := pmetric.NewMetrics()
    scopeMetrics := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
    now := pcommon.NewTimestampFromTime(time.Now())
    
    scraper.addStorageMetrics(scopeMetrics, storage, now)
    
    metrics := scopeMetrics.Metrics()
    assert.Equal(t, 3, metrics.Len())
    assert.Equal(t, "snowflake.storage.bytes.total", metrics.At(0).Name())
}

func TestScraper_AddLoginMetrics(t *testing.T) {
    config := &Config{
        User:      "test",
        Password:  "test",
        Account:   "test-account",
        Warehouse: "TEST_WH",
    }
    
    scraper := newTestScraper(t, config)
    
    logins := []loginHistoryRow{
        {
            isSuccess:  "YES",
            errorCode:  sql.NullString{String: "", Valid: false},
            loginCount: 100,
        },
        {
            isSuccess:  "NO",
            errorCode:  sql.NullString{String: "390201", Valid: true},
            loginCount: 5,
        },
    }
    
    md := pmetric.NewMetrics()
    scopeMetrics := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
    now := pcommon.NewTimestampFromTime(time.Now())
    
    scraper.addLoginMetrics(scopeMetrics, logins, now)
    
    metrics := scopeMetrics.Metrics()
    assert.Equal(t, 2, metrics.Len())
}

func TestScraper_AddCustomQueryMetrics(t *testing.T) {
    config := &Config{
        User:      "test",
        Password:  "test",
        Account:   "test-account",
        Warehouse: "TEST_WH",
        CustomQueries: CustomQueriesConfig{
            Enabled: true,
            Queries: []CustomQuery{
                {
                    Name:         "test_query",
                    ValueColumn:  "METRIC_VALUE",
                    LabelColumns: []string{"LABEL1"},
                },
            },
        },
    }
    
    scraper := newTestScraper(t, config)
    
    result := customQueryResult{
        name:       "test_query",
        metricType: "gauge",
        rows: []map[string]interface{}{
            {
                "LABEL1":       "value1",
                "METRIC_VALUE": 42.5,
            },
        },
    }
    
    md := pmetric.NewMetrics()
    scopeMetrics := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
    now := pcommon.NewTimestampFromTime(time.Now())
    
    scraper.addCustomQueryMetrics(scopeMetrics, result, now)
    
    metrics := scopeMetrics.Metrics()
    assert.Equal(t, 1, metrics.Len())
    assert.Equal(t, "snowflake.custom.test_query", metrics.At(0).Name())
}

func TestScraper_BuildMetrics(t *testing.T) {
    config := &Config{
        User:      "test",
        Password:  "test",
        Account:   "test-account",
        Warehouse: "TEST_WH",
        Database:  "SNOWFLAKE",
        Metrics: MetricsConfig{
            CurrentQueries: MetricCategoryConfig{
                Enabled:  true,
                Interval: "1m",
            },
            WarehouseLoad:  MetricCategoryConfig{Enabled: false},
            QueryHistory:   MetricCategoryConfig{Enabled: false},
            CreditUsage:    MetricCategoryConfig{Enabled: false},
            StorageMetrics: MetricCategoryConfig{Enabled: false},
            LoginHistory:   MetricCategoryConfig{Enabled: false},
            DataPipeline:   MetricCategoryConfig{Enabled: false},
            DatabaseStorage: MetricCategoryConfig{Enabled: false},
            TaskHistory:    MetricCategoryConfig{Enabled: false},
            ReplicationUsage: MetricCategoryConfig{Enabled: false},
            AutoClusteringHistory: MetricCategoryConfig{Enabled: false},
        },
    }
    
    scraper := newTestScraper(t, config)
    
    metrics := &snowflakeMetrics{
        currentQueries: []currentQueryRow{
            {
                warehouseName:   sql.NullString{String: "WH1", Valid: true},
                queryCount:      10,
            },
        },
    }
    
    md := scraper.buildMetrics(metrics)
    
    assert.Equal(t, 1, md.ResourceMetrics().Len())
    
    resourceAttrs := md.ResourceMetrics().At(0).Resource().Attributes()
    account, exists := resourceAttrs.Get("snowflake.account.name")
    assert.True(t, exists)
    assert.Equal(t, "test-account", account.AsString())
}
