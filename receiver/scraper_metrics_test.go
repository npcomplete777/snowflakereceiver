package snowflakereceiver

import (
    "database/sql"
    "testing"
    "time"
    
    "github.com/stretchr/testify/assert"
    "go.opentelemetry.io/collector/pdata/pcommon"
    "go.opentelemetry.io/collector/pdata/pmetric"
)

func TestScraper_AddWarehouseLoadMetrics(t *testing.T) {
    config := &Config{
        User:      "test",
        Password:  "test",
        Account:   "test-account",
        Warehouse: "TEST_WH",
    }
    
    scraper := newTestScraper(t, config)
    
    loads := []warehouseLoadRow{
        {
            warehouseName:          sql.NullString{String: "WH1", Valid: true},
            avgRunning:             sql.NullFloat64{Float64: 5.5, Valid: true},
            avgQueuedLoad:          sql.NullFloat64{Float64: 2.0, Valid: true},
            avgQueuedProvisioning:  sql.NullFloat64{Float64: 0.5, Valid: true},
            avgBlocked:             sql.NullFloat64{Float64: 0.0, Valid: true},
        },
    }
    
    md := pmetric.NewMetrics()
    scopeMetrics := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
    now := pcommon.NewTimestampFromTime(time.Now())
    
    scraper.addWarehouseLoadMetrics(scopeMetrics, loads, now)
    
    metrics := scopeMetrics.Metrics()
    assert.Equal(t, 4, metrics.Len())
    
    assert.Equal(t, "snowflake.warehouse.queries.running", metrics.At(0).Name())
    assert.Equal(t, "snowflake.warehouse.queries.queued.overload", metrics.At(1).Name())
    assert.Equal(t, "snowflake.warehouse.queries.queued.provisioning", metrics.At(2).Name())
    assert.Equal(t, "snowflake.warehouse.queries.blocked", metrics.At(3).Name())
}

func TestScraper_AddQueryMetrics(t *testing.T) {
    config := &Config{
        User:      "test",
        Password:  "test",
        Account:   "test-account",
        Warehouse: "TEST_WH",
    }
    
    scraper := newTestScraper(t, config)
    
    stats := []queryStatRow{
        {
            warehouseName:      sql.NullString{String: "WH1", Valid: true},
            queryType:          sql.NullString{String: "SELECT", Valid: true},
            executionStatus:    sql.NullString{String: "SUCCESS", Valid: true},
            queryCount:         100,
            avgExecutionTime:   sql.NullFloat64{Float64: 2500.0, Valid: true},
            avgBytesScanned:    sql.NullFloat64{Float64: 1024000.0, Valid: true},
            avgBytesWritten:    sql.NullFloat64{Float64: 512000.0, Valid: true},
            avgRowsProduced:    sql.NullFloat64{Float64: 1000.0, Valid: true},
            avgCompilationTime: sql.NullFloat64{Float64: 100.0, Valid: true},
        },
    }
    
    md := pmetric.NewMetrics()
    scopeMetrics := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
    now := pcommon.NewTimestampFromTime(time.Now())
    
    scraper.addQueryMetrics(scopeMetrics, stats, now)
    
    metrics := scopeMetrics.Metrics()
    assert.Equal(t, 6, metrics.Len())
    
    assert.Equal(t, "snowflake.queries.count", metrics.At(0).Name())
    assert.Equal(t, "snowflake.queries.execution.time", metrics.At(1).Name())
    assert.Equal(t, "snowflake.queries.bytes.scanned", metrics.At(2).Name())
    assert.Equal(t, "snowflake.queries.bytes.written", metrics.At(3).Name())
    assert.Equal(t, "snowflake.queries.rows.produced", metrics.At(4).Name())
    assert.Equal(t, "snowflake.queries.compilation.time", metrics.At(5).Name())
}

func TestScraper_AddPipeMetrics(t *testing.T) {
    config := &Config{
        User:      "test",
        Password:  "test",
        Account:   "test-account",
        Warehouse: "TEST_WH",
    }
    
    scraper := newTestScraper(t, config)
    
    pipes := []pipeUsageRow{
        {
            pipeName:      sql.NullString{String: "MY_PIPE", Valid: true},
            totalCredits:  5.25,
            bytesInserted: sql.NullFloat64{Float64: 10240000.0, Valid: true},
            filesInserted: sql.NullInt64{Int64: 100, Valid: true},
        },
    }
    
    md := pmetric.NewMetrics()
    scopeMetrics := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
    now := pcommon.NewTimestampFromTime(time.Now())
    
    scraper.addPipeMetrics(scopeMetrics, pipes, now)
    
    metrics := scopeMetrics.Metrics()
    assert.Equal(t, 3, metrics.Len())
}

func TestScraper_AddDatabaseStorageMetrics(t *testing.T) {
    config := &Config{
        User:      "test",
        Password:  "test",
        Account:   "test-account",
        Warehouse: "TEST_WH",
    }
    
    scraper := newTestScraper(t, config)
    
    dbStorage := []databaseStorageRow{
        {
            databaseName:     sql.NullString{String: "MY_DB", Valid: true},
            avgDatabaseBytes: sql.NullFloat64{Float64: 1024000.0, Valid: true},
            avgFailsafeBytes: sql.NullFloat64{Float64: 102400.0, Valid: true},
        },
    }
    
    md := pmetric.NewMetrics()
    scopeMetrics := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
    now := pcommon.NewTimestampFromTime(time.Now())
    
    scraper.addDatabaseStorageMetrics(scopeMetrics, dbStorage, now)
    
    metrics := scopeMetrics.Metrics()
    assert.Equal(t, 2, metrics.Len())
}

func TestScraper_AddTaskHistoryMetrics(t *testing.T) {
    config := &Config{
        User:      "test",
        Password:  "test",
        Account:   "test-account",
        Warehouse: "TEST_WH",
    }
    
    scraper := newTestScraper(t, config)
    
    tasks := []taskHistoryRow{
        {
            databaseName:     sql.NullString{String: "MY_DB", Valid: true},
            schemaName:       sql.NullString{String: "MY_SCHEMA", Valid: true},
            taskName:         sql.NullString{String: "MY_TASK", Valid: true},
            state:            sql.NullString{String: "SUCCEEDED", Valid: true},
            executionCount:   50,
            avgExecutionTime: sql.NullFloat64{Float64: 5000.0, Valid: true},
        },
    }
    
    md := pmetric.NewMetrics()
    scopeMetrics := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
    now := pcommon.NewTimestampFromTime(time.Now())
    
    scraper.addTaskHistoryMetrics(scopeMetrics, tasks, now)
    
    metrics := scopeMetrics.Metrics()
    assert.Equal(t, 2, metrics.Len())
}

func TestScraper_AddReplicationMetrics(t *testing.T) {
    config := &Config{
        User:      "test",
        Password:  "test",
        Account:   "test-account",
        Warehouse: "TEST_WH",
    }
    
    scraper := newTestScraper(t, config)
    
    replications := []replicationUsageRow{
        {
            databaseName:     sql.NullString{String: "MY_DB", Valid: true},
            totalCredits:     3.5,
            bytesTransferred: sql.NullFloat64{Float64: 10240000.0, Valid: true},
        },
    }
    
    md := pmetric.NewMetrics()
    scopeMetrics := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
    now := pcommon.NewTimestampFromTime(time.Now())
    
    scraper.addReplicationMetrics(scopeMetrics, replications, now)
    
    metrics := scopeMetrics.Metrics()
    assert.Equal(t, 2, metrics.Len())
}

func TestScraper_AddAutoClusteringMetrics(t *testing.T) {
    config := &Config{
        User:      "test",
        Password:  "test",
        Account:   "test-account",
        Warehouse: "TEST_WH",
    }
    
    scraper := newTestScraper(t, config)
    
    clusterings := []autoClusteringRow{
        {
            databaseName:     sql.NullString{String: "MY_DB", Valid: true},
            schemaName:       sql.NullString{String: "MY_SCHEMA", Valid: true},
            tableName:        sql.NullString{String: "MY_TABLE", Valid: true},
            totalCredits:     2.5,
            bytesReclustered: sql.NullFloat64{Float64: 10240000.0, Valid: true},
            rowsReclustered:  sql.NullFloat64{Float64: 100000.0, Valid: true},
        },
    }
    
    md := pmetric.NewMetrics()
    scopeMetrics := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
    now := pcommon.NewTimestampFromTime(time.Now())
    
    scraper.addAutoClusteringMetrics(scopeMetrics, clusterings, now)
    
    metrics := scopeMetrics.Metrics()
    assert.Equal(t, 3, metrics.Len())
}

func TestScraper_AddEventTableMetrics(t *testing.T) {
    config := &Config{
        User:      "test",
        Password:  "test",
        Account:   "test-account",
        Warehouse: "TEST_WH",
    }
    
    scraper := newTestScraper(t, config)
    
    events := []eventTableRow{
        {
            eventType:    "QUERY",
            severity:     sql.NullString{String: "INFO", Valid: true},
            errorMessage: sql.NullString{String: "", Valid: false},
        },
        {
            eventType:    "QUERY",
            severity:     sql.NullString{String: "ERROR", Valid: true},
            errorMessage: sql.NullString{String: "Query failed", Valid: true},
        },
    }
    
    md := pmetric.NewMetrics()
    scopeMetrics := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
    now := pcommon.NewTimestampFromTime(time.Now())
    
    scraper.addEventTableMetrics(scopeMetrics, events, now)
    
    metrics := scopeMetrics.Metrics()
    assert.Greater(t, metrics.Len(), 0)
}

func TestScraper_AddOrgCreditMetrics(t *testing.T) {
    config := &Config{
        User:      "test",
        Password:  "test",
        Account:   "test-account",
        Warehouse: "TEST_WH",
    }
    
    scraper := newTestScraper(t, config)
    
    orgCredits := []orgCreditUsageRow{
        {
            organizationName: sql.NullString{String: "MY_ORG", Valid: true},
            accountName:      sql.NullString{String: "MY_ACCOUNT", Valid: true},
            serviceType:      sql.NullString{String: "COMPUTE", Valid: true},
            totalCredits:     100.5,
        },
    }
    
    md := pmetric.NewMetrics()
    scopeMetrics := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
    now := pcommon.NewTimestampFromTime(time.Now())
    
    scraper.addOrgCreditMetrics(scopeMetrics, orgCredits, now)
    
    metrics := scopeMetrics.Metrics()
    assert.Equal(t, 1, metrics.Len())
    assert.Equal(t, "snowflake.organization.credits.usage", metrics.At(0).Name())
}

func TestScraper_AddOrgStorageMetrics(t *testing.T) {
    config := &Config{
        User:      "test",
        Password:  "test",
        Account:   "test-account",
        Warehouse: "TEST_WH",
    }
    
    scraper := newTestScraper(t, config)
    
    orgStorage := []orgStorageUsageRow{
        {
            organizationName: sql.NullString{String: "MY_ORG", Valid: true},
            accountName:      sql.NullString{String: "MY_ACCOUNT", Valid: true},
            avgStorageBytes:  sql.NullFloat64{Float64: 1024000000.0, Valid: true},
            avgStageBytes:    sql.NullFloat64{Float64: 102400000.0, Valid: true},
            avgFailsafeBytes: sql.NullFloat64{Float64: 10240000.0, Valid: true},
        },
    }
    
    md := pmetric.NewMetrics()
    scopeMetrics := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
    now := pcommon.NewTimestampFromTime(time.Now())
    
    scraper.addOrgStorageMetrics(scopeMetrics, orgStorage, now)
    
    metrics := scopeMetrics.Metrics()
    assert.Equal(t, 3, metrics.Len())
}

func TestScraper_AddOrgDataTransferMetrics(t *testing.T) {
    config := &Config{
        User:      "test",
        Password:  "test",
        Account:   "test-account",
        Warehouse: "TEST_WH",
    }
    
    scraper := newTestScraper(t, config)
    
    transfers := []orgDataTransferRow{
        {
            organizationName:      sql.NullString{String: "MY_ORG", Valid: true},
            sourceAccountName:     sql.NullString{String: "ACCOUNT1", Valid: true},
            targetAccountName:     sql.NullString{String: "ACCOUNT2", Valid: true},
            sourceRegion:          sql.NullString{String: "US_EAST", Valid: true},
            targetRegion:          sql.NullString{String: "US_WEST", Valid: true},
            totalBytesTransferred: sql.NullFloat64{Float64: 10240000.0, Valid: true},
        },
    }
    
    md := pmetric.NewMetrics()
    scopeMetrics := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
    now := pcommon.NewTimestampFromTime(time.Now())
    
    scraper.addOrgDataTransferMetrics(scopeMetrics, transfers, now)
    
    metrics := scopeMetrics.Metrics()
    assert.Equal(t, 1, metrics.Len())
}

func TestScraper_AddOrgContractMetrics(t *testing.T) {
    config := &Config{
        User:      "test",
        Password:  "test",
        Account:   "test-account",
        Warehouse: "TEST_WH",
    }
    
    scraper := newTestScraper(t, config)
    
    contracts := []orgContractUsageRow{
        {
            organizationName:  sql.NullString{String: "MY_ORG", Valid: true},
            contractNumber:    sql.NullInt64{Int64: 12345, Valid: true},
            totalCreditsUsed:  1000.5,
            totalCreditsBilled: sql.NullFloat64{Float64: 1200.0, Valid: true},
        },
    }
    
    md := pmetric.NewMetrics()
    scopeMetrics := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
    now := pcommon.NewTimestampFromTime(time.Now())
    
    scraper.addOrgContractMetrics(scopeMetrics, contracts, now)
    
    metrics := scopeMetrics.Metrics()
    assert.Equal(t, 2, metrics.Len())
}

func TestScraper_AddCustomQueryMetrics_MultipleTypes(t *testing.T) {
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
                    ValueColumn:  "VALUE",
                    LabelColumns: []string{"LABEL"},
                },
            },
        },
    }
    
    scraper := newTestScraper(t, config)
    
    tests := []struct {
        name  string
        value interface{}
    }{
        {"float64", float64(42.5)},
        {"float32", float32(42.5)},
        {"int64", int64(42)},
        {"int32", int32(42)},
        {"int", int(42)},
        {"string", "42.5"},
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result := customQueryResult{
                name:       "test_query",
                metricType: "gauge",
                rows: []map[string]interface{}{
                    {
                        "LABEL": "test",
                        "VALUE": tt.value,
                    },
                },
            }
            
            md := pmetric.NewMetrics()
            scopeMetrics := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
            now := pcommon.NewTimestampFromTime(time.Now())
            
            scraper.addCustomQueryMetrics(scopeMetrics, result, now)
            
            metrics := scopeMetrics.Metrics()
            assert.Equal(t, 1, metrics.Len())
        })
    }
}
