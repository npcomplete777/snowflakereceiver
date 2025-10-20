package snowflakereceiver

import (
    "context"
    "database/sql"
    "testing"
    "time"
    
    "github.com/DATA-DOG/go-sqlmock"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

// Test the 0% coverage functions

func TestClient_QueryEventTableLogs(t *testing.T) {
    client, mock := newMockClient(t)
    defer client.db.Close()
    
    client.config.EventTables.TableName = "SNOWFLAKE.ACCOUNT_USAGE.EVENT_TABLE"
    
    rows := sqlmock.NewRows([]string{
        "EVENT_TYPE", "SEVERITY_TEXT", "ERROR_MESSAGE",
    }).
        AddRow("QUERY", "INFO", sql.NullString{}).
        AddRow("QUERY", "ERROR", "Query failed")
    
    mock.ExpectQuery("SELECT(.+)FROM SNOWFLAKE.ACCOUNT_USAGE.EVENT_TABLE").
        WillReturnRows(rows)
    
    metrics := &snowflakeMetrics{}
    err := client.queryEventTableLogs(context.Background(), metrics, "QUERY")
    
    require.NoError(t, err)
    assert.Len(t, metrics.eventQueryLogs, 2)
}

func TestClient_QueryEventTableLogs_NoTableName(t *testing.T) {
    client, _ := newMockClient(t)
    defer client.db.Close()
    
    client.config.EventTables.TableName = ""
    
    metrics := &snowflakeMetrics{}
    err := client.queryEventTableLogs(context.Background(), metrics, "QUERY")
    
    require.Error(t, err)
    assert.Contains(t, err.Error(), "event table name not configured")
}

func TestClient_QueryOrgCreditUsage(t *testing.T) {
    client, mock := newMockClient(t)
    defer client.db.Close()
    
    rows := sqlmock.NewRows([]string{
        "ORGANIZATION_NAME", "ACCOUNT_NAME", "SERVICE_TYPE", "TOTAL_CREDITS",
    }).AddRow("MY_ORG", "MY_ACCOUNT", "COMPUTE", 100.5)
    
    mock.ExpectQuery("SELECT(.+)FROM SNOWFLAKE.ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY").
        WillReturnRows(rows)
    
    metrics := &snowflakeMetrics{}
    err := client.queryOrgCreditUsage(context.Background(), metrics)
    
    require.NoError(t, err)
    assert.Len(t, metrics.orgCreditUsage, 1)
    assert.Equal(t, 100.5, metrics.orgCreditUsage[0].totalCredits)
}

func TestClient_QueryOrgStorageUsage(t *testing.T) {
    client, mock := newMockClient(t)
    defer client.db.Close()
    
    rows := sqlmock.NewRows([]string{
        "ORGANIZATION_NAME", "ACCOUNT_NAME", "AVG_STORAGE_BYTES",
        "AVG_STAGE_BYTES", "AVG_FAILSAFE_BYTES",
    }).AddRow("MY_ORG", "MY_ACCOUNT", 1024000000.0, 102400000.0, 10240000.0)
    
    mock.ExpectQuery("SELECT(.+)FROM SNOWFLAKE.ORGANIZATION_USAGE.STORAGE_DAILY_HISTORY").
        WillReturnRows(rows)
    
    metrics := &snowflakeMetrics{}
    err := client.queryOrgStorageUsage(context.Background(), metrics)
    
    require.NoError(t, err)
    assert.Len(t, metrics.orgStorageUsage, 1)
}

func TestClient_QueryOrgDataTransfer(t *testing.T) {
    client, mock := newMockClient(t)
    defer client.db.Close()
    
    rows := sqlmock.NewRows([]string{
        "ORGANIZATION_NAME", "SOURCE_ACCOUNT_NAME", "TARGET_ACCOUNT_NAME",
        "SOURCE_REGION", "TARGET_REGION", "TOTAL_BYTES_TRANSFERRED",
    }).AddRow("MY_ORG", "ACCOUNT1", "ACCOUNT2", "US_EAST", "US_WEST", 10240000.0)
    
    mock.ExpectQuery("SELECT(.+)FROM SNOWFLAKE.ORGANIZATION_USAGE.DATA_TRANSFER_DAILY_HISTORY").
        WillReturnRows(rows)
    
    metrics := &snowflakeMetrics{}
    err := client.queryOrgDataTransfer(context.Background(), metrics)
    
    require.NoError(t, err)
    assert.Len(t, metrics.orgDataTransfer, 1)
}

func TestClient_QueryOrgContractUsage(t *testing.T) {
    client, mock := newMockClient(t)
    defer client.db.Close()
    
    rows := sqlmock.NewRows([]string{
        "ORGANIZATION_NAME", "CONTRACT_NUMBER", "TOTAL_CREDITS_USED", "TOTAL_CREDITS_BILLED",
    }).AddRow("MY_ORG", 12345, 1000.5, 1200.0)
    
    mock.ExpectQuery("SELECT(.+)FROM SNOWFLAKE.ORGANIZATION_USAGE.CONTRACT_ITEMS").
        WillReturnRows(rows)
    
    metrics := &snowflakeMetrics{}
    err := client.queryOrgContractUsage(context.Background(), metrics)
    
    require.NoError(t, err)
    assert.Len(t, metrics.orgContractUsage, 1)
}

func TestClient_QueryMetrics_AllEnabled(t *testing.T) {
    client, mock := newMockClient(t)
    defer client.db.Close()
    
    // Enable all metrics
    client.config.Metrics = MetricsConfig{
        CurrentQueries:        MetricCategoryConfig{Enabled: true},
        WarehouseLoad:         MetricCategoryConfig{Enabled: true},
        QueryHistory:          MetricCategoryConfig{Enabled: true},
        CreditUsage:           MetricCategoryConfig{Enabled: true},
        StorageMetrics:        MetricCategoryConfig{Enabled: true},
        LoginHistory:          MetricCategoryConfig{Enabled: true},
        DataPipeline:          MetricCategoryConfig{Enabled: true},
        DatabaseStorage:       MetricCategoryConfig{Enabled: true},
        TaskHistory:           MetricCategoryConfig{Enabled: true},
        ReplicationUsage:      MetricCategoryConfig{Enabled: true},
        AutoClusteringHistory: MetricCategoryConfig{Enabled: true},
    }
    
    // Mock all queries
    mock.ExpectQuery("SELECT(.+)QUERY_HISTORY").WillReturnRows(sqlmock.NewRows([]string{"WAREHOUSE_NAME", "QUERY_TYPE", "EXECUTION_STATUS", "QUERY_COUNT", "AVG_EXECUTION_TIME", "AVG_BYTES_SCANNED"}))
    mock.ExpectQuery("SELECT(.+)WAREHOUSE_LOAD_HISTORY").WillReturnRows(sqlmock.NewRows([]string{"WAREHOUSE_NAME", "AVG_RUNNING", "AVG_QUEUED_LOAD", "AVG_QUEUED_PROVISIONING", "AVG_BLOCKED"}))
    mock.ExpectQuery("SELECT(.+)QUERY_HISTORY(.+)ACCOUNT_USAGE").WillReturnRows(sqlmock.NewRows([]string{"WAREHOUSE_NAME", "QUERY_TYPE", "EXECUTION_STATUS", "QUERY_COUNT", "AVG_EXECUTION_TIME", "AVG_BYTES_SCANNED", "AVG_BYTES_WRITTEN", "AVG_ROWS_PRODUCED", "AVG_COMPILATION_TIME"}))
    mock.ExpectQuery("SELECT(.+)WAREHOUSE_METERING_HISTORY").WillReturnRows(sqlmock.NewRows([]string{"WAREHOUSE_NAME", "TOTAL_CREDITS", "COMPUTE_CREDITS", "CLOUD_SERVICE_CREDITS"}))
    mock.ExpectQuery("SELECT(.+)STORAGE_USAGE").WillReturnRows(sqlmock.NewRows([]string{"TOTAL_STORAGE_BYTES", "STAGE_BYTES", "FAILSAFE_BYTES"}))
    mock.ExpectQuery("SELECT(.+)LOGIN_HISTORY").WillReturnRows(sqlmock.NewRows([]string{"IS_SUCCESS", "ERROR_CODE", "LOGIN_COUNT"}))
    mock.ExpectQuery("SELECT(.+)PIPE_USAGE_HISTORY").WillReturnRows(sqlmock.NewRows([]string{"PIPE_NAME", "TOTAL_CREDITS", "BYTES_INSERTED", "FILES_INSERTED"}))
    mock.ExpectQuery("SELECT(.+)DATABASE_STORAGE_USAGE_HISTORY").WillReturnRows(sqlmock.NewRows([]string{"DATABASE_NAME", "AVG_DATABASE_BYTES", "AVG_FAILSAFE_BYTES"}))
    mock.ExpectQuery("SELECT(.+)TASK_HISTORY").WillReturnRows(sqlmock.NewRows([]string{"DATABASE_NAME", "SCHEMA_NAME", "TASK_NAME", "STATE", "EXECUTION_COUNT", "AVG_EXECUTION_TIME"}))
    mock.ExpectQuery("SELECT(.+)REPLICATION_USAGE_HISTORY").WillReturnRows(sqlmock.NewRows([]string{"DATABASE_NAME", "TOTAL_CREDITS", "BYTES_TRANSFERRED"}))
    mock.ExpectQuery("SELECT(.+)AUTOMATIC_CLUSTERING_HISTORY").WillReturnRows(sqlmock.NewRows([]string{"DATABASE_NAME", "SCHEMA_NAME", "TABLE_NAME", "TOTAL_CREDITS", "BYTES_RECLUSTERED", "ROWS_RECLUSTERED"}))
    
    metrics, err := client.queryMetrics(context.Background())
    
    require.NoError(t, err)
    assert.NotNil(t, metrics)
}

func TestOrgStorageUsageColumns_AllGetters(t *testing.T) {
    cols := OrgStorageUsageColumns{}
    
    assert.Equal(t, "ORGANIZATION_NAME", cols.GetOrganizationName())
    assert.Equal(t, "ACCOUNT_NAME", cols.GetAccountName())
    assert.Equal(t, "AVERAGE_STORAGE_BYTES", cols.GetStorageBytes())
    assert.Equal(t, "AVERAGE_STAGE_BYTES", cols.GetStageBytes())
    assert.Equal(t, "AVERAGE_FAILSAFE_BYTES", cols.GetFailsafeBytes())
    assert.Equal(t, "USAGE_DATE", cols.GetUsageDate())
}

func TestOrgDataTransferColumns_AllGetters(t *testing.T) {
    cols := OrgDataTransferColumns{}
    
    assert.Equal(t, "ORGANIZATION_NAME", cols.GetOrganizationName())
    assert.Equal(t, "SOURCE_ACCOUNT_NAME", cols.GetSourceAccountName())
    assert.Equal(t, "TARGET_ACCOUNT_NAME", cols.GetTargetAccountName())
    assert.Equal(t, "SOURCE_REGION", cols.GetSourceRegion())
    assert.Equal(t, "TARGET_REGION", cols.GetTargetRegion())
    assert.Equal(t, "BYTES_TRANSFERRED", cols.GetBytesTransferred())
    assert.Equal(t, "TRANSFER_DATE", cols.GetTransferDate())
}

func TestOrgContractUsageColumns_AllGetters(t *testing.T) {
    cols := OrgContractUsageColumns{}
    
    assert.Equal(t, "ORGANIZATION_NAME", cols.GetOrganizationName())
    assert.Equal(t, "CONTRACT_NUMBER", cols.GetContractNumber())
    assert.Equal(t, "CREDITS_USED", cols.GetCreditsUsed())
    assert.Equal(t, "CREDITS_BILLED", cols.GetCreditsBilled())
    assert.Equal(t, "USAGE_DATE", cols.GetUsageDate())
}

func TestOrgCreditUsageColumns_AllGetters(t *testing.T) {
    cols := OrgCreditUsageColumns{}
    
    assert.Equal(t, "SERVICE_TYPE", cols.GetServiceType())
    assert.Equal(t, "USAGE_DATE", cols.GetUsageDate())
}

func TestOrgStorageUsageConfig_GetInterval(t *testing.T) {
    cfg := OrgStorageUsageConfig{Interval: "2h"}
    assert.Equal(t, 2*time.Hour, cfg.GetInterval(1*time.Hour))
}

func TestOrgDataTransferConfig_GetInterval(t *testing.T) {
    cfg := OrgDataTransferConfig{Interval: "3h"}
    interval := cfg.GetInterval(1 * time.Hour)
    assert.Equal(t, 3*time.Hour, interval)
}

func TestOrgContractUsageConfig_GetInterval(t *testing.T) {
    cfg := OrgContractUsageConfig{Interval: "6h"}
    interval := cfg.GetInterval(12 * time.Hour)
    assert.Equal(t, 6*time.Hour, interval)
}

func TestOrgCreditUsageConfig_GetInterval(t *testing.T) {
    cfg := OrgCreditUsageConfig{Interval: "30m"}
    interval := cfg.GetInterval(1 * time.Hour)
    assert.Equal(t, 30*time.Minute, interval)
}
