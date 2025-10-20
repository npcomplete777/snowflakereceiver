package snowflakereceiver

import (
    "context"
    "database/sql"
    "testing"
    
    "github.com/DATA-DOG/go-sqlmock"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
    "go.uber.org/zap"
)

func newMockClient(t *testing.T) (*snowflakeClient, sqlmock.Sqlmock) {
    db, mock, err := sqlmock.New()
    require.NoError(t, err)
    
    config := &Config{
        User:      "test",
        Password:  "test",
        Account:   "test-account",
        Warehouse: "TEST_WH",
        Database:  "SNOWFLAKE",
        Schema:    "ACCOUNT_USAGE",
    }
    
    client := &snowflakeClient{
        logger:       zap.NewNop(),
        config:       config,
        db:           db,
        queryTimeout: defaultQueryTimeout,
    }
    
    return client, mock
}

func TestNewSnowflakeClient(t *testing.T) {
    config := &Config{
        User:      "test",
        Password:  "test",
        Account:   "test-account",
        Warehouse: "TEST_WH",
    }
    
    client, err := newSnowflakeClient(zap.NewNop(), config)
    
    require.NoError(t, err)
    assert.NotNil(t, client)
    assert.Equal(t, config, client.config)
}

func TestClient_QueryCurrentQueries(t *testing.T) {
    client, mock := newMockClient(t)
    defer client.db.Close()
    
    rows := sqlmock.NewRows([]string{
        "WAREHOUSE_NAME", "QUERY_TYPE", "EXECUTION_STATUS",
        "QUERY_COUNT", "AVG_EXECUTION_TIME", "AVG_BYTES_SCANNED",
    }).AddRow("WH1", "SELECT", "SUCCESS", 10, 1500.0, 1024000.0)
    
    mock.ExpectQuery("SELECT(.+)FROM TABLE\\(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY\\(\\)\\)(.+)LIMIT").
        WithArgs(maxRowsPerQuery).
        WillReturnRows(rows)
    
    metrics := &snowflakeMetrics{}
    err := client.queryCurrentQueries(context.Background(), metrics)
    
    require.NoError(t, err)
    assert.Len(t, metrics.currentQueries, 1)
    assert.Equal(t, "WH1", metrics.currentQueries[0].warehouseName.String)
    assert.Equal(t, int64(10), metrics.currentQueries[0].queryCount)
}

func TestClient_QueryWarehouseLoad(t *testing.T) {
    client, mock := newMockClient(t)
    defer client.db.Close()
    
    rows := sqlmock.NewRows([]string{
        "WAREHOUSE_NAME", "AVG_RUNNING", "AVG_QUEUED_LOAD",
        "AVG_QUEUED_PROVISIONING", "AVG_BLOCKED",
    }).AddRow("WH1", 5.5, 2.0, 0.5, 0.0)
    
    mock.ExpectQuery("SELECT(.+)FROM TABLE\\(SNOWFLAKE.INFORMATION_SCHEMA.WAREHOUSE_LOAD_HISTORY\\(\\)\\)(.+)LIMIT").
        WithArgs(maxRowsPerQuery).
        WillReturnRows(rows)
    
    metrics := &snowflakeMetrics{}
    err := client.queryWarehouseLoad(context.Background(), metrics)
    
    require.NoError(t, err)
    assert.Len(t, metrics.warehouseLoad, 1)
    assert.Equal(t, 5.5, metrics.warehouseLoad[0].avgRunning.Float64)
}

func TestClient_QueryQueryHistory(t *testing.T) {
    client, mock := newMockClient(t)
    defer client.db.Close()
    
    rows := sqlmock.NewRows([]string{
        "WAREHOUSE_NAME", "QUERY_TYPE", "EXECUTION_STATUS", "QUERY_COUNT",
        "AVG_EXECUTION_TIME", "AVG_BYTES_SCANNED", "AVG_BYTES_WRITTEN",
        "AVG_ROWS_PRODUCED", "AVG_COMPILATION_TIME",
    }).AddRow("WH1", "SELECT", "SUCCESS", 100, 2500.0, 1024000.0, 512000.0, 1000.0, 100.0)
    
    mock.ExpectQuery("SELECT(.+)FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY(.+)LIMIT").
        WithArgs(maxRowsPerQuery).
        WillReturnRows(rows)
    
    metrics := &snowflakeMetrics{}
    err := client.queryQueryHistory(context.Background(), metrics)
    
    require.NoError(t, err)
    assert.Len(t, metrics.queryStats, 1)
    assert.Equal(t, int64(100), metrics.queryStats[0].queryCount)
}

func TestClient_QueryCreditUsage(t *testing.T) {
    client, mock := newMockClient(t)
    defer client.db.Close()
    
    rows := sqlmock.NewRows([]string{
        "WAREHOUSE_NAME", "TOTAL_CREDITS", "COMPUTE_CREDITS", "CLOUD_SERVICE_CREDITS",
    }).AddRow("WH1", 10.5, 8.0, 2.5)
    
    mock.ExpectQuery("SELECT(.+)FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY(.+)LIMIT").
        WithArgs(maxRowsPerQuery).
        WillReturnRows(rows)
    
    metrics := &snowflakeMetrics{}
    err := client.queryCreditUsage(context.Background(), metrics)
    
    require.NoError(t, err)
    assert.Len(t, metrics.creditUsage, 1)
    assert.Equal(t, 10.5, metrics.creditUsage[0].totalCredits)
}

func TestClient_QueryStorageUsage(t *testing.T) {
    client, mock := newMockClient(t)
    defer client.db.Close()
    
    rows := sqlmock.NewRows([]string{
        "TOTAL_STORAGE_BYTES", "STAGE_BYTES", "FAILSAFE_BYTES",
    }).AddRow(1024000000.0, 102400000.0, 10240000.0)
    
    mock.ExpectQuery("SELECT(.+)FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE").
        WillReturnRows(rows)
    
    metrics := &snowflakeMetrics{}
    err := client.queryStorageUsage(context.Background(), metrics)
    
    require.NoError(t, err)
    assert.Len(t, metrics.storageUsage, 1)
    assert.Equal(t, 1024000000.0, metrics.storageUsage[0].totalStorageBytes.Float64)
}

func TestClient_QueryLoginHistory(t *testing.T) {
    client, mock := newMockClient(t)
    defer client.db.Close()
    
    rows := sqlmock.NewRows([]string{
        "IS_SUCCESS", "ERROR_CODE", "LOGIN_COUNT",
    }).
        AddRow("YES", sql.NullString{}, 100).
        AddRow("NO", "390201", 5)
    
    mock.ExpectQuery("SELECT(.+)FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY(.+)LIMIT").
        WithArgs(maxRowsPerQuery).
        WillReturnRows(rows)
    
    metrics := &snowflakeMetrics{}
    err := client.queryLoginHistory(context.Background(), metrics)
    
    require.NoError(t, err)
    assert.Len(t, metrics.loginHistory, 2)
    assert.Equal(t, "YES", metrics.loginHistory[0].isSuccess)
    assert.Equal(t, int64(100), metrics.loginHistory[0].loginCount)
}

func TestClient_QueryPipeUsage(t *testing.T) {
    client, mock := newMockClient(t)
    defer client.db.Close()
    
    rows := sqlmock.NewRows([]string{
        "PIPE_NAME", "TOTAL_CREDITS", "BYTES_INSERTED", "FILES_INSERTED",
    }).AddRow("MY_PIPE", 5.25, 10240000.0, 100)
    
    mock.ExpectQuery("SELECT(.+)FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY(.+)LIMIT").
        WithArgs(maxRowsPerQuery).
        WillReturnRows(rows)
    
    metrics := &snowflakeMetrics{}
    err := client.queryPipeUsage(context.Background(), metrics)
    
    require.NoError(t, err)
    assert.Len(t, metrics.pipeUsage, 1)
    assert.Equal(t, 5.25, metrics.pipeUsage[0].totalCredits)
}

func TestClient_QueryDatabaseStorage(t *testing.T) {
    client, mock := newMockClient(t)
    defer client.db.Close()
    
    rows := sqlmock.NewRows([]string{
        "DATABASE_NAME", "AVG_DATABASE_BYTES", "AVG_FAILSAFE_BYTES",
    }).AddRow("MY_DB", 1024000.0, 102400.0)
    
    mock.ExpectQuery("SELECT(.+)FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY(.+)LIMIT").
        WithArgs(maxRowsPerQuery).
        WillReturnRows(rows)
    
    metrics := &snowflakeMetrics{}
    err := client.queryDatabaseStorage(context.Background(), metrics)
    
    require.NoError(t, err)
    assert.Len(t, metrics.databaseStorage, 1)
    assert.Equal(t, "MY_DB", metrics.databaseStorage[0].databaseName.String)
}

func TestClient_QueryTaskHistory(t *testing.T) {
    client, mock := newMockClient(t)
    defer client.db.Close()
    
    rows := sqlmock.NewRows([]string{
        "DATABASE_NAME", "SCHEMA_NAME", "TASK_NAME", "STATE",
        "EXECUTION_COUNT", "AVG_EXECUTION_TIME",
    }).AddRow("MY_DB", "MY_SCHEMA", "MY_TASK", "SUCCEEDED", 50, 5000.0)
    
    mock.ExpectQuery("SELECT(.+)FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY(.+)LIMIT").
        WithArgs(maxRowsPerQuery).
        WillReturnRows(rows)
    
    metrics := &snowflakeMetrics{}
    err := client.queryTaskHistory(context.Background(), metrics)
    
    require.NoError(t, err)
    assert.Len(t, metrics.taskHistory, 1)
    assert.Equal(t, int64(50), metrics.taskHistory[0].executionCount)
}

func TestClient_QueryReplicationUsage(t *testing.T) {
    client, mock := newMockClient(t)
    defer client.db.Close()
    
    rows := sqlmock.NewRows([]string{
        "DATABASE_NAME", "TOTAL_CREDITS", "BYTES_TRANSFERRED",
    }).AddRow("MY_DB", 3.5, 10240000.0)
    
    mock.ExpectQuery("SELECT(.+)FROM SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_USAGE_HISTORY(.+)LIMIT").
        WithArgs(maxRowsPerQuery).
        WillReturnRows(rows)
    
    metrics := &snowflakeMetrics{}
    err := client.queryReplicationUsage(context.Background(), metrics)
    
    require.NoError(t, err)
    assert.Len(t, metrics.replicationUsage, 1)
    assert.Equal(t, 3.5, metrics.replicationUsage[0].totalCredits)
}

func TestClient_QueryAutoClusteringHistory(t *testing.T) {
    client, mock := newMockClient(t)
    defer client.db.Close()
    
    rows := sqlmock.NewRows([]string{
        "DATABASE_NAME", "SCHEMA_NAME", "TABLE_NAME", "TOTAL_CREDITS",
        "BYTES_RECLUSTERED", "ROWS_RECLUSTERED",
    }).AddRow("MY_DB", "MY_SCHEMA", "MY_TABLE", 2.5, 10240000.0, 100000.0)
    
    mock.ExpectQuery("SELECT(.+)FROM SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY(.+)LIMIT").
        WithArgs(maxRowsPerQuery).
        WillReturnRows(rows)
    
    metrics := &snowflakeMetrics{}
    err := client.queryAutoClusteringHistory(context.Background(), metrics)
    
    require.NoError(t, err)
    assert.Len(t, metrics.autoClusteringHistory, 1)
    assert.Equal(t, 2.5, metrics.autoClusteringHistory[0].totalCredits)
}

func TestClient_ExecuteCustomQuery(t *testing.T) {
    client, mock := newMockClient(t)
    defer client.db.Close()
    
    query := &CustomQuery{
        Name:         "test_query",
        ValueColumn:  "METRIC_VALUE",
        LabelColumns: []string{"LABEL1"},
        SQL:          "SELECT 'test' as LABEL1, 42.0 as METRIC_VALUE",
    }
    
    rows := sqlmock.NewRows([]string{"LABEL1", "METRIC_VALUE"}).
        AddRow("test", 42.0)
    
    mock.ExpectQuery("SELECT 'test' as LABEL1, 42.0 as METRIC_VALUE").
        WillReturnRows(rows)
    
    metrics := &snowflakeMetrics{}
    err := client.executeCustomQuery(context.Background(), metrics, query)
    
    require.NoError(t, err)
    assert.Len(t, metrics.customQueryResults, 1)
    assert.Equal(t, "test_query", metrics.customQueryResults[0].name)
    assert.Len(t, metrics.customQueryResults[0].rows, 1)
}

func TestClient_QueryMetrics_ErrorHandling(t *testing.T) {
    client, mock := newMockClient(t)
    defer client.db.Close()
    
    client.config.Metrics.CurrentQueries.Enabled = true
    client.config.Metrics.CreditUsage.Enabled = true
    
    // Current queries will fail
    mock.ExpectQuery("SELECT(.+)FROM TABLE\\(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY\\(\\)\\)").
        WillReturnError(sql.ErrNoRows)
    
    // Credit usage will succeed
    creditRows := sqlmock.NewRows([]string{
        "WAREHOUSE_NAME", "TOTAL_CREDITS", "COMPUTE_CREDITS", "CLOUD_SERVICE_CREDITS",
    }).AddRow("WH1", 10.5, 8.0, 2.5)
    
    mock.ExpectQuery("SELECT(.+)FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY").
        WithArgs(maxRowsPerQuery).
        WillReturnRows(creditRows)
    
    metrics, err := client.queryMetrics(context.Background())
    
    // Should not error - graceful degradation
    require.NoError(t, err)
    assert.Len(t, metrics.currentQueries, 0)
    assert.Len(t, metrics.creditUsage, 1)
}

func TestClient_QueryMetrics_AllDisabled(t *testing.T) {
    client, mock := newMockClient(t)
    defer client.db.Close()
    
    // All metrics disabled
    client.config.Metrics = MetricsConfig{}
    
    metrics, err := client.queryMetrics(context.Background())
    
    require.NoError(t, err)
    assert.NotNil(t, metrics)
    assert.Len(t, metrics.currentQueries, 0)
    assert.Len(t, metrics.creditUsage, 0)
    
    // Verify no queries were executed
    assert.NoError(t, mock.ExpectationsWereMet())
}
