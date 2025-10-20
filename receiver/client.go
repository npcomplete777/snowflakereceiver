package snowflakereceiver

import (
    "context"
    "database/sql"
    "fmt"
    
    sf "github.com/snowflakedb/gosnowflake"
    "go.uber.org/zap"
)

type snowflakeClient struct {
    db     *sql.DB
    logger *zap.Logger
    config *Config
}

func newSnowflakeClient(logger *zap.Logger, config *Config) (*snowflakeClient, error) {
    dsn, err := buildDSN(config)
    if err != nil {
        return nil, fmt.Errorf("failed to build DSN: %w", err)
    }
    
    db, err := sql.Open("snowflake", dsn)
    if err != nil {
        return nil, fmt.Errorf("failed to open snowflake connection: %w", err)
    }
    
    db.SetMaxOpenConns(5)
    db.SetMaxIdleConns(2)
    
    return &snowflakeClient{
        db:     db,
        logger: logger,
        config: config,
    }, nil
}

func buildDSN(config *Config) (string, error) {
    cfg := &sf.Config{
        Account:   config.Account,
        User:      config.User,
        Password:  config.Password,
        Database:  config.Database,
        Schema:    config.Schema,
        Warehouse: config.Warehouse,
        Role:      config.Role,
    }
    
    dsn, err := sf.DSN(cfg)
    if err != nil {
        return "", fmt.Errorf("failed to create DSN: %w", err)
    }
    
    return dsn, nil
}

func (c *snowflakeClient) connect(ctx context.Context) error {
    return c.db.PingContext(ctx)
}

func (c *snowflakeClient) close() error {
    if c.db != nil {
        return c.db.Close()
    }
    return nil
}

// Query all metrics based on config
func (c *snowflakeClient) queryMetrics(ctx context.Context) (*snowflakeMetrics, error) {
    metrics := &snowflakeMetrics{}
    
    // INFORMATION_SCHEMA queries (REAL-TIME - no latency!)
    if c.config.Metrics.CurrentQueries {
        queries, err := c.queryCurrentQueries(ctx)
        if err != nil {
            c.logger.Warn("Failed to query current queries", zap.Error(err))
        } else {
            metrics.currentQueries = queries
        }
    }
    
    if c.config.Metrics.WarehouseLoad {
        load, err := c.queryWarehouseLoadHistory(ctx)
        if err != nil {
            c.logger.Warn("Failed to query warehouse load", zap.Error(err))
        } else {
            metrics.warehouseLoad = load
        }
    }
    
    // ACCOUNT_USAGE queries (45min-3hr latency)
    if c.config.Metrics.QueryHistory {
        stats, err := c.queryQueryStats(ctx)
        if err != nil {
            c.logger.Warn("Failed to query query stats", zap.Error(err))
        } else {
            metrics.queryStats = stats
        }
    }
    
    if c.config.Metrics.CreditUsage {
        credits, err := c.queryWarehouseCreditUsage(ctx)
        if err != nil {
            c.logger.Warn("Failed to query credit usage", zap.Error(err))
        } else {
            metrics.creditUsage = credits
        }
    }
    
    if c.config.Metrics.StorageMetrics {
        storage, err := c.queryStorageUsage(ctx)
        if err != nil {
            c.logger.Warn("Failed to query storage usage", zap.Error(err))
        } else {
            metrics.storageUsage = storage
        }
    }
    
    if c.config.Metrics.LoginHistory {
        logins, err := c.queryLoginHistory(ctx)
        if err != nil {
            c.logger.Warn("Failed to query login history", zap.Error(err))
        } else {
            metrics.loginHistory = logins
        }
    }
    
    if c.config.Metrics.DataPipeline {
        pipes, err := c.queryPipeUsage(ctx)
        if err != nil {
            c.logger.Warn("Failed to query pipe usage", zap.Error(err))
        } else {
            metrics.pipeUsage = pipes
        }
    }
    
    if c.config.Metrics.DatabaseStorage {
        dbStorage, err := c.queryDatabaseStorage(ctx)
        if err != nil {
            c.logger.Warn("Failed to query database storage", zap.Error(err))
        } else {
            metrics.databaseStorage = dbStorage
        }
    }
    
    if c.config.Metrics.TaskHistory {
        tasks, err := c.queryTaskHistory(ctx)
        if err != nil {
            c.logger.Warn("Failed to query task history", zap.Error(err))
        } else {
            metrics.taskHistory = tasks
        }
    }
    
    if c.config.Metrics.ReplicationUsage {
        replication, err := c.queryReplicationUsage(ctx)
        if err != nil {
            c.logger.Warn("Failed to query replication usage", zap.Error(err))
        } else {
            metrics.replicationUsage = replication
        }
    }
    
    if c.config.Metrics.AutoClusteringHistory {
        clustering, err := c.queryAutoClusteringHistory(ctx)
        if err != nil {
            c.logger.Warn("Failed to query auto-clustering history", zap.Error(err))
        } else {
            metrics.autoClusteringHistory = clustering
        }
    }
    
    return metrics, nil
}

// ========== INFORMATION_SCHEMA Queries (REAL-TIME!) ==========

func (c *snowflakeClient) queryCurrentQueries(ctx context.Context) ([]currentQueryRow, error) {
    // Table function for last 5 minutes of queries - REAL TIME!
    query := `
        SELECT 
            WAREHOUSE_NAME,
            QUERY_TYPE,
            EXECUTION_STATUS,
            COUNT(*) as QUERY_COUNT,
            AVG(TOTAL_ELAPSED_TIME) as AVG_EXECUTION_TIME,
            AVG(BYTES_SCANNED) as AVG_BYTES_SCANNED
        FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
            END_TIME_RANGE_START => DATEADD('minutes', -5, CURRENT_TIMESTAMP())
        ))
        GROUP BY WAREHOUSE_NAME, QUERY_TYPE, EXECUTION_STATUS
    `
    
    rows, err := c.db.QueryContext(ctx, query)
    if err != nil {
        return nil, fmt.Errorf("failed to execute current queries query: %w", err)
    }
    defer rows.Close()
    
    var results []currentQueryRow
    for rows.Next() {
        var r currentQueryRow
        err := rows.Scan(
            &r.warehouseName,
            &r.queryType,
            &r.executionStatus,
            &r.queryCount,
            &r.avgExecutionTime,
            &r.avgBytesScanned,
        )
        if err != nil {
            c.logger.Warn("Failed to scan current query row", zap.Error(err))
            continue
        }
        results = append(results, r)
    }
    
    return results, rows.Err()
}

func (c *snowflakeClient) queryWarehouseLoadHistory(ctx context.Context) ([]warehouseLoadRow, error) {
    // Warehouse load for last 5 minutes - NEAR REAL TIME
    // Note: Cannot use parameterized query with table functions, must concatenate
    query := fmt.Sprintf(`
        SELECT 
            WAREHOUSE_NAME,
            AVG_RUNNING,
            AVG_QUEUED_LOAD,
            AVG_QUEUED_PROVISIONING,
            AVG_BLOCKED
        FROM TABLE(INFORMATION_SCHEMA.WAREHOUSE_LOAD_HISTORY(
            WAREHOUSE_NAME => '%s',
            DATE_RANGE_START => DATEADD('minutes', -5, CURRENT_TIMESTAMP())
        ))
    `, c.config.Warehouse)
    
    rows, err := c.db.QueryContext(ctx, query)
    if err != nil {
        return nil, fmt.Errorf("failed to execute warehouse load query: %w", err)
    }
    defer rows.Close()
    
    var results []warehouseLoadRow
    for rows.Next() {
        var r warehouseLoadRow
        err := rows.Scan(
            &r.warehouseName,
            &r.avgRunning,
            &r.avgQueuedLoad,
            &r.avgQueuedProvisioning,
            &r.avgBlocked,
        )
        if err != nil {
            c.logger.Warn("Failed to scan warehouse load row", zap.Error(err))
            continue
        }
        results = append(results, r)
    }
    
    return results, rows.Err()
}

// ========== ACCOUNT_USAGE Queries (Historical) ==========

func (c *snowflakeClient) queryQueryStats(ctx context.Context) ([]queryStatRow, error) {
    query := `
        SELECT 
            WAREHOUSE_NAME,
            QUERY_TYPE,
            EXECUTION_STATUS,
            COUNT(*) as QUERY_COUNT,
            AVG(TOTAL_ELAPSED_TIME) as AVG_EXECUTION_TIME,
            AVG(BYTES_SCANNED) as AVG_BYTES_SCANNED,
            AVG(BYTES_WRITTEN) as AVG_BYTES_WRITTEN,
            AVG(BYTES_DELETED) as AVG_BYTES_DELETED,
            AVG(ROWS_PRODUCED) as AVG_ROWS_PRODUCED,
            AVG(COMPILATION_TIME) as AVG_COMPILATION_TIME
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
        GROUP BY WAREHOUSE_NAME, QUERY_TYPE, EXECUTION_STATUS
    `
    
    rows, err := c.db.QueryContext(ctx, query)
    if err != nil {
        return nil, fmt.Errorf("failed to execute query stats query: %w", err)
    }
    defer rows.Close()
    
    var results []queryStatRow
    for rows.Next() {
        var r queryStatRow
        err := rows.Scan(
            &r.warehouseName,
            &r.queryType,
            &r.executionStatus,
            &r.queryCount,
            &r.avgExecutionTime,
            &r.avgBytesScanned,
            &r.avgBytesWritten,
            &r.avgBytesDeleted,
            &r.avgRowsProduced,
            &r.avgCompilationTime,
        )
        if err != nil {
            c.logger.Warn("Failed to scan query stat row", zap.Error(err))
            continue
        }
        results = append(results, r)
    }
    
    return results, rows.Err()
}

func (c *snowflakeClient) queryWarehouseCreditUsage(ctx context.Context) ([]creditUsageRow, error) {
    query := `
        SELECT 
            WAREHOUSE_NAME,
            SUM(CREDITS_USED) as TOTAL_CREDITS,
            SUM(CREDITS_USED_COMPUTE) as COMPUTE_CREDITS,
            SUM(CREDITS_USED_CLOUD_SERVICES) as CLOUD_SERVICE_CREDITS
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
        WHERE START_TIME >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
        GROUP BY WAREHOUSE_NAME
    `
    
    rows, err := c.db.QueryContext(ctx, query)
    if err != nil {
        return nil, fmt.Errorf("failed to execute credit usage query: %w", err)
    }
    defer rows.Close()
    
    var results []creditUsageRow
    for rows.Next() {
        var r creditUsageRow
        err := rows.Scan(&r.warehouseName, &r.totalCredits, &r.computeCredits, &r.cloudServiceCredits)
        if err != nil {
            c.logger.Warn("Failed to scan credit usage row", zap.Error(err))
            continue
        }
        results = append(results, r)
    }
    
    return results, rows.Err()
}

func (c *snowflakeClient) queryStorageUsage(ctx context.Context) ([]storageUsageRow, error) {
    query := `
        SELECT 
            USAGE_DATE,
            AVG(STORAGE_BYTES) as TOTAL_STORAGE_BYTES,
            AVG(STAGE_BYTES) as STAGE_BYTES,
            AVG(FAILSAFE_BYTES) as FAILSAFE_BYTES
        FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE
        WHERE USAGE_DATE >= DATEADD(day, -1, CURRENT_DATE())
        GROUP BY USAGE_DATE
        ORDER BY USAGE_DATE DESC
        LIMIT 1
    `
    
    rows, err := c.db.QueryContext(ctx, query)
    if err != nil {
        return nil, fmt.Errorf("failed to execute storage usage query: %w", err)
    }
    defer rows.Close()
    
    var results []storageUsageRow
    for rows.Next() {
        var r storageUsageRow
        err := rows.Scan(&r.usageDate, &r.totalStorageBytes, &r.stageBytes, &r.failsafeBytes)
        if err != nil {
            c.logger.Warn("Failed to scan storage usage row", zap.Error(err))
            continue
        }
        results = append(results, r)
    }
    
    return results, rows.Err()
}

func (c *snowflakeClient) queryLoginHistory(ctx context.Context) ([]loginHistoryRow, error) {
    query := `
        SELECT 
            IS_SUCCESS,
            ERROR_CODE,
            COUNT(*) as LOGIN_COUNT
        FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
        WHERE EVENT_TIMESTAMP >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
        GROUP BY IS_SUCCESS, ERROR_CODE
    `
    
    rows, err := c.db.QueryContext(ctx, query)
    if err != nil {
        return nil, fmt.Errorf("failed to execute login history query: %w", err)
    }
    defer rows.Close()
    
    var results []loginHistoryRow
    for rows.Next() {
        var r loginHistoryRow
        err := rows.Scan(&r.isSuccess, &r.errorCode, &r.loginCount)
        if err != nil {
            c.logger.Warn("Failed to scan login history row", zap.Error(err))
            continue
        }
        results = append(results, r)
    }
    
    return results, rows.Err()
}

func (c *snowflakeClient) queryPipeUsage(ctx context.Context) ([]pipeUsageRow, error) {
    query := `
        SELECT 
            PIPE_NAME,
            SUM(CREDITS_USED) as TOTAL_CREDITS,
            SUM(BYTES_INSERTED) as TOTAL_BYTES_INSERTED,
            SUM(FILES_INSERTED) as TOTAL_FILES_INSERTED
        FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
        WHERE START_TIME >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
        GROUP BY PIPE_NAME
    `
    
    rows, err := c.db.QueryContext(ctx, query)
    if err != nil {
        return nil, fmt.Errorf("failed to execute pipe usage query: %w", err)
    }
    defer rows.Close()
    
    var results []pipeUsageRow
    for rows.Next() {
        var r pipeUsageRow
        err := rows.Scan(&r.pipeName, &r.totalCredits, &r.bytesInserted, &r.filesInserted)
        if err != nil {
            c.logger.Warn("Failed to scan pipe usage row", zap.Error(err))
            continue
        }
        results = append(results, r)
    }
    
    return results, rows.Err()
}

func (c *snowflakeClient) queryDatabaseStorage(ctx context.Context) ([]databaseStorageRow, error) {
    query := `
        SELECT 
            DATABASE_NAME,
            AVG(AVERAGE_DATABASE_BYTES) as AVG_DATABASE_BYTES,
            AVG(AVERAGE_FAILSAFE_BYTES) as AVG_FAILSAFE_BYTES
        FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY
        WHERE USAGE_DATE >= DATEADD(day, -1, CURRENT_DATE())
        GROUP BY DATABASE_NAME
    `
    
    rows, err := c.db.QueryContext(ctx, query)
    if err != nil {
        return nil, fmt.Errorf("failed to execute database storage query: %w", err)
    }
    defer rows.Close()
    
    var results []databaseStorageRow
    for rows.Next() {
        var r databaseStorageRow
        err := rows.Scan(&r.databaseName, &r.avgDatabaseBytes, &r.avgFailsafeBytes)
        if err != nil {
            c.logger.Warn("Failed to scan database storage row", zap.Error(err))
            continue
        }
        results = append(results, r)
    }
    
    return results, rows.Err()
}

func (c *snowflakeClient) queryTaskHistory(ctx context.Context) ([]taskHistoryRow, error) {
    // Fixed: Use COUNT for execution count, don't use SUM on timestamps
    query := `
        SELECT 
            DATABASE_NAME,
            SCHEMA_NAME,
            NAME as TASK_NAME,
            STATE,
            COUNT(*) as EXECUTION_COUNT,
            AVG(DATEDIFF('millisecond', SCHEDULED_TIME, QUERY_START_TIME)) as AVG_SCHEDULED_DELAY_MS
        FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
        WHERE QUERY_START_TIME >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
        GROUP BY DATABASE_NAME, SCHEMA_NAME, NAME, STATE
    `
    
    rows, err := c.db.QueryContext(ctx, query)
    if err != nil {
        return nil, fmt.Errorf("failed to execute task history query: %w", err)
    }
    defer rows.Close()
    
    var results []taskHistoryRow
    for rows.Next() {
        var r taskHistoryRow
        err := rows.Scan(
            &r.databaseName,
            &r.schemaName,
            &r.taskName,
            &r.state,
            &r.executionCount,
            &r.avgScheduledTime,
        )
        if err != nil {
            c.logger.Warn("Failed to scan task history row", zap.Error(err))
            continue
        }
        results = append(results, r)
    }
    
    return results, rows.Err()
}

func (c *snowflakeClient) queryReplicationUsage(ctx context.Context) ([]replicationUsageRow, error) {
    query := `
        SELECT 
            DATABASE_NAME,
            SUM(CREDITS_USED) as TOTAL_CREDITS,
            SUM(BYTES_TRANSFERRED) as TOTAL_BYTES_TRANSFERRED
        FROM SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_USAGE_HISTORY
        WHERE START_TIME >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
        GROUP BY DATABASE_NAME
    `
    
    rows, err := c.db.QueryContext(ctx, query)
    if err != nil {
        return nil, fmt.Errorf("failed to execute replication usage query: %w", err)
    }
    defer rows.Close()
    
    var results []replicationUsageRow
    for rows.Next() {
        var r replicationUsageRow
        err := rows.Scan(&r.databaseName, &r.totalCredits, &r.bytesTransferred)
        if err != nil {
            c.logger.Warn("Failed to scan replication usage row", zap.Error(err))
            continue
        }
        results = append(results, r)
    }
    
    return results, rows.Err()
}

func (c *snowflakeClient) queryAutoClusteringHistory(ctx context.Context) ([]autoClusteringRow, error) {
    query := `
        SELECT 
            DATABASE_NAME,
            SCHEMA_NAME,
            TABLE_NAME,
            SUM(CREDITS_USED) as TOTAL_CREDITS,
            SUM(NUM_BYTES_RECLUSTERED) as TOTAL_BYTES_RECLUSTERED,
            SUM(NUM_ROWS_RECLUSTERED) as TOTAL_ROWS_RECLUSTERED
        FROM SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY
        WHERE START_TIME >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
        GROUP BY DATABASE_NAME, SCHEMA_NAME, TABLE_NAME
    `
    
    rows, err := c.db.QueryContext(ctx, query)
    if err != nil {
        return nil, fmt.Errorf("failed to execute auto-clustering history query: %w", err)
    }
    defer rows.Close()
    
    var results []autoClusteringRow
    for rows.Next() {
        var r autoClusteringRow
        err := rows.Scan(
            &r.databaseName,
            &r.schemaName,
            &r.tableName,
            &r.totalCredits,
            &r.bytesReclustered,
            &r.rowsReclustered,
        )
        if err != nil {
            c.logger.Warn("Failed to scan auto-clustering row", zap.Error(err))
            continue
        }
        results = append(results, r)
    }
    
    return results, rows.Err()
}

// Data structures
type snowflakeMetrics struct {
    // INFORMATION_SCHEMA (real-time)
    currentQueries []currentQueryRow
    warehouseLoad  []warehouseLoadRow
    
    // ACCOUNT_USAGE (historical)
    queryStats            []queryStatRow
    creditUsage           []creditUsageRow
    storageUsage          []storageUsageRow
    loginHistory          []loginHistoryRow
    pipeUsage             []pipeUsageRow
    databaseStorage       []databaseStorageRow
    taskHistory           []taskHistoryRow
    replicationUsage      []replicationUsageRow
    autoClusteringHistory []autoClusteringRow
}

// INFORMATION_SCHEMA row types
type currentQueryRow struct {
    warehouseName    sql.NullString
    queryType        sql.NullString
    executionStatus  sql.NullString
    queryCount       int64
    avgExecutionTime sql.NullFloat64
    avgBytesScanned  sql.NullFloat64
}

type warehouseLoadRow struct {
    warehouseName         sql.NullString
    avgRunning            sql.NullFloat64
    avgQueuedLoad         sql.NullFloat64
    avgQueuedProvisioning sql.NullFloat64
    avgBlocked            sql.NullFloat64
}

// ACCOUNT_USAGE row types
type queryStatRow struct {
    warehouseName      sql.NullString
    queryType          sql.NullString
    executionStatus    sql.NullString
    queryCount         int64
    avgExecutionTime   sql.NullFloat64
    avgBytesScanned    sql.NullFloat64
    avgBytesWritten    sql.NullFloat64
    avgBytesDeleted    sql.NullFloat64
    avgRowsProduced    sql.NullFloat64
    avgCompilationTime sql.NullFloat64
}

type creditUsageRow struct {
    warehouseName       sql.NullString
    totalCredits        float64
    computeCredits      sql.NullFloat64
    cloudServiceCredits sql.NullFloat64
}

type storageUsageRow struct {
    usageDate         sql.NullTime
    totalStorageBytes sql.NullFloat64
    stageBytes        sql.NullFloat64
    failsafeBytes     sql.NullFloat64
}

type loginHistoryRow struct {
    isSuccess  string
    errorCode  sql.NullString
    loginCount int64
}

type pipeUsageRow struct {
    pipeName      sql.NullString
    totalCredits  float64
    bytesInserted sql.NullFloat64
    filesInserted sql.NullInt64
}

type databaseStorageRow struct {
    databaseName     sql.NullString
    avgDatabaseBytes sql.NullFloat64
    avgFailsafeBytes sql.NullFloat64
}

type taskHistoryRow struct {
    databaseName      sql.NullString
    schemaName        sql.NullString
    taskName          sql.NullString
    state             sql.NullString
    executionCount    int64
    avgScheduledTime  sql.NullFloat64
}

type replicationUsageRow struct {
    databaseName      sql.NullString
    totalCredits      float64
    bytesTransferred  sql.NullFloat64
}

type autoClusteringRow struct {
    databaseName      sql.NullString
    schemaName        sql.NullString
    tableName         sql.NullString
    totalCredits      float64
    bytesReclustered  sql.NullFloat64
    rowsReclustered   sql.NullFloat64
}
