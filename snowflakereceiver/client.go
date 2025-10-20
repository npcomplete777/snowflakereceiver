package snowflakereceiver

import (
    "context"
    "database/sql"
    "fmt"
    "math"
    "regexp"
    "time"
    
    _ "github.com/snowflakedb/gosnowflake"
    "go.uber.org/zap"
    "golang.org/x/time/rate"
)

// Table/column name whitelist regex (alphanumeric, underscore, dot)
var identifierRegex = regexp.MustCompile(`^[a-zA-Z0-9_\.]+$`)

type snowflakeClient struct {
    logger       *zap.Logger
    config       *Config
    db           *sql.DB
    rateLimiter  *rate.Limiter
    
    // Self-monitoring metrics
    queryCount   int64
    errorCount   int64
    retryCount   int64
}

type snowflakeMetrics struct {
    currentQueries        []currentQueryRow
    warehouseLoad         []warehouseLoadRow
    queryStats            []queryStatRow
    creditUsage           []creditUsageRow
    storageUsage          []storageUsageRow
    loginHistory          []loginHistoryRow
    pipeUsage             []pipeUsageRow
    databaseStorage       []databaseStorageRow
    taskHistory           []taskHistoryRow
    replicationUsage      []replicationUsageRow
    autoClusteringHistory []autoClusteringRow
    eventQueryLogs        []eventTableRow
    eventTaskLogs         []eventTableRow
    eventFunctionLogs     []eventTableRow
    eventProcedureLogs    []eventTableRow
    orgCreditUsage        []orgCreditUsageRow
    orgStorageUsage       []orgStorageUsageRow
    orgDataTransfer       []orgDataTransferRow
    orgContractUsage      []orgContractUsageRow
    customQueryResults    []customQueryResult
}

type currentQueryRow struct {
    warehouseName    sql.NullString
    queryType        sql.NullString
    executionStatus  sql.NullString
    queryCount       int64
    avgExecutionTime sql.NullFloat64
    avgBytesScanned  sql.NullFloat64
}

type warehouseLoadRow struct {
    warehouseName          sql.NullString
    avgRunning             sql.NullFloat64
    avgQueuedLoad          sql.NullFloat64
    avgQueuedProvisioning  sql.NullFloat64
    avgBlocked             sql.NullFloat64
}

type queryStatRow struct {
    warehouseName      sql.NullString
    queryType          sql.NullString
    executionStatus    sql.NullString
    queryCount         int64
    avgExecutionTime   sql.NullFloat64
    avgBytesScanned    sql.NullFloat64
    avgBytesWritten    sql.NullFloat64
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
    databaseName      sql.NullString
    avgDatabaseBytes  sql.NullFloat64
    avgFailsafeBytes  sql.NullFloat64
}

type taskHistoryRow struct {
    databaseName      sql.NullString
    schemaName        sql.NullString
    taskName          sql.NullString
    state             sql.NullString
    executionCount    int64
    avgExecutionTime  sql.NullFloat64
}

type replicationUsageRow struct {
    databaseName     sql.NullString
    totalCredits     float64
    bytesTransferred sql.NullFloat64
}

type autoClusteringRow struct {
    databaseName      sql.NullString
    schemaName        sql.NullString
    tableName         sql.NullString
    totalCredits      float64
    bytesReclustered  sql.NullFloat64
    rowsReclustered   sql.NullFloat64
}

type eventTableRow struct {
    eventType    string
    severity     sql.NullString
    errorMessage sql.NullString
}

type orgCreditUsageRow struct {
    organizationName sql.NullString
    accountName      sql.NullString
    serviceType      sql.NullString
    totalCredits     float64
}

type orgStorageUsageRow struct {
    organizationName sql.NullString
    accountName      sql.NullString
    avgStorageBytes  sql.NullFloat64
    avgStageBytes    sql.NullFloat64
    avgFailsafeBytes sql.NullFloat64
}

type orgDataTransferRow struct {
    organizationName      sql.NullString
    sourceAccountName     sql.NullString
    targetAccountName     sql.NullString
    sourceRegion          sql.NullString
    targetRegion          sql.NullString
    totalBytesTransferred sql.NullFloat64
}

type orgContractUsageRow struct {
    organizationName  sql.NullString
    contractNumber    sql.NullInt64
    totalCreditsUsed  float64
    totalCreditsBilled sql.NullFloat64
}

type customQueryResult struct {
    name       string
    metricType string
    rows       []map[string]interface{}
}

// validateIdentifier validates table/column names against SQL injection
func validateIdentifier(identifier string) error {
    if !identifierRegex.MatchString(identifier) {
        return fmt.Errorf("invalid identifier: %s (must be alphanumeric with underscores/dots only)", identifier)
    }
    return nil
}

func newSnowflakeClient(logger *zap.Logger, config *Config) (*snowflakeClient, error) {
    // Create rate limiter based on config
    qps := config.GetRateLimitQPS()
    limiter := rate.NewLimiter(rate.Limit(qps), qps)
    
    return &snowflakeClient{
        logger:      logger,
        config:      config,
        rateLimiter: limiter,
    }, nil
}

// Close closes the database connection
func (c *snowflakeClient) Close() error {
    if c.db != nil {
        c.logger.Info("Closing Snowflake connection",
            zap.Int64("total_queries", c.queryCount),
            zap.Int64("total_errors", c.errorCount),
            zap.Int64("total_retries", c.retryCount))
        return c.db.Close()
    }
    return nil
}

func (c *snowflakeClient) connect(ctx context.Context) error {
    if c.db != nil {
        return nil
    }
    
    // Add connection timeout to context
    connectCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
    defer cancel()
    
    // Build actual DSN with password (for connection only, never logged)
    dsn := fmt.Sprintf("%s:%s@%s/%s/%s?warehouse=%s",
        c.config.User,
        c.config.Password,
        c.config.Account,
        c.config.Database,
        c.config.Schema,
        c.config.Warehouse,
    )
    
    db, err := sql.Open("snowflake", dsn)
    if err != nil {
        // Use sanitized DSN for error logging
        c.logger.Error("Failed to open Snowflake connection",
            zap.String("dsn", c.config.SanitizedDSN()),
            zap.Error(err))
        return fmt.Errorf("failed to open Snowflake connection: %w", err)
    }
    
    // Set connection pool limits
    db.SetMaxOpenConns(10)
    db.SetMaxIdleConns(5)
    db.SetConnMaxLifetime(30 * time.Minute)
    
    if err := db.PingContext(connectCtx); err != nil {
        db.Close()
        // Use sanitized DSN for error logging
        c.logger.Error("Failed to ping Snowflake",
            zap.String("dsn", c.config.SanitizedDSN()),
            zap.Error(err))
        return fmt.Errorf("failed to ping Snowflake: %w", err)
    }
    
    c.db = db
    // Use sanitized DSN for success logging
    c.logger.Info("Successfully connected to Snowflake",
        zap.String("dsn", c.config.SanitizedDSN()),
        zap.Int("rate_limit_qps", c.config.GetRateLimitQPS()))
    return nil
}

// executeWithRetry executes a function with exponential backoff retry
func (c *snowflakeClient) executeWithRetry(ctx context.Context, operation string, fn func(context.Context) error) error {
    maxRetries := c.config.GetMaxRetries()
    initialDelay := c.config.GetRetryInitialDelay()
    maxDelay := c.config.GetRetryMaxDelay()
    
    var lastErr error
    for attempt := 0; attempt <= maxRetries; attempt++ {
        // Wait for rate limiter
        if err := c.rateLimiter.Wait(ctx); err != nil {
            return fmt.Errorf("rate limiter error: %w", err)
        }
        
        // Execute the operation
        c.queryCount++
        err := fn(ctx)
        
        if err == nil {
            return nil
        }
        
        lastErr = err
        c.errorCount++
        
        // Don't retry on final attempt
        if attempt == maxRetries {
            break
        }
        
        // Calculate exponential backoff delay
        delay := time.Duration(math.Pow(2, float64(attempt))) * initialDelay
        if delay > maxDelay {
            delay = maxDelay
        }
        
        c.retryCount++
        c.logger.Warn("Retrying operation after error",
            zap.String("operation", operation),
            zap.Int("attempt", attempt+1),
            zap.Int("max_retries", maxRetries),
            zap.Duration("delay", delay),
            zap.Error(err))
        
        // Wait before retry
        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-time.After(delay):
            // Continue to next retry
        }
    }
    
    return fmt.Errorf("operation %s failed after %d retries: %w", operation, maxRetries, lastErr)
}

func (c *snowflakeClient) queryMetrics(ctx context.Context) (*snowflakeMetrics, error) {
    metrics := &snowflakeMetrics{}
    
    if c.config.Metrics.CurrentQueries.Enabled {
        if err := c.queryCurrentQueries(ctx, metrics); err != nil {
            c.logger.Warn("Failed to query current queries", zap.Error(err))
        }
    }
    
    if c.config.Metrics.WarehouseLoad.Enabled {
        if err := c.queryWarehouseLoad(ctx, metrics); err != nil {
            c.logger.Warn("Failed to query warehouse load", zap.Error(err))
        }
    }
    
    if c.config.Metrics.QueryHistory.Enabled {
        if err := c.queryQueryHistory(ctx, metrics); err != nil {
            c.logger.Warn("Failed to query query history", zap.Error(err))
        }
    }
    
    if c.config.Metrics.CreditUsage.Enabled {
        if err := c.queryCreditUsage(ctx, metrics); err != nil {
            c.logger.Warn("Failed to query credit usage", zap.Error(err))
        }
    }
    
    if c.config.Metrics.StorageMetrics.Enabled {
        if err := c.queryStorageUsage(ctx, metrics); err != nil {
            c.logger.Warn("Failed to query storage usage", zap.Error(err))
        }
    }
    
    if c.config.Metrics.LoginHistory.Enabled {
        if err := c.queryLoginHistory(ctx, metrics); err != nil {
            c.logger.Warn("Failed to query login history", zap.Error(err))
        }
    }
    
    if c.config.Metrics.DataPipeline.Enabled {
        if err := c.queryPipeUsage(ctx, metrics); err != nil {
            c.logger.Warn("Failed to query pipe usage", zap.Error(err))
        }
    }
    
    if c.config.Metrics.DatabaseStorage.Enabled {
        if err := c.queryDatabaseStorage(ctx, metrics); err != nil {
            c.logger.Warn("Failed to query database storage", zap.Error(err))
        }
    }
    
    if c.config.Metrics.TaskHistory.Enabled {
        if err := c.queryTaskHistory(ctx, metrics); err != nil {
            c.logger.Warn("Failed to query task history", zap.Error(err))
        }
    }
    
    if c.config.Metrics.ReplicationUsage.Enabled {
        if err := c.queryReplicationUsage(ctx, metrics); err != nil {
            c.logger.Warn("Failed to query replication usage", zap.Error(err))
        }
    }
    
    if c.config.Metrics.AutoClusteringHistory.Enabled {
        if err := c.queryAutoClusteringHistory(ctx, metrics); err != nil {
            c.logger.Warn("Failed to query auto-clustering history", zap.Error(err))
        }
    }
    
    if c.config.EventTables.Enabled {
        if c.config.EventTables.QueryLogs.Enabled {
            if err := c.queryEventTableLogs(ctx, metrics, "QUERY"); err != nil {
                c.logger.Warn("Failed to query event table query logs", zap.Error(err))
            }
        }
        if c.config.EventTables.TaskLogs.Enabled {
            if err := c.queryEventTableLogs(ctx, metrics, "TASK"); err != nil {
                c.logger.Warn("Failed to query event table task logs", zap.Error(err))
            }
        }
        if c.config.EventTables.FunctionLogs.Enabled {
            if err := c.queryEventTableLogs(ctx, metrics, "FUNCTION"); err != nil {
                c.logger.Warn("Failed to query event table function logs", zap.Error(err))
            }
        }
        if c.config.EventTables.ProcedureLogs.Enabled {
            if err := c.queryEventTableLogs(ctx, metrics, "PROCEDURE"); err != nil {
                c.logger.Warn("Failed to query event table procedure logs", zap.Error(err))
            }
        }
    }
    
    if c.config.Organization.Enabled {
        if c.config.Organization.OrgCreditUsage.Enabled {
            if err := c.queryOrgCreditUsage(ctx, metrics); err != nil {
                c.logger.Warn("Failed to query org credit usage", zap.Error(err))
            }
        }
        if c.config.Organization.OrgStorageUsage.Enabled {
            if err := c.queryOrgStorageUsage(ctx, metrics); err != nil {
                c.logger.Warn("Failed to query org storage usage", zap.Error(err))
            }
        }
        if c.config.Organization.OrgDataTransfer.Enabled {
            if err := c.queryOrgDataTransfer(ctx, metrics); err != nil {
                c.logger.Warn("Failed to query org data transfer", zap.Error(err))
            }
        }
        if c.config.Organization.OrgContractUsage.Enabled {
            if err := c.queryOrgContractUsage(ctx, metrics); err != nil {
                c.logger.Warn("Failed to query org contract usage", zap.Error(err))
            }
        }
    }
    
    if c.config.CustomQueries.Enabled {
        for _, query := range c.config.CustomQueries.Queries {
            if err := c.executeCustomQuery(ctx, metrics, &query); err != nil {
                c.logger.Warn("Failed to execute custom query",
                    zap.String("query_name", query.Name),
                    zap.Error(err))
            }
        }
    }
    
    return metrics, nil
}

// Helper to execute query with timeout and retry
func (c *snowflakeClient) queryWithRetry(ctx context.Context, operation string, query string, args ...interface{}) (*sql.Rows, error) {
    var rows *sql.Rows
    
    err := c.executeWithRetry(ctx, operation, func(ctx context.Context) error {
        queryCtx, cancel := context.WithTimeout(ctx, c.config.GetQueryTimeout())
        defer cancel()
        
        r, err := c.db.QueryContext(queryCtx, query, args...)
        if err != nil {
            return err
        }
        rows = r
        return nil
    })
    
    return rows, err
}

func (c *snowflakeClient) queryCurrentQueries(ctx context.Context, metrics *snowflakeMetrics) error {
    query := `
        SELECT 
            WAREHOUSE_NAME,
            QUERY_TYPE,
            EXECUTION_STATUS,
            COUNT(*) as QUERY_COUNT,
            AVG(TOTAL_ELAPSED_TIME) as AVG_EXECUTION_TIME,
            AVG(BYTES_SCANNED) as AVG_BYTES_SCANNED
        FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY())
        WHERE START_TIME >= DATEADD(minute, -5, CURRENT_TIMESTAMP())
        GROUP BY WAREHOUSE_NAME, QUERY_TYPE, EXECUTION_STATUS
        LIMIT ?
    `
    
    rows, err := c.queryWithRetry(ctx, "current_queries", query, c.config.GetMaxRowsPerQuery())
    if err != nil {
        return fmt.Errorf("failed to query current queries: %w", err)
    }
    defer rows.Close()
    
    maxRows := c.config.GetMaxRowsPerQuery()
    rowCount := 0
    
    for rows.Next() {
        if rowCount >= maxRows {
            c.logger.Warn("Query result truncated at max rows",
                zap.String("query", "current_queries"),
                zap.Int("max_rows", maxRows))
            break
        }
        
        var row currentQueryRow
        if err := rows.Scan(
            &row.warehouseName,
            &row.queryType,
            &row.executionStatus,
            &row.queryCount,
            &row.avgExecutionTime,
            &row.avgBytesScanned,
        ); err != nil {
            return fmt.Errorf("failed to scan current query row: %w", err)
        }
        metrics.currentQueries = append(metrics.currentQueries, row)
        rowCount++
    }
    
    return rows.Err()
}

func (c *snowflakeClient) queryWarehouseLoad(ctx context.Context, metrics *snowflakeMetrics) error {
    query := `
        SELECT 
            WAREHOUSE_NAME,
            AVG(AVG_RUNNING) as AVG_RUNNING,
            AVG(AVG_QUEUED_LOAD) as AVG_QUEUED_LOAD,
            AVG(AVG_QUEUED_PROVISIONING) as AVG_QUEUED_PROVISIONING,
            AVG(AVG_BLOCKED) as AVG_BLOCKED
        FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.WAREHOUSE_LOAD_HISTORY())
        WHERE START_TIME >= DATEADD(minute, -5, CURRENT_TIMESTAMP())
        GROUP BY WAREHOUSE_NAME
        LIMIT ?
    `
    
    rows, err := c.queryWithRetry(ctx, "warehouse_load", query, c.config.GetMaxRowsPerQuery())
    if err != nil {
        return fmt.Errorf("failed to query warehouse load: %w", err)
    }
    defer rows.Close()
    
    maxRows := c.config.GetMaxRowsPerQuery()
    rowCount := 0
    
    for rows.Next() {
        if rowCount >= maxRows {
            c.logger.Warn("Query result truncated at max rows",
                zap.String("query", "warehouse_load"),
                zap.Int("max_rows", maxRows))
            break
        }
        
        var row warehouseLoadRow
        if err := rows.Scan(
            &row.warehouseName,
            &row.avgRunning,
            &row.avgQueuedLoad,
            &row.avgQueuedProvisioning,
            &row.avgBlocked,
        ); err != nil {
            return fmt.Errorf("failed to scan warehouse load row: %w", err)
        }
        metrics.warehouseLoad = append(metrics.warehouseLoad, row)
        rowCount++
    }
    
    return rows.Err()
}

func (c *snowflakeClient) queryQueryHistory(ctx context.Context, metrics *snowflakeMetrics) error {
    query := `
        SELECT 
            WAREHOUSE_NAME,
            QUERY_TYPE,
            EXECUTION_STATUS,
            COUNT(*) as QUERY_COUNT,
            AVG(TOTAL_ELAPSED_TIME) as AVG_EXECUTION_TIME,
            AVG(BYTES_SCANNED) as AVG_BYTES_SCANNED,
            AVG(BYTES_WRITTEN) as AVG_BYTES_WRITTEN,
            AVG(ROWS_PRODUCED) as AVG_ROWS_PRODUCED,
            AVG(COMPILATION_TIME) as AVG_COMPILATION_TIME
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
        GROUP BY WAREHOUSE_NAME, QUERY_TYPE, EXECUTION_STATUS
        LIMIT ?
    `
    
    rows, err := c.queryWithRetry(ctx, "query_history", query, c.config.GetMaxRowsPerQuery())
    if err != nil {
        return fmt.Errorf("failed to query query history: %w", err)
    }
    defer rows.Close()
    
    maxRows := c.config.GetMaxRowsPerQuery()
    rowCount := 0
    
    for rows.Next() {
        if rowCount >= maxRows {
            c.logger.Warn("Query result truncated at max rows",
                zap.String("query", "query_history"),
                zap.Int("max_rows", maxRows))
            break
        }
        
        var row queryStatRow
        if err := rows.Scan(
            &row.warehouseName,
            &row.queryType,
            &row.executionStatus,
            &row.queryCount,
            &row.avgExecutionTime,
            &row.avgBytesScanned,
            &row.avgBytesWritten,
            &row.avgRowsProduced,
            &row.avgCompilationTime,
        ); err != nil {
            return fmt.Errorf("failed to scan query history row: %w", err)
        }
        metrics.queryStats = append(metrics.queryStats, row)
        rowCount++
    }
    
    return rows.Err()
}

func (c *snowflakeClient) queryCreditUsage(ctx context.Context, metrics *snowflakeMetrics) error {
    query := `
        SELECT 
            WAREHOUSE_NAME,
            SUM(CREDITS_USED) as TOTAL_CREDITS,
            SUM(CREDITS_USED_COMPUTE) as COMPUTE_CREDITS,
            SUM(CREDITS_USED_CLOUD_SERVICES) as CLOUD_SERVICE_CREDITS
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
        WHERE START_TIME >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
        GROUP BY WAREHOUSE_NAME
        LIMIT ?
    `
    
    rows, err := c.queryWithRetry(ctx, "credit_usage", query, c.config.GetMaxRowsPerQuery())
    if err != nil {
        return fmt.Errorf("failed to query credit usage: %w", err)
    }
    defer rows.Close()
    
    maxRows := c.config.GetMaxRowsPerQuery()
    rowCount := 0
    
    for rows.Next() {
        if rowCount >= maxRows {
            c.logger.Warn("Query result truncated at max rows",
                zap.String("query", "credit_usage"),
                zap.Int("max_rows", maxRows))
            break
        }
        
        var row creditUsageRow
        if err := rows.Scan(
            &row.warehouseName,
            &row.totalCredits,
            &row.computeCredits,
            &row.cloudServiceCredits,
        ); err != nil {
            return fmt.Errorf("failed to scan credit usage row: %w", err)
        }
        metrics.creditUsage = append(metrics.creditUsage, row)
        rowCount++
    }
    
    return rows.Err()
}

func (c *snowflakeClient) queryStorageUsage(ctx context.Context, metrics *snowflakeMetrics) error {
    query := `
        SELECT 
            AVG(STORAGE_BYTES + STAGE_BYTES + FAILSAFE_BYTES) as TOTAL_STORAGE_BYTES,
            AVG(STAGE_BYTES) as STAGE_BYTES,
            AVG(FAILSAFE_BYTES) as FAILSAFE_BYTES
        FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE
        WHERE USAGE_DATE >= DATEADD(day, -1, CURRENT_DATE())
    `
    
    rows, err := c.queryWithRetry(ctx, "storage_usage", query)
    if err != nil {
        return fmt.Errorf("failed to query storage usage: %w", err)
    }
    defer rows.Close()
    
    if rows.Next() {
        var row storageUsageRow
        if err := rows.Scan(
            &row.totalStorageBytes,
            &row.stageBytes,
            &row.failsafeBytes,
        ); err != nil {
            return fmt.Errorf("failed to scan storage usage row: %w", err)
        }
        metrics.storageUsage = append(metrics.storageUsage, row)
    }
    
    return rows.Err()
}

func (c *snowflakeClient) queryLoginHistory(ctx context.Context, metrics *snowflakeMetrics) error {
    query := `
        SELECT 
            IS_SUCCESS,
            ERROR_CODE,
            COUNT(*) as LOGIN_COUNT
        FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY
        WHERE EVENT_TIMESTAMP >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
        GROUP BY IS_SUCCESS, ERROR_CODE
        LIMIT ?
    `
    
    rows, err := c.queryWithRetry(ctx, "login_history", query, c.config.GetMaxRowsPerQuery())
    if err != nil {
        return fmt.Errorf("failed to query login history: %w", err)
    }
    defer rows.Close()
    
    maxRows := c.config.GetMaxRowsPerQuery()
    rowCount := 0
    
    for rows.Next() {
        if rowCount >= maxRows {
            c.logger.Warn("Query result truncated at max rows",
                zap.String("query", "login_history"),
                zap.Int("max_rows", maxRows))
            break
        }
        
        var row loginHistoryRow
        if err := rows.Scan(
            &row.isSuccess,
            &row.errorCode,
            &row.loginCount,
        ); err != nil {
            return fmt.Errorf("failed to scan login history row: %w", err)
        }
        metrics.loginHistory = append(metrics.loginHistory, row)
        rowCount++
    }
    
    return rows.Err()
}

func (c *snowflakeClient) queryPipeUsage(ctx context.Context, metrics *snowflakeMetrics) error {
    query := `
        SELECT 
            PIPE_NAME,
            SUM(CREDITS_USED) as TOTAL_CREDITS,
            SUM(BYTES_INSERTED) as BYTES_INSERTED,
            SUM(FILES_INSERTED) as FILES_INSERTED
        FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
        WHERE START_TIME >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
        GROUP BY PIPE_NAME
        LIMIT ?
    `
    
    rows, err := c.queryWithRetry(ctx, "pipe_usage", query, c.config.GetMaxRowsPerQuery())
    if err != nil {
        return fmt.Errorf("failed to query pipe usage: %w", err)
    }
    defer rows.Close()
    
    maxRows := c.config.GetMaxRowsPerQuery()
    rowCount := 0
    
    for rows.Next() {
        if rowCount >= maxRows {
            c.logger.Warn("Query result truncated at max rows",
                zap.String("query", "pipe_usage"),
                zap.Int("max_rows", maxRows))
            break
        }
        
        var row pipeUsageRow
        if err := rows.Scan(
            &row.pipeName,
            &row.totalCredits,
            &row.bytesInserted,
            &row.filesInserted,
        ); err != nil {
            return fmt.Errorf("failed to scan pipe usage row: %w", err)
        }
        metrics.pipeUsage = append(metrics.pipeUsage, row)
        rowCount++
    }
    
    return rows.Err()
}

func (c *snowflakeClient) queryDatabaseStorage(ctx context.Context, metrics *snowflakeMetrics) error {
    query := `
        SELECT 
            DATABASE_NAME,
            AVG(AVERAGE_DATABASE_BYTES) as AVG_DATABASE_BYTES,
            AVG(AVERAGE_FAILSAFE_BYTES) as AVG_FAILSAFE_BYTES
        FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY
        WHERE USAGE_DATE >= DATEADD(day, -1, CURRENT_DATE())
        GROUP BY DATABASE_NAME
        LIMIT ?
    `
    
    rows, err := c.queryWithRetry(ctx, "database_storage", query, c.config.GetMaxRowsPerQuery())
    if err != nil {
        return fmt.Errorf("failed to query database storage: %w", err)
    }
    defer rows.Close()
    
    maxRows := c.config.GetMaxRowsPerQuery()
    rowCount := 0
    
    for rows.Next() {
        if rowCount >= maxRows {
            c.logger.Warn("Query result truncated at max rows",
                zap.String("query", "database_storage"),
                zap.Int("max_rows", maxRows))
            break
        }
        
        var row databaseStorageRow
        if err := rows.Scan(
            &row.databaseName,
            &row.avgDatabaseBytes,
            &row.avgFailsafeBytes,
        ); err != nil {
            return fmt.Errorf("failed to scan database storage row: %w", err)
        }
        metrics.databaseStorage = append(metrics.databaseStorage, row)
        rowCount++
    }
    
    return rows.Err()
}

func (c *snowflakeClient) queryTaskHistory(ctx context.Context, metrics *snowflakeMetrics) error {
    query := `
        SELECT 
            DATABASE_NAME,
            SCHEMA_NAME,
            NAME as TASK_NAME,
            STATE,
            COUNT(*) as EXECUTION_COUNT,
            AVG(DATEDIFF(MILLISECOND, SCHEDULED_TIME, COMPLETED_TIME)) as AVG_EXECUTION_TIME
        FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
        WHERE SCHEDULED_TIME >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
          AND COMPLETED_TIME IS NOT NULL
        GROUP BY DATABASE_NAME, SCHEMA_NAME, NAME, STATE
        LIMIT ?
    `
    
    rows, err := c.queryWithRetry(ctx, "task_history", query, c.config.GetMaxRowsPerQuery())
    if err != nil {
        return fmt.Errorf("failed to query task history: %w", err)
    }
    defer rows.Close()
    
    maxRows := c.config.GetMaxRowsPerQuery()
    rowCount := 0
    
    for rows.Next() {
        if rowCount >= maxRows {
            c.logger.Warn("Query result truncated at max rows",
                zap.String("query", "task_history"),
                zap.Int("max_rows", maxRows))
            break
        }
        
        var row taskHistoryRow
        if err := rows.Scan(
            &row.databaseName,
            &row.schemaName,
            &row.taskName,
            &row.state,
            &row.executionCount,
            &row.avgExecutionTime,
        ); err != nil {
            return fmt.Errorf("failed to scan task history row: %w", err)
        }
        metrics.taskHistory = append(metrics.taskHistory, row)
        rowCount++
    }
    
    return rows.Err()
}

func (c *snowflakeClient) queryReplicationUsage(ctx context.Context, metrics *snowflakeMetrics) error {
    query := `
        SELECT 
            DATABASE_NAME,
            SUM(CREDITS_USED) as TOTAL_CREDITS,
            SUM(BYTES_TRANSFERRED) as BYTES_TRANSFERRED
        FROM SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_USAGE_HISTORY
        WHERE START_TIME >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
        GROUP BY DATABASE_NAME
        LIMIT ?
    `
    
    rows, err := c.queryWithRetry(ctx, "replication_usage", query, c.config.GetMaxRowsPerQuery())
    if err != nil {
        return fmt.Errorf("failed to query replication usage: %w", err)
    }
    defer rows.Close()
    
    maxRows := c.config.GetMaxRowsPerQuery()
    rowCount := 0
    
    for rows.Next() {
        if rowCount >= maxRows {
            c.logger.Warn("Query result truncated at max rows",
                zap.String("query", "replication_usage"),
                zap.Int("max_rows", maxRows))
            break
        }
        
        var row replicationUsageRow
        if err := rows.Scan(
            &row.databaseName,
            &row.totalCredits,
            &row.bytesTransferred,
        ); err != nil {
            return fmt.Errorf("failed to scan replication usage row: %w", err)
        }
        metrics.replicationUsage = append(metrics.replicationUsage, row)
        rowCount++
    }
    
    return rows.Err()
}

func (c *snowflakeClient) queryAutoClusteringHistory(ctx context.Context, metrics *snowflakeMetrics) error {
    query := `
        SELECT 
            DATABASE_NAME,
            SCHEMA_NAME,
            TABLE_NAME,
            SUM(CREDITS_USED) as TOTAL_CREDITS,
            SUM(NUM_BYTES_RECLUSTERED) as BYTES_RECLUSTERED,
            SUM(NUM_ROWS_RECLUSTERED) as ROWS_RECLUSTERED
        FROM SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY
        WHERE START_TIME >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
        GROUP BY DATABASE_NAME, SCHEMA_NAME, TABLE_NAME
        LIMIT ?
    `
    
    rows, err := c.queryWithRetry(ctx, "auto_clustering", query, c.config.GetMaxRowsPerQuery())
    if err != nil {
        return fmt.Errorf("failed to query auto-clustering history: %w", err)
    }
    defer rows.Close()
    
    maxRows := c.config.GetMaxRowsPerQuery()
    rowCount := 0
    
    for rows.Next() {
        if rowCount >= maxRows {
            c.logger.Warn("Query result truncated at max rows",
                zap.String("query", "auto_clustering"),
                zap.Int("max_rows", maxRows))
            break
        }
        
        var row autoClusteringRow
        if err := rows.Scan(
            &row.databaseName,
            &row.schemaName,
            &row.tableName,
            &row.totalCredits,
            &row.bytesReclustered,
            &row.rowsReclustered,
        ); err != nil {
            return fmt.Errorf("failed to scan auto-clustering row: %w", err)
        }
        metrics.autoClusteringHistory = append(metrics.autoClusteringHistory, row)
        rowCount++
    }
    
    return rows.Err()
}

func (c *snowflakeClient) queryEventTableLogs(ctx context.Context, metrics *snowflakeMetrics, eventType string) error {
    if c.config.EventTables.TableName == "" {
        return fmt.Errorf("event table name not configured")
    }
    
    // Validate table name to prevent SQL injection
    if err := validateIdentifier(c.config.EventTables.TableName); err != nil {
        return fmt.Errorf("invalid event table name: %w", err)
    }
    
    // Validate event type (whitelist)
    validEventTypes := map[string]bool{
        "QUERY": true, "TASK": true, "FUNCTION": true, "PROCEDURE": true,
    }
    if !validEventTypes[eventType] {
        return fmt.Errorf("invalid event type: %s", eventType)
    }
    
    query := fmt.Sprintf(`
        SELECT 
            RESOURCE_ATTRIBUTES['snow.event.type']::STRING as EVENT_TYPE,
            SEVERITY_TEXT,
            RECORD['error.message']::STRING as ERROR_MESSAGE
        FROM %s
        WHERE TIMESTAMP >= DATEADD(minute, -5, CURRENT_TIMESTAMP())
          AND RESOURCE_ATTRIBUTES['snow.event.type']::STRING = ?
        LIMIT ?
    `, c.config.EventTables.TableName)
    
    rows, err := c.queryWithRetry(ctx, "event_table_"+eventType, query, eventType, c.config.GetMaxRowsPerQuery())
    if err != nil {
        return fmt.Errorf("failed to query event table logs: %w", err)
    }
    defer rows.Close()
    
    var events []eventTableRow
    maxRows := c.config.GetMaxRowsPerQuery()
    rowCount := 0
    
    for rows.Next() {
        if rowCount >= maxRows {
            c.logger.Warn("Query result truncated at max rows",
                zap.String("query", "event_table_"+eventType),
                zap.Int("max_rows", maxRows))
            break
        }
        
        var row eventTableRow
        if err := rows.Scan(
            &row.eventType,
            &row.severity,
            &row.errorMessage,
        ); err != nil {
            return fmt.Errorf("failed to scan event table row: %w", err)
        }
        events = append(events, row)
        rowCount++
    }
    
    switch eventType {
    case "QUERY":
        metrics.eventQueryLogs = events
    case "TASK":
        metrics.eventTaskLogs = events
    case "FUNCTION":
        metrics.eventFunctionLogs = events
    case "PROCEDURE":
        metrics.eventProcedureLogs = events
    }
    
    return rows.Err()
}

func (c *snowflakeClient) queryOrgCreditUsage(ctx context.Context, metrics *snowflakeMetrics) error {
    cols := c.config.Organization.OrgCreditUsage.Columns
    
    // Validate all column names
    for _, col := range []string{
        cols.GetOrganizationName(),
        cols.GetAccountName(),
        cols.GetServiceType(),
        cols.GetCreditsUsed(),
        cols.GetUsageDate(),
    } {
        if err := validateIdentifier(col); err != nil {
            return fmt.Errorf("invalid column name in org credit usage: %w", err)
        }
    }
    
    query := fmt.Sprintf(`
        SELECT 
            %s as ORGANIZATION_NAME,
            %s as ACCOUNT_NAME,
            %s as SERVICE_TYPE,
            SUM(%s) as TOTAL_CREDITS
        FROM SNOWFLAKE.ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY
        WHERE %s >= DATEADD(day, -1, CURRENT_DATE())
        GROUP BY %s, %s, %s
        LIMIT ?
    `,
        cols.GetOrganizationName(),
        cols.GetAccountName(),
        cols.GetServiceType(),
        cols.GetCreditsUsed(),
        cols.GetUsageDate(),
        cols.GetOrganizationName(),
        cols.GetAccountName(),
        cols.GetServiceType(),
    )
    
    rows, err := c.queryWithRetry(ctx, "org_credit_usage", query, c.config.GetMaxRowsPerQuery())
    if err != nil {
        return fmt.Errorf("failed to query org credit usage: %w", err)
    }
    defer rows.Close()
    
    maxRows := c.config.GetMaxRowsPerQuery()
    rowCount := 0
    
    for rows.Next() {
        if rowCount >= maxRows {
            c.logger.Warn("Query result truncated at max rows",
                zap.String("query", "org_credit_usage"),
                zap.Int("max_rows", maxRows))
            break
        }
        
        var row orgCreditUsageRow
        if err := rows.Scan(
            &row.organizationName,
            &row.accountName,
            &row.serviceType,
            &row.totalCredits,
        ); err != nil {
            return fmt.Errorf("failed to scan org credit usage row: %w", err)
        }
        metrics.orgCreditUsage = append(metrics.orgCreditUsage, row)
        rowCount++
    }
    
    return rows.Err()
}

func (c *snowflakeClient) queryOrgStorageUsage(ctx context.Context, metrics *snowflakeMetrics) error {
    cols := c.config.Organization.OrgStorageUsage.Columns
    
    // Validate all column names
    for _, col := range []string{
        cols.GetOrganizationName(),
        cols.GetAccountName(),
        cols.GetStorageBytes(),
        cols.GetStageBytes(),
        cols.GetFailsafeBytes(),
        cols.GetUsageDate(),
    } {
        if err := validateIdentifier(col); err != nil {
            return fmt.Errorf("invalid column name in org storage usage: %w", err)
        }
    }
    
    query := fmt.Sprintf(`
        SELECT 
            %s as ORGANIZATION_NAME,
            %s as ACCOUNT_NAME,
            AVG(%s) as AVG_STORAGE_BYTES,
            AVG(%s) as AVG_STAGE_BYTES,
            AVG(%s) as AVG_FAILSAFE_BYTES
        FROM SNOWFLAKE.ORGANIZATION_USAGE.STORAGE_DAILY_HISTORY
        WHERE %s >= DATEADD(day, -1, CURRENT_DATE())
        GROUP BY %s, %s
        LIMIT ?
    `,
        cols.GetOrganizationName(),
        cols.GetAccountName(),
        cols.GetStorageBytes(),
        cols.GetStageBytes(),
        cols.GetFailsafeBytes(),
        cols.GetUsageDate(),
        cols.GetOrganizationName(),
        cols.GetAccountName(),
    )
    
    rows, err := c.queryWithRetry(ctx, "org_storage_usage", query, c.config.GetMaxRowsPerQuery())
    if err != nil {
        return fmt.Errorf("failed to query org storage usage: %w", err)
    }
    defer rows.Close()
    
    maxRows := c.config.GetMaxRowsPerQuery()
    rowCount := 0
    
    for rows.Next() {
        if rowCount >= maxRows {
            c.logger.Warn("Query result truncated at max rows",
                zap.String("query", "org_storage_usage"),
                zap.Int("max_rows", maxRows))
            break
        }
        
        var row orgStorageUsageRow
        if err := rows.Scan(
            &row.organizationName,
            &row.accountName,
            &row.avgStorageBytes,
            &row.avgStageBytes,
            &row.avgFailsafeBytes,
        ); err != nil {
            return fmt.Errorf("failed to scan org storage usage row: %w", err)
        }
        metrics.orgStorageUsage = append(metrics.orgStorageUsage, row)
        rowCount++
    }
    
    return rows.Err()
}

func (c *snowflakeClient) queryOrgDataTransfer(ctx context.Context, metrics *snowflakeMetrics) error {
    cols := c.config.Organization.OrgDataTransfer.Columns
    
    // Validate all column names
    for _, col := range []string{
        cols.GetOrganizationName(),
        cols.GetSourceAccountName(),
        cols.GetTargetAccountName(),
        cols.GetSourceRegion(),
        cols.GetTargetRegion(),
        cols.GetBytesTransferred(),
        cols.GetTransferDate(),
    } {
        if err := validateIdentifier(col); err != nil {
            return fmt.Errorf("invalid column name in org data transfer: %w", err)
        }
    }
    
    query := fmt.Sprintf(`
        SELECT 
            %s as ORGANIZATION_NAME,
            %s as SOURCE_ACCOUNT_NAME,
            %s as TARGET_ACCOUNT_NAME,
            %s as SOURCE_REGION,
            %s as TARGET_REGION,
            SUM(%s) as TOTAL_BYTES_TRANSFERRED
        FROM SNOWFLAKE.ORGANIZATION_USAGE.DATA_TRANSFER_DAILY_HISTORY
        WHERE %s >= DATEADD(day, -1, CURRENT_DATE())
        GROUP BY %s, %s, %s, %s, %s
        LIMIT ?
    `,
        cols.GetOrganizationName(),
        cols.GetSourceAccountName(),
        cols.GetTargetAccountName(),
        cols.GetSourceRegion(),
        cols.GetTargetRegion(),
        cols.GetBytesTransferred(),
        cols.GetTransferDate(),
        cols.GetOrganizationName(),
        cols.GetSourceAccountName(),
        cols.GetTargetAccountName(),
        cols.GetSourceRegion(),
        cols.GetTargetRegion(),
    )
    
    rows, err := c.queryWithRetry(ctx, "org_data_transfer", query, c.config.GetMaxRowsPerQuery())
    if err != nil {
        return fmt.Errorf("failed to query org data transfer: %w", err)
    }
    defer rows.Close()
    
    maxRows := c.config.GetMaxRowsPerQuery()
    rowCount := 0
    
    for rows.Next() {
        if rowCount >= maxRows {
            c.logger.Warn("Query result truncated at max rows",
                zap.String("query", "org_data_transfer"),
                zap.Int("max_rows", maxRows))
            break
        }
        
        var row orgDataTransferRow
        if err := rows.Scan(
            &row.organizationName,
            &row.sourceAccountName,
            &row.targetAccountName,
            &row.sourceRegion,
            &row.targetRegion,
            &row.totalBytesTransferred,
        ); err != nil {
            return fmt.Errorf("failed to scan org data transfer row: %w", err)
        }
        metrics.orgDataTransfer = append(metrics.orgDataTransfer, row)
        rowCount++
    }
    
    return rows.Err()
}

func (c *snowflakeClient) queryOrgContractUsage(ctx context.Context, metrics *snowflakeMetrics) error {
    cols := c.config.Organization.OrgContractUsage.Columns
    
    // Validate all column names
    for _, col := range []string{
        cols.GetOrganizationName(),
        cols.GetContractNumber(),
        cols.GetCreditsUsed(),
        cols.GetCreditsBilled(),
        cols.GetUsageDate(),
    } {
        if err := validateIdentifier(col); err != nil {
            return fmt.Errorf("invalid column name in org contract usage: %w", err)
        }
    }
    
    query := fmt.Sprintf(`
        SELECT 
            %s as ORGANIZATION_NAME,
            %s as CONTRACT_NUMBER,
            SUM(%s) as TOTAL_CREDITS_USED,
            SUM(%s) as TOTAL_CREDITS_BILLED
        FROM SNOWFLAKE.ORGANIZATION_USAGE.CONTRACT_ITEMS
        WHERE %s >= DATEADD(day, -1, CURRENT_DATE())
        GROUP BY %s, %s
        LIMIT ?
    `,
        cols.GetOrganizationName(),
        cols.GetContractNumber(),
        cols.GetCreditsUsed(),
        cols.GetCreditsBilled(),
        cols.GetUsageDate(),
        cols.GetOrganizationName(),
        cols.GetContractNumber(),
    )
    
    rows, err := c.queryWithRetry(ctx, "org_contract_usage", query, c.config.GetMaxRowsPerQuery())
    if err != nil {
        return fmt.Errorf("failed to query org contract usage: %w", err)
    }
    defer rows.Close()
    
    maxRows := c.config.GetMaxRowsPerQuery()
    rowCount := 0
    
    for rows.Next() {
        if rowCount >= maxRows {
            c.logger.Warn("Query result truncated at max rows",
                zap.String("query", "org_contract_usage"),
                zap.Int("max_rows", maxRows))
            break
        }
        
        var row orgContractUsageRow
        if err := rows.Scan(
            &row.organizationName,
            &row.contractNumber,
            &row.totalCreditsUsed,
            &row.totalCreditsBilled,
        ); err != nil {
            return fmt.Errorf("failed to scan org contract usage row: %w", err)
        }
        metrics.orgContractUsage = append(metrics.orgContractUsage, row)
        rowCount++
    }
    
    return rows.Err()
}

func (c *snowflakeClient) executeCustomQuery(ctx context.Context, metrics *snowflakeMetrics, query *CustomQuery) error {
    rows, err := c.queryWithRetry(ctx, "custom_"+query.Name, query.SQL)
    if err != nil {
        return fmt.Errorf("failed to execute custom query %s: %w", query.Name, err)
    }
    defer rows.Close()
    
    columns, err := rows.Columns()
    if err != nil {
        return fmt.Errorf("failed to get columns for custom query %s: %w", query.Name, err)
    }
    
    var resultRows []map[string]interface{}
    rowCount := 0
    maxRows := c.config.GetMaxRowsPerQuery()
    
    for rows.Next() {
        if rowCount >= maxRows {
            c.logger.Warn("Custom query exceeded max rows",
                zap.String("query_name", query.Name),
                zap.Int("max_rows", maxRows))
            break
        }
        
        values := make([]interface{}, len(columns))
        valuePtrs := make([]interface{}, len(columns))
        for i := range values {
            valuePtrs[i] = &values[i]
        }
        
        if err := rows.Scan(valuePtrs...); err != nil {
            return fmt.Errorf("failed to scan custom query row: %w", err)
        }
        
        row := make(map[string]interface{})
        for i, col := range columns {
            row[col] = values[i]
        }
        resultRows = append(resultRows, row)
        rowCount++
    }
    
    metrics.customQueryResults = append(metrics.customQueryResults, customQueryResult{
        name:       query.Name,
        metricType: query.MetricType,
        rows:       resultRows,
    })
    
    return rows.Err()
}
