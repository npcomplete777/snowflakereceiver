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
    
    return metrics, nil
}

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

// Data structures
type snowflakeMetrics struct {
    queryStats      []queryStatRow
    creditUsage     []creditUsageRow
    storageUsage    []storageUsageRow
    loginHistory    []loginHistoryRow
    pipeUsage       []pipeUsageRow
    databaseStorage []databaseStorageRow
}

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
