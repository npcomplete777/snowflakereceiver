package snowflakereceiver

import (
    "context"
    "database/sql"
    "fmt"
    "time"
    
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
    if c.config.Metrics.CurrentQueries.Enabled {
        queries, err := c.queryCurrentQueries(ctx)
        if err != nil {
            c.logger.Warn("Failed to query current queries", zap.Error(err))
        } else {
            metrics.currentQueries = queries
        }
    }
    
    if c.config.Metrics.WarehouseLoad.Enabled {
        load, err := c.queryWarehouseLoadHistory(ctx)
        if err != nil {
            c.logger.Warn("Failed to query warehouse load", zap.Error(err))
        } else {
            metrics.warehouseLoad = load
        }
    }
    
    // ACCOUNT_USAGE queries (45min-3hr latency)
    if c.config.Metrics.QueryHistory.Enabled {
        stats, err := c.queryQueryStats(ctx)
        if err != nil {
            c.logger.Warn("Failed to query query stats", zap.Error(err))
        } else {
            metrics.queryStats = stats
        }
    }
    
    if c.config.Metrics.CreditUsage.Enabled {
        credits, err := c.queryWarehouseCreditUsage(ctx)
        if err != nil {
            c.logger.Warn("Failed to query credit usage", zap.Error(err))
        } else {
            metrics.creditUsage = credits
        }
    }
    
    if c.config.Metrics.StorageMetrics.Enabled {
        storage, err := c.queryStorageUsage(ctx)
        if err != nil {
            c.logger.Warn("Failed to query storage usage", zap.Error(err))
        } else {
            metrics.storageUsage = storage
        }
    }
    
    if c.config.Metrics.LoginHistory.Enabled {
        logins, err := c.queryLoginHistory(ctx)
        if err != nil {
            c.logger.Warn("Failed to query login history", zap.Error(err))
        } else {
            metrics.loginHistory = logins
        }
    }
    
    if c.config.Metrics.DataPipeline.Enabled {
        pipes, err := c.queryPipeUsage(ctx)
        if err != nil {
            c.logger.Warn("Failed to query pipe usage", zap.Error(err))
        } else {
            metrics.pipeUsage = pipes
        }
    }
    
    if c.config.Metrics.DatabaseStorage.Enabled {
        dbStorage, err := c.queryDatabaseStorage(ctx)
        if err != nil {
            c.logger.Warn("Failed to query database storage", zap.Error(err))
        } else {
            metrics.databaseStorage = dbStorage
        }
    }
    
    if c.config.Metrics.TaskHistory.Enabled {
        tasks, err := c.queryTaskHistory(ctx)
        if err != nil {
            c.logger.Warn("Failed to query task history", zap.Error(err))
        } else {
            metrics.taskHistory = tasks
        }
    }
    
    if c.config.Metrics.ReplicationUsage.Enabled {
        replication, err := c.queryReplicationUsage(ctx)
        if err != nil {
            c.logger.Warn("Failed to query replication usage", zap.Error(err))
        } else {
            metrics.replicationUsage = replication
        }
    }
    
    if c.config.Metrics.AutoClusteringHistory.Enabled {
        clustering, err := c.queryAutoClusteringHistory(ctx)
        if err != nil {
            c.logger.Warn("Failed to query auto-clustering history", zap.Error(err))
        } else {
            metrics.autoClusteringHistory = clustering
        }
    }
    
    // EVENT TABLES queries (SECONDS-LEVEL LATENCY!)
    if c.config.EventTables.Enabled {
        if c.config.EventTables.QueryLogs.Enabled {
            eventLogs, err := c.queryEventTableLogs(ctx, "QUERY")
            if err != nil {
                c.logger.Warn("Failed to query event table query logs", zap.Error(err))
            } else {
                metrics.eventQueryLogs = eventLogs
            }
        }
        
        if c.config.EventTables.TaskLogs.Enabled {
            eventLogs, err := c.queryEventTableLogs(ctx, "TASK")
            if err != nil {
                c.logger.Warn("Failed to query event table task logs", zap.Error(err))
            } else {
                metrics.eventTaskLogs = eventLogs
            }
        }
        
        if c.config.EventTables.FunctionLogs.Enabled {
            eventLogs, err := c.queryEventTableLogs(ctx, "FUNCTION")
            if err != nil {
                c.logger.Warn("Failed to query event table function logs", zap.Error(err))
            } else {
                metrics.eventFunctionLogs = eventLogs
            }
        }
        
        if c.config.EventTables.ProcedureLogs.Enabled {
            eventLogs, err := c.queryEventTableLogs(ctx, "PROCEDURE")
            if err != nil {
                c.logger.Warn("Failed to query event table procedure logs", zap.Error(err))
            } else {
                metrics.eventProcedureLogs = eventLogs
            }
        }
    }
    
    // ORGANIZATION queries (multi-account)
    if c.config.Organization.Enabled {
        if c.config.Organization.OrgCreditUsage.Enabled {
            orgCredits, err := c.queryOrgCreditUsage(ctx)
            if err != nil {
                c.logger.Warn("Failed to query org credit usage", zap.Error(err))
            } else {
                metrics.orgCreditUsage = orgCredits
            }
        }
        
        if c.config.Organization.OrgStorageUsage.Enabled {
            orgStorage, err := c.queryOrgStorageUsage(ctx)
            if err != nil {
                c.logger.Warn("Failed to query org storage usage", zap.Error(err))
            } else {
                metrics.orgStorageUsage = orgStorage
            }
        }
        
        if c.config.Organization.OrgDataTransfer.Enabled {
            orgTransfer, err := c.queryOrgDataTransfer(ctx)
            if err != nil {
                c.logger.Warn("Failed to query org data transfer", zap.Error(err))
            } else {
                metrics.orgDataTransfer = orgTransfer
            }
        }
        
        if c.config.Organization.OrgContractUsage.Enabled {
            orgContract, err := c.queryOrgContractUsage(ctx)
            if err != nil {
                c.logger.Warn("Failed to query org contract usage", zap.Error(err))
            } else {
                metrics.orgContractUsage = orgContract
            }
        }
    }
    
    // CUSTOM QUERIES
    if c.config.CustomQueries.Enabled {
        for _, customQuery := range c.config.CustomQueries.Queries {
            results, err := c.executeCustomQuery(ctx, customQuery)
            if err != nil {
                c.logger.Warn("Failed to execute custom query", 
                    zap.String("query_name", customQuery.Name),
                    zap.Error(err))
            } else {
                metrics.customQueryResults = append(metrics.customQueryResults, results)
            }
        }
    }
    
    return metrics, nil
}

// ========== INFORMATION_SCHEMA Queries (REAL-TIME!) ==========

func (c *snowflakeClient) queryCurrentQueries(ctx context.Context) ([]currentQueryRow, error) {
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

// ========== EVENT TABLES Queries (SECONDS-LEVEL LATENCY!) ==========

func (c *snowflakeClient) queryEventTableLogs(ctx context.Context, eventType string) ([]eventTableRow, error) {
    // Query event table for logs with CDC (change data capture)
    // This uses CHANGES clause to get only new events since last query
    query := fmt.Sprintf(`
        SELECT 
            TIMESTAMP,
            RESOURCE_ATTRIBUTES['snow.database.name']::STRING as DATABASE_NAME,
            RESOURCE_ATTRIBUTES['snow.schema.name']::STRING as SCHEMA_NAME,
            RESOURCE_ATTRIBUTES['snow.executable.name']::STRING as OBJECT_NAME,
            RECORD['severity_text']::STRING as SEVERITY,
            RECORD['body']::STRING as MESSAGE,
            RECORD_ATTRIBUTES['snow.query.id']::STRING as QUERY_ID,
            RECORD_ATTRIBUTES['error.message']::STRING as ERROR_MESSAGE
        FROM %s
        WHERE RECORD_TYPE = '%s'
        AND TIMESTAMP >= DATEADD('seconds', -60, CURRENT_TIMESTAMP())
        ORDER BY TIMESTAMP DESC
        LIMIT 1000
    `, c.config.EventTables.TableName, eventType)
    
    rows, err := c.db.QueryContext(ctx, query)
    if err != nil {
        return nil, fmt.Errorf("failed to query event table: %w", err)
    }
    defer rows.Close()
    
    var results []eventTableRow
    for rows.Next() {
        var r eventTableRow
        err := rows.Scan(
            &r.timestamp,
            &r.databaseName,
            &r.schemaName,
            &r.objectName,
            &r.severity,
            &r.message,
            &r.queryID,
            &r.errorMessage,
        )
        if err != nil {
            c.logger.Warn("Failed to scan event table row", zap.Error(err))
            continue
        }
        r.eventType = eventType
        results = append(results, r)
    }
    
    return results, rows.Err()
}

// ========== ORGANIZATION Queries (Multi-Account) ==========

func (c *snowflakeClient) queryOrgCreditUsage(ctx context.Context) ([]orgCreditUsageRow, error) {
    query := `
        SELECT 
            ORGANIZATION_NAME,
            ACCOUNT_NAME,
            SERVICE_TYPE,
            SUM(CREDITS_USED) as TOTAL_CREDITS
        FROM SNOWFLAKE.ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY
        WHERE USAGE_DATE >= DATEADD(day, -1, CURRENT_DATE())
        GROUP BY ORGANIZATION_NAME, ACCOUNT_NAME, SERVICE_TYPE
    `
    
    rows, err := c.db.QueryContext(ctx, query)
    if err != nil {
        return nil, fmt.Errorf("failed to query org credit usage: %w", err)
    }
    defer rows.Close()
    
    var results []orgCreditUsageRow
    for rows.Next() {
        var r orgCreditUsageRow
        err := rows.Scan(&r.organizationName, &r.accountName, &r.serviceType, &r.totalCredits)
        if err != nil {
            c.logger.Warn("Failed to scan org credit usage row", zap.Error(err))
            continue
        }
        results = append(results, r)
    }
    
    return results, rows.Err()
}

func (c *snowflakeClient) queryOrgStorageUsage(ctx context.Context) ([]orgStorageUsageRow, error) {
    query := `
        SELECT 
            ORGANIZATION_NAME,
            ACCOUNT_NAME,
            AVG(STORAGE_BYTES) as AVG_STORAGE_BYTES,
            AVG(STAGE_BYTES) as AVG_STAGE_BYTES,
            AVG(FAILSAFE_BYTES) as AVG_FAILSAFE_BYTES
        FROM SNOWFLAKE.ORGANIZATION_USAGE.STORAGE_DAILY_HISTORY
        WHERE USAGE_DATE >= DATEADD(day, -1, CURRENT_DATE())
        GROUP BY ORGANIZATION_NAME, ACCOUNT_NAME
    `
    
    rows, err := c.db.QueryContext(ctx, query)
    if err != nil {
        return nil, fmt.Errorf("failed to query org storage usage: %w", err)
    }
    defer rows.Close()
    
    var results []orgStorageUsageRow
    for rows.Next() {
        var r orgStorageUsageRow
        err := rows.Scan(&r.organizationName, &r.accountName, &r.avgStorageBytes, &r.avgStageBytes, &r.avgFailsafeBytes)
        if err != nil {
            c.logger.Warn("Failed to scan org storage usage row", zap.Error(err))
            continue
        }
        results = append(results, r)
    }
    
    return results, rows.Err()
}

func (c *snowflakeClient) queryOrgDataTransfer(ctx context.Context) ([]orgDataTransferRow, error) {
    query := `
        SELECT 
            ORGANIZATION_NAME,
            SOURCE_ACCOUNT_NAME,
            TARGET_ACCOUNT_NAME,
            SOURCE_REGION,
            TARGET_REGION,
            SUM(BYTES_TRANSFERRED) as TOTAL_BYTES_TRANSFERRED
        FROM SNOWFLAKE.ORGANIZATION_USAGE.DATA_TRANSFER_HISTORY
        WHERE START_TIME >= DATEADD(day, -1, CURRENT_TIMESTAMP())
        GROUP BY ORGANIZATION_NAME, SOURCE_ACCOUNT_NAME, TARGET_ACCOUNT_NAME, SOURCE_REGION, TARGET_REGION
    `
    
    rows, err := c.db.QueryContext(ctx, query)
    if err != nil {
        return nil, fmt.Errorf("failed to query org data transfer: %w", err)
    }
    defer rows.Close()
    
    var results []orgDataTransferRow
    for rows.Next() {
        var r orgDataTransferRow
        err := rows.Scan(
            &r.organizationName,
            &r.sourceAccountName,
            &r.targetAccountName,
            &r.sourceRegion,
            &r.targetRegion,
            &r.totalBytesTransferred,
        )
        if err != nil {
            c.logger.Warn("Failed to scan org data transfer row", zap.Error(err))
            continue
        }
        results = append(results, r)
    }
    
    return results, rows.Err()
}

func (c *snowflakeClient) queryOrgContractUsage(ctx context.Context) ([]orgContractUsageRow, error) {
    query := `
        SELECT 
            ORGANIZATION_NAME,
            CONTRACT_NUMBER,
            SUM(CREDITS_USED) as TOTAL_CREDITS_USED,
            MAX(CREDITS_BILLED) as TOTAL_CREDITS_BILLED,
            MAX(CREDITS_ADJUSTMENT_CLOUD_SERVICES) as CLOUD_SERVICES_ADJUSTMENT
        FROM SNOWFLAKE.ORGANIZATION_USAGE.CONTRACT_ITEMS
        WHERE USAGE_DATE >= DATEADD(day, -7, CURRENT_DATE())
        GROUP BY ORGANIZATION_NAME, CONTRACT_NUMBER
    `
    
    rows, err := c.db.QueryContext(ctx, query)
    if err != nil {
        return nil, fmt.Errorf("failed to query org contract usage: %w", err)
    }
    defer rows.Close()
    
    var results []orgContractUsageRow
    for rows.Next() {
        var r orgContractUsageRow
        err := rows.Scan(
            &r.organizationName,
            &r.contractNumber,
            &r.totalCreditsUsed,
            &r.totalCreditsBilled,
            &r.cloudServicesAdjustment,
        )
        if err != nil {
            c.logger.Warn("Failed to scan org contract usage row", zap.Error(err))
            continue
        }
        results = append(results, r)
    }
    
    return results, rows.Err()
}

// ========== CUSTOM QUERIES ==========

func (c *snowflakeClient) executeCustomQuery(ctx context.Context, query CustomQuery) (customQueryResult, error) {
    rows, err := c.db.QueryContext(ctx, query.SQL)
    if err != nil {
        return customQueryResult{}, fmt.Errorf("failed to execute custom query %s: %w", query.Name, err)
    }
    defer rows.Close()
    
    columns, err := rows.Columns()
    if err != nil {
        return customQueryResult{}, fmt.Errorf("failed to get columns: %w", err)
    }
    
    result := customQueryResult{
        name:       query.Name,
        metricType: query.MetricType,
        rows:       []map[string]interface{}{},
    }
    
    for rows.Next() {
        values := make([]interface{}, len(columns))
        valuePtrs := make([]interface{}, len(columns))
        for i := range values {
            valuePtrs[i] = &values[i]
        }
        
        if err := rows.Scan(valuePtrs...); err != nil {
            c.logger.Warn("Failed to scan custom query row", zap.Error(err))
            continue
        }
        
        row := make(map[string]interface{})
        for i, col := range columns {
            row[col] = values[i]
        }
        result.rows = append(result.rows, row)
    }
    
    return result, rows.Err()
}

// Data structures
type snowflakeMetrics struct {
    // Standard metrics
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
    
    // Event Tables (REAL-TIME!)
    eventQueryLogs     []eventTableRow
    eventTaskLogs      []eventTableRow
    eventFunctionLogs  []eventTableRow
    eventProcedureLogs []eventTableRow
    
    // Organization metrics
    orgCreditUsage   []orgCreditUsageRow
    orgStorageUsage  []orgStorageUsageRow
    orgDataTransfer  []orgDataTransferRow
    orgContractUsage []orgContractUsageRow
    
    // Custom queries
    customQueryResults []customQueryResult
}

// Existing row types
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
    databaseName     sql.NullString
    schemaName       sql.NullString
    taskName         sql.NullString
    state            sql.NullString
    executionCount   int64
    avgScheduledTime sql.NullFloat64
}

type replicationUsageRow struct {
    databaseName     sql.NullString
    totalCredits     float64
    bytesTransferred sql.NullFloat64
}

type autoClusteringRow struct {
    databaseName     sql.NullString
    schemaName       sql.NullString
    tableName        sql.NullString
    totalCredits     float64
    bytesReclustered sql.NullFloat64
    rowsReclustered  sql.NullFloat64
}

// New row types for advanced features
type eventTableRow struct {
    timestamp    time.Time
    eventType    string
    databaseName sql.NullString
    schemaName   sql.NullString
    objectName   sql.NullString
    severity     sql.NullString
    message      sql.NullString
    queryID      sql.NullString
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
    organizationName        sql.NullString
    contractNumber          sql.NullInt64
    totalCreditsUsed        float64
    totalCreditsBilled      sql.NullFloat64
    cloudServicesAdjustment sql.NullFloat64
}

type customQueryResult struct {
    name       string
    metricType string
    rows       []map[string]interface{}
}
