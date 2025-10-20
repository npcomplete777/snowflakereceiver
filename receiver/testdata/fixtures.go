package testdata

import "database/sql"

// Mock Snowflake row data for testing
type MockCurrentQueryRow struct {
    WarehouseName    string
    QueryType        string
    ExecutionStatus  string
    QueryCount       int64
    AvgExecutionTime float64
    AvgBytesScanned  float64
}

type MockCreditUsageRow struct {
    WarehouseName       string
    TotalCredits        float64
    ComputeCredits      float64
    CloudServiceCredits float64
}

// Helper to create sql.NullString
func NewNullString(s string, valid bool) sql.NullString {
    return sql.NullString{String: s, Valid: valid}
}

// Helper to create sql.NullFloat64
func NewNullFloat64(f float64, valid bool) sql.NullFloat64 {
    return sql.NullFloat64{Float64: f, Valid: valid}
}

// Helper to create sql.NullInt64
func NewNullInt64(i int64, valid bool) sql.NullInt64 {
    return sql.NullInt64{Int64: i, Valid: valid}
}

// Sample valid config YAML
const ValidConfigYAML = `
user: "test_user"
password: "test_password"
account: "test-account"
warehouse: "TEST_WH"
database: "SNOWFLAKE"
schema: "ACCOUNT_USAGE"

metrics:
  current_queries:
    enabled: true
    interval: "1m"
  warehouse_load:
    enabled: true
    interval: "2m"
  query_history:
    enabled: false
  credit_usage:
    enabled: true
    interval: "5m"
  storage_metrics:
    enabled: false
  login_history:
    enabled: false
  data_pipeline:
    enabled: false
  database_storage:
    enabled: false
  task_history:
    enabled: false
  replication_usage:
    enabled: false
  auto_clustering_history:
    enabled: false

event_tables:
  enabled: false

organization:
  enabled: false

custom_queries:
  enabled: false
`

// Sample custom query config
const CustomQueryConfigYAML = `
user: "test_user"
password: "test_password"
account: "test-account"
warehouse: "TEST_WH"
database: "SNOWFLAKE"
schema: "ACCOUNT_USAGE"

metrics:
  current_queries:
    enabled: false
  warehouse_load:
    enabled: false
  query_history:
    enabled: false
  credit_usage:
    enabled: false
  storage_metrics:
    enabled: false
  login_history:
    enabled: false
  data_pipeline:
    enabled: false
  database_storage:
    enabled: false
  task_history:
    enabled: false
  replication_usage:
    enabled: false
  auto_clustering_history:
    enabled: false

event_tables:
  enabled: false

organization:
  enabled: false

custom_queries:
  enabled: true
  queries:
    - name: "test_query"
      interval: "10m"
      metric_type: "gauge"
      value_column: "METRIC_VALUE"
      label_columns: ["LABEL1", "LABEL2"]
      sql: "SELECT 'foo' as LABEL1, 'bar' as LABEL2, 42.0 as METRIC_VALUE"
`

// Invalid configs for negative testing
const MissingUserConfigYAML = `
password: "test_password"
account: "test-account"
warehouse: "TEST_WH"
`

const MissingPasswordConfigYAML = `
user: "test_user"
account: "test-account"
warehouse: "TEST_WH"
`

const MissingAccountConfigYAML = `
user: "test_user"
password: "test_password"
warehouse: "TEST_WH"
`

const MissingWarehouseConfigYAML = `
user: "test_user"
password: "test_password"
account: "test-account"
`
