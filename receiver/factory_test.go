package snowflakereceiver

import (
    "context"
    "testing"
    
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
    "go.opentelemetry.io/collector/component"
    "go.opentelemetry.io/collector/component/componenttest"
    "go.opentelemetry.io/collector/consumer/consumertest"
    "go.opentelemetry.io/collector/receiver"
)

func TestNewFactory(t *testing.T) {
    factory := NewFactory()
    
    assert.NotNil(t, factory)
    assert.Equal(t, component.MustNewType("snowflake"), factory.Type())
}

func TestFactory_CreateDefaultConfig(t *testing.T) {
    factory := NewFactory()
    cfg := factory.CreateDefaultConfig()
    
    require.NotNil(t, cfg)
    
    snowflakeCfg, ok := cfg.(*Config)
    require.True(t, ok)
    
    assert.Equal(t, "SNOWFLAKE", snowflakeCfg.Database)
    assert.Equal(t, "ACCOUNT_USAGE", snowflakeCfg.Schema)
}

func TestFactory_CreateMetricsReceiver(t *testing.T) {
    factory := NewFactory()
    cfg := &Config{
        User:      "test",
        Password:  "test",
        Account:   "test-account",
        Warehouse: "TEST_WH",
        Database:  "SNOWFLAKE",
        Schema:    "ACCOUNT_USAGE",
        Metrics: MetricsConfig{
            CurrentQueries: MetricCategoryConfig{
                Enabled:  true,
                Interval: "1m",
            },
        },
    }
    
    ctx := context.Background()
    settings := receiver.Settings{
        ID:                component.MustNewID("snowflake"),
        TelemetrySettings: componenttest.NewNopTelemetrySettings(),
    }
    
    consumer := consumertest.NewNop()
    
    recv, err := factory.CreateMetrics(ctx, settings, cfg, consumer)
    
    require.NoError(t, err)
    assert.NotNil(t, recv)
}

func TestFactory_CreateMetricsReceiver_MinimalConfig(t *testing.T) {
    factory := NewFactory()
    cfg := &Config{
        User:      "test",
        Password:  "test",
        Account:   "test-account",
        Warehouse: "TEST_WH",
    }
    
    ctx := context.Background()
    settings := receiver.Settings{
        ID:                component.MustNewID("snowflake"),
        TelemetrySettings: componenttest.NewNopTelemetrySettings(),
    }
    
    consumer := consumertest.NewNop()
    
    recv, err := factory.CreateMetrics(ctx, settings, cfg, consumer)
    
    // Should still create receiver even with minimal config
    assert.NoError(t, err)
    assert.NotNil(t, recv)
}

func TestFactory_Type(t *testing.T) {
    factory := NewFactory()
    
    expectedType := component.MustNewType("snowflake")
    assert.Equal(t, expectedType, factory.Type())
}

func TestFactory_CreateMetricsReceiver_AllMetricsEnabled(t *testing.T) {
    factory := NewFactory()
    cfg := &Config{
        User:      "test",
        Password:  "test",
        Account:   "test-account",
        Warehouse: "TEST_WH",
        Database:  "SNOWFLAKE",
        Schema:    "ACCOUNT_USAGE",
        Metrics: MetricsConfig{
            CurrentQueries: MetricCategoryConfig{
                Enabled:  true,
                Interval: "1m",
            },
            WarehouseLoad: MetricCategoryConfig{
                Enabled:  true,
                Interval: "1m",
            },
            QueryHistory: MetricCategoryConfig{
                Enabled:  true,
                Interval: "5m",
            },
            CreditUsage: MetricCategoryConfig{
                Enabled:  true,
                Interval: "5m",
            },
            StorageMetrics: MetricCategoryConfig{
                Enabled:  true,
                Interval: "30m",
            },
            LoginHistory: MetricCategoryConfig{
                Enabled:  true,
                Interval: "10m",
            },
            DataPipeline: MetricCategoryConfig{
                Enabled:  true,
                Interval: "10m",
            },
            DatabaseStorage: MetricCategoryConfig{
                Enabled:  true,
                Interval: "30m",
            },
            TaskHistory: MetricCategoryConfig{
                Enabled:  true,
                Interval: "10m",
            },
            ReplicationUsage: MetricCategoryConfig{
                Enabled:  true,
                Interval: "15m",
            },
            AutoClusteringHistory: MetricCategoryConfig{
                Enabled:  true,
                Interval: "15m",
            },
        },
        EventTables: EventTablesConfig{
            Enabled:   true,
            TableName: "SNOWFLAKE.ACCOUNT_USAGE.EVENT_TABLE",
            QueryLogs: MetricCategoryConfig{
                Enabled:  true,
                Interval: "30s",
            },
            TaskLogs: MetricCategoryConfig{
                Enabled:  true,
                Interval: "30s",
            },
            FunctionLogs: MetricCategoryConfig{
                Enabled:  true,
                Interval: "30s",
            },
            ProcedureLogs: MetricCategoryConfig{
                Enabled:  true,
                Interval: "30s",
            },
        },
        Organization: OrganizationConfig{
            Enabled: true,
            OrgCreditUsage: OrgCreditUsageConfig{
                Enabled:  true,
                Interval: "1h",
            },
            OrgStorageUsage: OrgStorageUsageConfig{
                Enabled:  true,
                Interval: "1h",
            },
            OrgDataTransfer: OrgDataTransferConfig{
                Enabled:  true,
                Interval: "1h",
            },
            OrgContractUsage: OrgContractUsageConfig{
                Enabled:  true,
                Interval: "12h",
            },
        },
        CustomQueries: CustomQueriesConfig{
            Enabled: true,
            Queries: []CustomQuery{
                {
                    Name:         "test_query",
                    Interval:     "5m",
                    MetricType:   "gauge",
                    ValueColumn:  "VALUE",
                    LabelColumns: []string{"LABEL"},
                    SQL:          "SELECT 'test' as LABEL, 1.0 as VALUE",
                },
            },
        },
    }
    
    ctx := context.Background()
    settings := receiver.Settings{
        ID:                component.MustNewID("snowflake"),
        TelemetrySettings: componenttest.NewNopTelemetrySettings(),
    }
    
    consumer := consumertest.NewNop()
    
    recv, err := factory.CreateMetrics(ctx, settings, cfg, consumer)
    
    require.NoError(t, err)
    assert.NotNil(t, recv)
}
