package snowflakereceiver

import (
    "testing"
    "time"
    
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

func TestConfig_Validate(t *testing.T) {
    tests := []struct {
        name        string
        config      *Config
        expectError bool
        errorMsg    string
    }{
        {
            name: "valid config",
            config: &Config{
                User:      "test_user",
                Password:  "test_password",
                Account:   "test-account",
                Warehouse: "TEST_WH",
                Database:  "SNOWFLAKE",
                Schema:    "ACCOUNT_USAGE",
            },
            expectError: false,
        },
        {
            name: "missing user",
            config: &Config{
                Password:  "test_password",
                Account:   "test-account",
                Warehouse: "TEST_WH",
            },
            expectError: true,
            errorMsg:    "user is required",
        },
        {
            name: "missing password",
            config: &Config{
                User:      "test_user",
                Account:   "test-account",
                Warehouse: "TEST_WH",
            },
            expectError: true,
            errorMsg:    "password is required",
        },
        {
            name: "missing account",
            config: &Config{
                User:      "test_user",
                Password:  "test_password",
                Warehouse: "TEST_WH",
            },
            expectError: true,
            errorMsg:    "account is required",
        },
        {
            name: "missing warehouse",
            config: &Config{
                User:     "test_user",
                Password: "test_password",
                Account:  "test-account",
            },
            expectError: true,
            errorMsg:    "warehouse is required",
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            err := tt.config.Validate()
            
            if tt.expectError {
                require.Error(t, err)
                assert.Contains(t, err.Error(), tt.errorMsg)
            } else {
                require.NoError(t, err)
            }
        })
    }
}

func TestMetricCategoryConfig_GetInterval(t *testing.T) {
    defaultInterval := 5 * time.Minute
    
    tests := []struct {
        name             string
        config           MetricCategoryConfig
        defaultInterval  time.Duration
        expectedInterval time.Duration
    }{
        {
            name: "valid interval string",
            config: MetricCategoryConfig{
                Enabled:  true,
                Interval: "2m",
            },
            defaultInterval:  defaultInterval,
            expectedInterval: 2 * time.Minute,
        },
        {
            name: "empty interval returns default",
            config: MetricCategoryConfig{
                Enabled:  true,
                Interval: "",
            },
            defaultInterval:  defaultInterval,
            expectedInterval: defaultInterval,
        },
        {
            name: "invalid interval returns default",
            config: MetricCategoryConfig{
                Enabled:  true,
                Interval: "invalid",
            },
            defaultInterval:  defaultInterval,
            expectedInterval: defaultInterval,
        },
        {
            name: "seconds interval",
            config: MetricCategoryConfig{
                Enabled:  true,
                Interval: "30s",
            },
            defaultInterval:  defaultInterval,
            expectedInterval: 30 * time.Second,
        },
        {
            name: "hours interval",
            config: MetricCategoryConfig{
                Enabled:  true,
                Interval: "1h",
            },
            defaultInterval:  defaultInterval,
            expectedInterval: 1 * time.Hour,
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result := tt.config.GetInterval(tt.defaultInterval)
            assert.Equal(t, tt.expectedInterval, result)
        })
    }
}

func TestConfig_GetBaseInterval(t *testing.T) {
    tests := []struct {
        name             string
        config           *Config
        expectedInterval time.Duration
    }{
        {
            name: "returns default 30s minimum",
            config: &Config{
                Metrics: MetricsConfig{
                    CurrentQueries: MetricCategoryConfig{
                        Enabled:  true,
                        Interval: "1m",
                    },
                    WarehouseLoad: MetricCategoryConfig{
                        Enabled:  true,
                        Interval: "2m",
                    },
                },
            },
            expectedInterval: 30 * time.Second,
        },
        {
            name: "returns shortest interval when event tables enabled",
            config: &Config{
                Metrics: MetricsConfig{
                    CurrentQueries: MetricCategoryConfig{
                        Enabled:  true,
                        Interval: "1m",
                    },
                },
                EventTables: EventTablesConfig{
                    Enabled: true,
                    QueryLogs: MetricCategoryConfig{
                        Enabled:  true,
                        Interval: "20s",
                    },
                },
            },
            expectedInterval: 20 * time.Second,
        },
        {
            name: "returns default when no intervals specified",
            config: &Config{
                Metrics: MetricsConfig{
                    CurrentQueries: MetricCategoryConfig{
                        Enabled:  true,
                        Interval: "",
                    },
                },
            },
            expectedInterval: 30 * time.Second,
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result := tt.config.GetBaseInterval()
            assert.Equal(t, tt.expectedInterval, result)
        })
    }
}

func TestCreateDefaultConfig(t *testing.T) {
    cfg := createDefaultConfig().(*Config)
    
    assert.Equal(t, "SNOWFLAKE", cfg.Database)
    assert.Equal(t, "ACCOUNT_USAGE", cfg.Schema)
    assert.True(t, cfg.Metrics.CurrentQueries.Enabled)
    assert.True(t, cfg.Metrics.QueryHistory.Enabled)
    assert.False(t, cfg.EventTables.Enabled)
    assert.False(t, cfg.Organization.Enabled)
}

func TestOrgCreditUsageColumns_Defaults(t *testing.T) {
    cols := OrgCreditUsageColumns{}
    
    assert.Equal(t, "ORGANIZATION_NAME", cols.GetOrganizationName())
    assert.Equal(t, "ACCOUNT_NAME", cols.GetAccountName())
    assert.Equal(t, "CREDITS", cols.GetCreditsUsed())
}

func TestCustomQuery_GetInterval(t *testing.T) {
    defaultInterval := 10 * time.Minute
    
    query := CustomQuery{
        Name:     "test_query",
        Interval: "5m",
    }
    
    result := query.GetInterval(defaultInterval)
    assert.Equal(t, 5*time.Minute, result)
}
