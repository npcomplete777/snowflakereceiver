package snowflakereceiver

import (
    "go.opentelemetry.io/collector/pdata/pcommon"
    "go.opentelemetry.io/collector/pdata/pmetric"
)

// addSelfMonitoringMetrics adds metrics about the receiver itself
func (s *snowflakeScraper) addSelfMonitoringMetrics(scopeMetrics pmetric.ScopeMetrics, now pcommon.Timestamp) {
    // Total queries executed
    queriesMetric := scopeMetrics.Metrics().AppendEmpty()
    queriesMetric.SetName("snowflake.receiver.queries.total")
    queriesMetric.SetDescription("Total number of queries executed by the receiver")
    queriesMetric.SetUnit("{queries}")
    queriesGauge := queriesMetric.SetEmptyGauge()
    queriesDp := queriesGauge.DataPoints().AppendEmpty()
    queriesDp.SetTimestamp(now)
    queriesDp.SetIntValue(s.client.queryCount)
    queriesDp.Attributes().PutStr("receiver.component", "snowflake")
    
    // Total errors
    errorsMetric := scopeMetrics.Metrics().AppendEmpty()
    errorsMetric.SetName("snowflake.receiver.errors.total")
    errorsMetric.SetDescription("Total number of errors encountered by the receiver")
    errorsMetric.SetUnit("{errors}")
    errorsGauge := errorsMetric.SetEmptyGauge()
    errorsDp := errorsGauge.DataPoints().AppendEmpty()
    errorsDp.SetTimestamp(now)
    errorsDp.SetIntValue(s.client.errorCount)
    errorsDp.Attributes().PutStr("receiver.component", "snowflake")
    
    // Total retries
    retriesMetric := scopeMetrics.Metrics().AppendEmpty()
    retriesMetric.SetName("snowflake.receiver.retries.total")
    retriesMetric.SetDescription("Total number of query retries performed by the receiver")
    retriesMetric.SetUnit("{retries}")
    retriesGauge := retriesMetric.SetEmptyGauge()
    retriesDp := retriesGauge.DataPoints().AppendEmpty()
    retriesDp.SetTimestamp(now)
    retriesDp.SetIntValue(s.client.retryCount)
    retriesDp.Attributes().PutStr("receiver.component", "snowflake")
    
    // Success rate
    if s.client.queryCount > 0 {
        successRate := float64(s.client.queryCount-s.client.errorCount) / float64(s.client.queryCount) * 100
        successMetric := scopeMetrics.Metrics().AppendEmpty()
        successMetric.SetName("snowflake.receiver.success.rate")
        successMetric.SetDescription("Success rate of queries (percentage)")
        successMetric.SetUnit("%")
        successGauge := successMetric.SetEmptyGauge()
        successDp := successGauge.DataPoints().AppendEmpty()
        successDp.SetTimestamp(now)
        successDp.SetDoubleValue(successRate)
        successDp.Attributes().PutStr("receiver.component", "snowflake")
    }
}
