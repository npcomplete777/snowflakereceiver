# Snowflake Receiver - Deployment Guide

## 🚀 Quick Start

### Prerequisites
- OpenTelemetry Collector Builder (OCB) installed
- Go 1.21+ installed
- Snowflake account with appropriate permissions
- Access to ACCOUNT_USAGE schema

### Build the Collector

Clone the repository and build:
- git clone <your-repo-url>
- cd snowflakereceiver
- ocb --config builder-config-snowflake.yaml
- Binary will be at: ./otelcol-snowflake/otelcol-snowflake

## 📝 Configuration

### Minimal Configuration

Create a config.yaml with:
- receivers: snowflake with user, password, account, warehouse
- exporters: debug or your preferred backend
- service: pipeline connecting receiver to exporter

### Production Configuration

Includes all Phase 1 & 2 security and reliability settings:
- query_timeout: "30s" (prevents hangs)
- max_rows_per_query: 10000 (prevents DoS)
- rate_limit_qps: 10 (rate limiting)
- max_retries: 3 (retry logic)
- retry_initial_delay: "1s"
- retry_max_delay: "30s"

## 🔒 Security Best Practices

1. Use environment variables for credentials
   - export SNOWFLAKE_USER="service_account"
   - export SNOWFLAKE_PASSWORD="your-secure-password"

2. Grant minimal Snowflake permissions
   - CREATE ROLE OTEL_MONITORING
   - GRANT USAGE ON DATABASE/SCHEMA
   - GRANT SELECT ON REQUIRED VIEWS

3. Network security
   - Use private endpoints
   - Enable TLS/SSL
   - Restrict access via firewall

## 🏃 Running the Collector

Direct execution:
  ./otelcol-snowflake/otelcol-snowflake --config config.yaml

As systemd service:
  Create /etc/systemd/system/otel-snowflake.service
  sudo systemctl enable otel-snowflake
  sudo systemctl start otel-snowflake

As Docker container:
  docker build -t otel-snowflake .
  docker run with environment variables

## 📊 Self-Monitoring Metrics (Phase 2)

The receiver exports metrics about itself:
- snowflake.receiver.queries.total
- snowflake.receiver.errors.total  
- snowflake.receiver.retries.total
- snowflake.receiver.success.rate

## 🔧 Troubleshooting

High Error Rate:
- Check success rate metric
- Increase query_timeout or max_retries
- Verify network connectivity

Rate Limiting Issues:
- Decrease rate_limit_qps
- Increase metric intervals

Connection Errors:
- Verify credentials
- Check account name format
- Verify warehouse status

## ⚙️ Performance Tuning

For high-volume: Lower rate_limit_qps, increase intervals
For low-latency: Higher rate_limit_qps, shorter intervals

## 📈 Monitoring Recommendations

Alert on:
- Success rate < 95%
- High error count
- Excessive retries
- High credit usage

## 🎯 Best Practices

1. Start with conservative settings
2. Monitor self-metrics
3. Use appropriate intervals
4. Enable only needed metrics
5. Test changes in non-production
6. Rotate credentials regularly
7. Review logs regularly

## 📚 Resources

- Snowflake ACCOUNT_USAGE docs
- OpenTelemetry Collector docs
- Snowflake best practices

Version: Phase 1 & 2 (Security + Reliability)
Last Updated: October 2025
