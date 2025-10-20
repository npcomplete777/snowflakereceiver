package snowflakereceiver

import (
    "context"
    
    "go.opentelemetry.io/collector/component"
    "go.opentelemetry.io/collector/consumer"
    "go.opentelemetry.io/collector/receiver"
    "go.opentelemetry.io/collector/scraper"
    "go.opentelemetry.io/collector/scraper/scraperhelper"
)

const (
    typeStr = "snowflake"
)

var (
    typeVal = component.MustNewType(typeStr)
)

func NewFactory() receiver.Factory {
    return receiver.NewFactory(
        typeVal,
        createDefaultConfig,
        receiver.WithMetrics(createMetricsReceiver, component.StabilityLevelAlpha),
    )
}

func createMetricsReceiver(
    ctx context.Context,
    settings receiver.Settings,
    cfg component.Config,
    consumer consumer.Metrics,
) (receiver.Metrics, error) {
    
    snowflakeCfg := cfg.(*Config)
    
    // Use the shortest interval as base scrape interval
    interval := snowflakeCfg.GetBaseInterval()
    
    s, err := newSnowflakeScraper(settings, snowflakeCfg)
    if err != nil {
        return nil, err
    }
    
    scraperCfg := &scraperhelper.ControllerConfig{
        CollectionInterval: interval,
        InitialDelay:       interval,
    }
    
    // Create scraper with shutdown support
    sc, err := scraper.NewMetrics(
        s.scrape,
        scraper.WithShutdown(s.Shutdown),
    )
    if err != nil {
        return nil, err
    }
    
    return scraperhelper.NewMetricsController(
        scraperCfg,
        settings,
        consumer,
        scraperhelper.AddScraper(typeVal, sc),
    )
}
