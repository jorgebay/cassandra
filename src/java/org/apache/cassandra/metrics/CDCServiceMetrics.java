package org.apache.cassandra.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Holds the CDC Service metrics.
 */
public final class CDCServiceMetrics
{
    private static final MetricNameFactory factory = new DefaultNameFactory("CDCService");

    public static final Meter hintsCreated = Metrics.meter(factory.createMetricName("HintsCreated"));
    public static final Meter hintsNotCreated = Metrics.meter(factory.createMetricName("HintsNotCreated"));
    public static final Counter producerMessagesInFlight = Metrics.counter(factory.createMetricName("ProducerMessagesInFlight"));
    public static final Meter producerFailures = Metrics.meter(factory.createMetricName("ProducerFailures"));
    public static final Meter producerTimedOut = Metrics.meter(factory.createMetricName("ProducerTimedOut"));
    public static final LatencyMetrics producerLatency = new LatencyMetrics(factory, "ProducerLatency");
}
