package org.apache.cassandra.cdc;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.metrics.MetricNameFactory;

/**
 * Represents a publisher of mutations events for Change Data Capture (CDC).
 */
public interface CDCProducer extends AutoCloseable
{
    /**
     * Initializes the {@link CDCProducer}.
     * It will be invoked once in the lifetime of the instance before any calls to send.
     * @param options The passthrough options.
     * @param factory The name factory that can be used to create custom metrics for the CDC Service.
     */
    CompletableFuture<Void> init(Map<String, Object> options, MetricNameFactory factory);

    /**
     * Publishes the mutation event.
     * It will only be invoked for mutations that should be tracked by the CDC.
     */
    CompletableFuture<Void> send(Mutation mutation, MutationCDCInfo info);
}
