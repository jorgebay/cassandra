package org.apache.cassandra.cdc;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.cassandra.db.Mutation;

/**
 * Represents a publisher of mutations events for Change Data Capture (CDC).
 */
public interface CDCProducer extends AutoCloseable
{
    CompletableFuture<Void> init(Map<String, Object> options);

    CompletableFuture<Void> send(Mutation mutation, MutationCDCInfo info);
}
