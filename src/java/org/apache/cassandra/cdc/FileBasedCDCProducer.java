package org.apache.cassandra.cdc;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.metrics.MetricNameFactory;

public class FileBasedCDCProducer implements CDCProducer
{
    public CompletableFuture<Void> init(Map<String, Object> options, MetricNameFactory factory)
    {
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<Void> send(Mutation mutation, MutationCDCInfo info)
    {
        return CompletableFuture.completedFuture(null);
    }

    public void close() throws Exception
    {

    }
}
