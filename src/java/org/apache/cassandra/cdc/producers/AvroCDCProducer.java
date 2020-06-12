package org.apache.cassandra.cdc.producers;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.cassandra.cdc.CDCProducer;
import org.apache.cassandra.cdc.MutationCDCInfo;
import org.apache.cassandra.cdc.producers.files.AvroFileTableWriter;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.metrics.MetricNameFactory;

/**
 * The Avro CDC Producer writes segments per table, using Avro Object Container Files.
 *
 * When a table schema changes, it creates a new segment with the table schema in the header.
 */
public class AvroCDCProducer implements CDCProducer
{
    private static final CompletableFuture<Void> completedFuture = CompletableFuture.completedFuture(null);
    private final AvroFileTableWriter writer = new AvroFileTableWriter();

    public CompletableFuture<Void> init(Map<String, Object> options, MetricNameFactory factory)
    {
        writer.init();
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<Void> send(Mutation mutation, MutationCDCInfo info)
    {
        Collection<PartitionUpdate> updates = mutation.getPartitionUpdates();
        CompletableFuture<?>[] futures = new CompletableFuture<?>[updates.size()];
        int index = 0;

        for (PartitionUpdate pkUpdate : updates)
        {
            futures[index++] = (pkUpdate.metadata().params.cdc) ? writer.append(pkUpdate, info.getSchemaVersion())
                                                                : completedFuture;
        }

        return CompletableFuture.allOf(futures);
    }

    public void close() throws Exception
    {
        writer.close();
    }
}
