package org.apache.cassandra.cdc.producers.files;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.TableId;

/**
 * Represents a thread-safe writer of a table cdc log in Avro format.
 */
public class AvroFileTableWriter implements AutoCloseable
{
    private ConcurrentHashMap<TableId, TableSegmentManager> segmentManager = new ConcurrentHashMap<>();

    public void init()
    {
        //TODO: Initialize background flusher thread
    }

    public void close() throws Exception
    {

    }

    public CompletableFuture<Void> append(PartitionUpdate update, UUID schemaVersion)
    {
        //TODO:  serialize into thread local buffer

        TableSegmentManager manager = segmentManager.computeIfAbsent(update.metadata().id,
                                                                     k -> new TableSegmentManager());

        int length = 0;
        // allocate using the table metadata and schema version and the size
        FileSegmentAllocation allocation = manager.allocate(length, schemaVersion, update.metadata());

        // TODO: copy the contents

        // mark as block written
        allocation.markAsWritten();

        return allocation.whenFlushed();
    }
}
