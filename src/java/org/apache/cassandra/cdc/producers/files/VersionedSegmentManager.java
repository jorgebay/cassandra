package org.apache.cassandra.cdc.producers.files;

import java.util.concurrent.CompletableFuture;

import org.apache.cassandra.schema.TableMetadata;

/**
 * Represents a segment manager for a given table version.
 */
class VersionedSegmentManager
{
    private final TableMetadata table;

    VersionedSegmentManager(TableMetadata table)
    {
        this.table = table;
    }

    FileSegmentAllocation allocate(int length)
    {
        throw new RuntimeException("Not implemented");
    }
}
