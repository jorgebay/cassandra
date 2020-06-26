package org.apache.cassandra.cdc.producers.files;

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

    TableMetadata getTable()
    {
        return this.table;
    }

    FileSegmentAllocation allocate(int length)
    {
        throw new RuntimeException("Not implemented");
    }
}
