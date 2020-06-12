package org.apache.cassandra.cdc.producers.files;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.schema.TableMetadata;

/**
 * Represents a segment manager for a given table.
 */
class TableSegmentManager
{
    private final ConcurrentHashMap<Integer, VersionedSegmentManager> managers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, Integer> tableHashes = new ConcurrentHashMap<>();

    /**
     * Gets a region of a buffer to be used.
     *
     * Different segments are used per table version, as the table schema is included in the header.
     */
    FileSegmentAllocation allocate(int length, UUID schemaVersion, TableMetadata table)
    {
        // Quick check without calculating the hashcode each time
        Integer tableHashCode = tableHashes.computeIfAbsent(schemaVersion, k -> table.hashCode());
        VersionedSegmentManager m = managers.computeIfAbsent(tableHashCode, k -> new VersionedSegmentManager(table));

        return m.allocate(length);
    }
}
