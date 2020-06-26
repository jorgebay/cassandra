package org.apache.cassandra.cdc.producers.files;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.schema.TableMetadata;

/**
 * Represents a segment manager for a given table.
 */
class TableSegmentManager
{
    private final ConcurrentHashMap<UUID, VersionedSegmentManager> managers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, VersionedSegmentManager> managersByHashCode = new ConcurrentHashMap<>();

    /**
     * Gets a region of a buffer to be used.
     *
     * Different segments are used per table version, as the table schema is included in the header.
     */
    FileSegmentAllocation allocate(int length, UUID schemaVersion, TableMetadata table)
    {
        VersionedSegmentManager m = managers.computeIfAbsent(schemaVersion, k -> {
            // Try to obtain an existing version manager for the table hash code
            int hashCode = table.hashCode();
            VersionedSegmentManager mById = managersByHashCode.computeIfAbsent(hashCode,
                                                                               h -> new VersionedSegmentManager(table));

            // Check table hashcode collisions
            return !table.equals(mById.getTable())
                ? new VersionedSegmentManager(table)
                : mById;
        });

        return m.allocate(length);
    }
}
