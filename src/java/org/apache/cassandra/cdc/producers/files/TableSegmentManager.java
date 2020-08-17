package org.apache.cassandra.cdc.producers.files;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.schema.TableMetadata;

/**
 * Manages segments and allocations for a given table.
 * <p>A {@link TableSegmentManager} owns one or more {@link VersionedSegmentManager} instances (one per
 * different table version), at the same time a {@link VersionedSegmentManager} owns one or more active segments.</p>
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

    Set<VersionedSegmentManager> getVersionedSegmentManagers()
    {
        return new HashSet<>(managers.values());
    }
}
