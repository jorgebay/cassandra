package org.apache.cassandra.cdc.producers.files;

import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Collectors3;

/**
 * Represents a segment manager for a given table version.
 */
class VersionedSegmentManager
{
    private final TableMetadata table;
    private final CopyOnWriteArrayList<Segment> segments = new CopyOnWriteArrayList<>();
    //TODO: Replace with actual size
    private static final int SEGMENT_MAX_LENGTH = 32*1024*1024;

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
        // Get or create a segment where the allocation will be made
        FileSegmentAllocation allocation = null;
        while (allocation == null)
        {
            Segment segment = getAvailableSegment();
            if (segment == null)
            {
                segment = getOrCreateSegment();
            }
            allocation = segment.allocate(length);
        }

        return allocation;
    }

    /** Gets or add a new available segment using locks */
    private synchronized Segment getOrCreateSegment()
    {
        Segment segment = getAvailableSegment();
        if (segment != null)
        {
            return segment;
        }
        segment = new Segment(SEGMENT_MAX_LENGTH);
        segments.add(segment);
        return segment;
    }

    /** Optimistically gets a segment that can allocate */
    Segment getAvailableSegment()
    {
        for (Segment item : segments)
        {
            if (item.canAllocate())
            {
                return item;
            }
        }

        return null;
    }

    /** Gets a snapshot of the existing segments */
    Collection<Segment> getExistingSegments()
    {
        return segments.stream().collect(Collectors3.toImmutableList());
    }
}
