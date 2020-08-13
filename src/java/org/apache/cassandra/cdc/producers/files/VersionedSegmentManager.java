package org.apache.cassandra.cdc.producers.files;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.schema.TableMetadata;

/**
 * Represents a segment manager for a given table version.
 */
class VersionedSegmentManager
{
    private final TableMetadata table;
    private final LinkedBlockingQueue<FileSegmentAllocation> allocations = new LinkedBlockingQueue<>();

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

    /**
     * Retrieves and remove all written allocations made so far.
     */
    Collection<FileSegmentAllocation> pollAll()
    {
        List<FileSegmentAllocation> list = new LinkedList<>();
        for (FileSegmentAllocation item = allocations.peek(); item != null; item = allocations.peek())
        {
            if (!item.wasWritten())
            {
                // The item was not written by writter thread yet
                allocations.add(item);
                break;
            }

            allocations.remove();
            list.add(item);
        }

        return list;
    }
}
