package org.apache.cassandra.cdc.producers.files;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Represents a single file backed by a single buffer where different "regions" are allocated for different mutations.
 */
class Segment
{
    private final AtomicReference<State> state = new AtomicReference<>(State.OPEN);
    private final LinkedBlockingQueue<FileSegmentAllocation> allocations = new LinkedBlockingQueue<>();
    private final AtomicInteger position = new AtomicInteger();
    private final AtomicInteger allocating = new AtomicInteger();
    //TODO: Replace with actual size
    private static final int MAX_LENGTH = 32*1024*1024;

    private enum State
    {
        OPEN, CLOSING, CLOSED
    }

    boolean canAllocate()
    {
        return state.get() == State.OPEN;
    }

    /**
     * A thread-safe allocator of regions.
     */
    FileSegmentAllocation allocate(int length)
    {
        allocating.incrementAndGet();
        if (state.get() != State.OPEN)
        {
            allocating.decrementAndGet();
            return null;
        }
        int start = movePosition(length);

        if (start == -1)
        {
            allocating.decrementAndGet();
            state.compareAndSet(State.OPEN, State.CLOSING);
            return null;
        }

        // TODO: Make sure flusher use the position from the allocation, not the position from here
        FileSegmentAllocation item = new DefaultFileSegmentAllocation(start, length);
        allocations.add(item);
        allocating.decrementAndGet();
        return item;
    }

    private int movePosition(int length)
    {
        while (true)
        {
            int start = position.get();
            int next = start + length;
            if (next >= MAX_LENGTH)
            {
                return -1;
            }
            if (position.compareAndSet(start, next))
            {
                return start;
            }
        }
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
