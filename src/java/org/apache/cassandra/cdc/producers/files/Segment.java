package org.apache.cassandra.cdc.producers.files;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Represents a single file backed by a single buffer where different "regions" are allocated for different mutations.
 */
class Segment
{
    private final AtomicReference<State> state = new AtomicReference<>(State.OPEN);
    private final ConcurrentHashMap<Integer, FileSegmentAllocation> allocations = new ConcurrentHashMap<>();
    private final AtomicInteger position = new AtomicInteger();
    private final AtomicInteger allocating = new AtomicInteger();
    private int pollPosition;
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

        FileSegmentAllocation item = new DefaultFileSegmentAllocation(start, length);
        allocations.put(start, item);
        allocating.decrementAndGet();
        return item;
    }

    boolean tryClose()
    {
        // Order of these conditions matter
        if (state.get() == State.CLOSING && allocating.get() == 0)
        {
            return state.compareAndSet(State.CLOSING, State.CLOSED);
        }

        return false;
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
     * Not thread-safe.
     */
    Collection<FileSegmentAllocation> pollAll()
    {
        List<FileSegmentAllocation> list = new LinkedList<>();
        while (pollPosition < position.get())
        {
            FileSegmentAllocation item = allocations.get(pollPosition);
            if (item == null || !item.wasWritten())
            {
                // The allocation is not ready to be flushed yet
                break;
            }

            allocations.remove(pollPosition);
            list.add(item);
            pollPosition += item.getLength();
        }

        return list;
    }
}
