package org.apache.cassandra.cdc.producers.files;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.Collections;
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
    private final ByteBuffer buffer;
    private final int maxLength;
    private int pollPosition;

    public Segment(int maxLength)
    {
        this.buffer = ByteBuffer.allocate(maxLength);
        this.maxLength = maxLength;
    }

    /**
     * Gets or creates the file channel.
     * Not thread safe.
     */
    public FileChannel getChannel()
    {
        throw new RuntimeException("Not implemented");
    }

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
            if (next >= maxLength)
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
     * Not thread safe.
     */
    SegmentSubrange pollAll()
    {
        List<FileSegmentAllocation> list = new LinkedList<>();
        int startIndex = pollPosition;
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

        if (list.size() == 0)
        {
            return SegmentSubrange.empty;
        }

        return new SegmentSubrange((ByteBuffer) buffer.duplicate().position(startIndex).limit(pollPosition), list);
    }

    /**
     * Represents information about a range within a segment, composed of a ByteBuffer (view) and a group
     * of allocations.
     */
    static class SegmentSubrange
    {
        private final ByteBuffer buffer;
        private final Collection<FileSegmentAllocation> allocations;

        private static final SegmentSubrange empty = new SegmentSubrange(
            ByteBuffer.allocate(0).asReadOnlyBuffer(),
            Collections.emptyList());

        SegmentSubrange(ByteBuffer buffer, Collection<FileSegmentAllocation> allocations)
        {
            this.buffer = buffer;
            this.allocations = allocations;
        }

        public ByteBuffer getBuffer()
        {
            return buffer;
        }

        public Collection<FileSegmentAllocation> getAllocations()
        {
            return allocations;
        }

        public boolean isEmpty()
        {
            return allocations.size() == 0;
        }
    }
}
