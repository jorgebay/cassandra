package org.apache.cassandra.cdc.producers.files;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.cdc.producers.files.Segment.SegmentSubrange;

import static org.apache.cassandra.cdc.CDCTestUtil.invokeParallel;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class SegmentTest
{
    @Test
    public void shouldSupportConcurrentAllocations()
    {
        Segment segment = new SegmentBuilder().build();
        final int times = 20;
        // Invoke n times trying to allocate buffers of 10, 20, 30, ... byte length
        List<FileSegmentAllocation> result = allocateParallel(segment, times);

        assertThat(result.size(), equalTo(times));
        assertThat(segment.getPosition(), equalTo(result.stream().mapToInt(FileSegmentAllocation::getLength).sum()));

        // Verify that internally it is structured correctly
        Map<Integer, FileSegmentAllocation> allocationMap = segment.getAllocationPositions();
        List<Integer> indexes = allocationMap.keySet().stream().sorted().collect(Collectors.toList());

        int position = 0;
        for (int index : indexes)
        {
            assertThat(index, equalTo(position));
            position += allocationMap.get(index).getLength();
        }
    }

    @Test
    public void pollAllShouldGetContiguousWrittenRanges()
    {
        Segment segment = new SegmentBuilder().build();
        allocateParallel(segment, 10);
        Map<Integer, FileSegmentAllocation> allocationMap = segment.getAllocationPositions();

        List<FileSegmentAllocation> expectedToPoll = new ArrayList<>();

        // As no items have been written yet, it should empty
        assertThat(segment.pollAll().isEmpty(), equalTo(true));

        FileSegmentAllocation forthAllocation = null;
        final int length = 10;
        int index = 0;
        // Mark all as written except the 4th allocation (index == 3)
        for (int i = 0; i < length; i++)
        {
            FileSegmentAllocation allocation = allocationMap.get(index);
            index += allocation.getLength();
            if (i == 3)
            {
                forthAllocation = allocation;
                continue;
            }

            allocation.markAsWritten();
            if (i < 3)
            {
                expectedToPoll.add(allocation);
            }
        }

        // It should have only obtain the first 3
        SegmentSubrange subrange = segment.pollAll();
        assertThat(subrange.getAllocations().size(), equalTo(3));
        assertEquals(expectedToPoll, subrange.getAllocations());
        assertThat(subrange.getBuffer().remaining(), equalTo(subrange.getAllocations()
                                                                     .stream()
                                                                     .mapToInt(FileSegmentAllocation::getLength)
                                                                     .sum()));

        // Next time should empty, until more items are written
        assertThat(segment.pollAll().isEmpty(), equalTo(true));

        assert forthAllocation != null;
        forthAllocation.markAsWritten();

        // Now the rest of the
        assertThat(segment.pollAll().getAllocations().size(), equalTo(length - 3));
    }

    private static List<FileSegmentAllocation> allocateParallel(Segment segment, int amount)
    {
        AtomicInteger counter = new AtomicInteger();
        // Invoke n times trying to allocate buffers of 10, 20, 30, ... byte length
        return invokeParallel(() -> {
            int length = 10 * counter.incrementAndGet();
            Thread.sleep(10);
            return segment.allocate(length);
        }, amount, 10);
    }

    private static class SegmentBuilder
    {
        private static final int maxLength = 10000;

        Segment build()
        {
            return new Segment(maxLength);
        }
    }
}
