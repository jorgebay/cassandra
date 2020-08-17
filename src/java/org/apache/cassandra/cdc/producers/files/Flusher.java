package org.apache.cassandra.cdc.producers.files;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import org.apache.cassandra.schema.TableId;

/**
 * Responsible for flushing all written allocations.
 */
class Flusher
{
    private final ConcurrentHashMap<TableId, TableSegmentManager> segmentManager;
    private final BiConsumer<VersionedSegmentManager, ByteBuffer[]> onChunkFlushed;

    Flusher(ConcurrentHashMap<TableId, TableSegmentManager> segmentManager, BiConsumer<VersionedSegmentManager, ByteBuffer[]> onChunkFlushed)
    {
        this.segmentManager = segmentManager;
        this.onChunkFlushed = onChunkFlushed;
    }

    void flush()
    {
//        Collection<TableSegmentManager> tableSegments = this.segmentManager.values();
//        for (TableSegmentManager t: tableSegments)
//        {
//            for (VersionedSegmentManager tableVersion : t.getVersionedSegmentManagers())
//            {
//                Collection<Segment> segments = tableVersion.getExistingSegments();
//                if (segments.size() == 0) {
//                    // TODO: Define when and how a channel can be closed and version removed from table
//                }
//
//                // TODO: Get or open file for the table version
//                FileChannel channel = getChannel();
//
//                Exception writeException = null;
//
//                try
//                {
//                    //channel.write(buffer);
//                }
//                catch (IOException e)
//                {
//                    writeException = e;
//                    //TODO: Mark file as errored
//                }
//
//                for (FileSegmentAllocation a : allocations)
//                {
//                    a.markAsFlushed(writeException);
//                }
//
//                if (writeException == null)
//                {
//                    onChunkFlushed.accept(tableVersion, buffers);
//                }
//            }
//        }
    }

    public void close()
    {
        // TODO: pollAll() and invoke futures
        // TODO: Close all file handles
    }

    private static FileChannel getChannel()
    {
        throw new RuntimeException("Not implemented: placeholder");
    }
}
