/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cdc.producers.files;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import org.apache.cassandra.cdc.producers.files.Segment.SegmentSubrange;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.SyncUtil;

/**
 * Responsible for flushing all written allocations.
 */
class Flusher
{
    private final ConcurrentHashMap<TableId, TableSegmentManager> segmentManager;
    private final BiConsumer<VersionedSegmentManager, ByteBuffer> onChunkFlushed;

    Flusher(ConcurrentHashMap<TableId, TableSegmentManager> segmentManager,
            BiConsumer<VersionedSegmentManager, ByteBuffer> onChunkFlushed)
    {
        this.segmentManager = segmentManager;
        this.onChunkFlushed = onChunkFlushed;
    }

    @SuppressWarnings("resource")
    void flush()
    {
        // TODO: Define when and how a channel can be closed and version removed from table
        Collection<TableSegmentManager> tables = this.segmentManager.values();
        for (TableSegmentManager t: tables)
        {
            for (VersionedSegmentManager tableVersion : t.getVersionedSegmentManagers())
            {
                Collection<Segment> segments = tableVersion.getExistingSegments();

                for (Segment s: segments)
                {
                    SegmentSubrange subrange = s.pollAll();
                    if (subrange.isEmpty())
                    {
                        continue;
                    }

                    Exception writeException = null;
                    ByteBuffer compressedBuffer = compress(subrange.getBuffer());
                    try
                    {
                        FileChannel channel = s.getChannel();
                        channel.write(compressedBuffer);
                        SyncUtil.force(channel, true);
                    }
                    catch (Exception e)
                    {
                        writeException = e;
                        //TODO: Mark file as errored
                    }

                    for (FileSegmentAllocation a : subrange.getAllocations())
                    {
                        a.markAsFlushed(writeException);
                    }

                    if (writeException == null)
                    {
                        onChunkFlushed.accept(tableVersion, (ByteBuffer) compressedBuffer.flip());
                    }
                }
            }
        }
    }

    /**
     * Gets a compressed chunk with CRC included.
     */
    private ByteBuffer compress(ByteBuffer buffer)
    {
        throw new RuntimeException("Not implemented");
    }

    public void close()
    {
        // TODO: pollAll() and invoke futures
        // TODO: Close all file handles
    }
}
