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
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.cassandra.cdc.CDCChunkMessage;
import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ExecutorUtils;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.net.Verb.CDC_CHUNK_REQ;

/**
 * Represents a thread-safe writer of a table cdc log in Avro format.
 */
public class AvroFileTableWriter implements AutoCloseable
{
    private Acks acks;

    public enum Acks
    {
        NONE, ONE, ALL
    }

    private final ConcurrentHashMap<TableId, TableSegmentManager> segmentManager = new ConcurrentHashMap<>();
    private final ScheduledExecutorService flushExecutor;
    private final Flusher flusher = new Flusher(segmentManager, this::chunkFlushedHandler);

    public AvroFileTableWriter()
    {
        ScheduledThreadPoolExecutor executor = new DebuggableScheduledThreadPoolExecutor("CDCFileWriterTasks");
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        flushExecutor = executor;
    }

    public void init(long flushDelayMs, Acks acks)
    {
        this.acks = acks;
        //TODO: Maybe schedule only after a write was issued
        flushExecutor.scheduleWithFixedDelay(flusher::flush,
                                             StorageService.RING_DELAY,
                                             flushDelayMs,
                                             MILLISECONDS);

    }

    public CompletableFuture<Void> append(PartitionUpdate update, UUID schemaVersion)
    {
        //TODO:  serialize into thread local buffer

        TableSegmentManager manager = segmentManager.computeIfAbsent(update.metadata().id,
                                                                     k -> new TableSegmentManager());

        int length = 0;
        // allocate using the table metadata and schema version and the size
        FileSegmentAllocation allocation = manager.allocate(length, schemaVersion, update.metadata());

        // TODO: copy the contents

        // mark block as written
        allocation.markAsWritten();

        CompletableFuture<Void> future = allocation.whenFlushed();

        switch (acks)
        {
            case ONE:
                return future.thenCompose(f -> allocation.whenWrittenOnAReplica());
            case ALL:
                return future.thenCompose(f -> allocation.whenWrittenOnAllReplicas());
            default:
                return future;
        }
    }

    private void chunkFlushedHandler(Segment segment, Collection<FileSegmentAllocation> allocations, ByteBuffer buffer)
    {
        // this method is invoked on the flusher thread, we shouldn't block on the flusher thread
        CDC_CHUNK_REQ.stage.execute(() -> sendToFollowers(segment, allocations, buffer));
    }

    private void sendToFollowers(Segment segment, Collection<FileSegmentAllocation> allocations, ByteBuffer buffer)
    {
        Message<CDCChunkMessage> message = Message.out(CDC_CHUNK_REQ,
                                                       new CDCChunkMessage(StorageService.instance.getLocalHostUUID(),
                                                                           new SegmentChunk(segment.getId(), buffer)));

        List<InetAddressAndPort> followers = getFollowers();
        int blockFor;
        switch (acks)
        {
            case ALL:
                blockFor = followers.size();
                break;
            case ONE:
                blockFor = 1;
                break;
            default:
                blockFor = 0;
                break;
        }

        ReplicationRequestCallback callback = new ReplicationRequestCallback(blockFor, followers.size());
        for (InetAddressAndPort endpoint : followers)
        {
            MessagingService.instance().sendWithCallback(message, endpoint, callback);
        }

        ReplicationRequestCallback.Outcome outcome = callback.await();
        Exception replicationException = outcome.toException();
        for (FileSegmentAllocation a : allocations)
        {
            a.markAsReplicated(replicationException);
        }
    }

    /**
     * Gets a group of nodes that are selected as replicas for the segments.
     * The group is the same as long as the hosts are UP and the topology doesn't change.
     */
    private List<InetAddressAndPort> getFollowers()
    {
        throw new RuntimeException("Not implemented");
    }

    public void close() throws Exception
    {
        Exception closeException = null;
        try
        {
            flushExecutor.submit(flusher::close).get(10L, SECONDS);
        }
        catch (Exception e)
        {
            closeException = e;
        }

        ExecutorUtils.shutdownAndWait(2L, SECONDS, flushExecutor);

        if (closeException != null)
        {
            throw closeException;
        }
    }
}
