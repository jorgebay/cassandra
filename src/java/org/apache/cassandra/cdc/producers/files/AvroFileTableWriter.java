package org.apache.cassandra.cdc.producers.files;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ExecutorUtils;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

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

    private void chunkFlushedHandler(VersionedSegmentManager tableVersion, ByteBuffer buffer)
    {
        // TODO: Send to another producer
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
