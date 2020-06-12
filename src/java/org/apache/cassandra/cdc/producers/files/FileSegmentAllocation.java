package org.apache.cassandra.cdc.producers.files;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

interface FileSegmentAllocation
{
    /** Gets a future that is completed when the allocation is flushed */
    CompletableFuture<Void> whenFlushed();

    ByteBuffer getBuffer();

    void markAsWritten();

    void markAsFlushed();

    boolean wasWritten();
}
