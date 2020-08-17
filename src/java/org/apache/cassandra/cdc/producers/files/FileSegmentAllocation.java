package org.apache.cassandra.cdc.producers.files;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

interface FileSegmentAllocation
{
    ByteBuffer getBuffer();

    void markAsWritten();

    void markAsFlushed(Exception e);

    //TODO: Determine where to call it
    void markAsReplicated(Exception e);

    boolean wasWritten();

    /** Gets a future that is completed when the allocation is flushed */
    CompletableFuture<Void> whenFlushed();

    CompletableFuture<Void> whenWrittenOnAllReplicas();

    CompletableFuture<Void> whenWrittenOnAReplica();

    int getLength();
}
