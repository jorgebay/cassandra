package org.apache.cassandra.cdc.producers.files;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

class DefaultFileSegmentAllocation implements FileSegmentAllocation
{
    private final int start;
    private final int length;

    DefaultFileSegmentAllocation(int start, int length)
    {
        this.start = start;
        this.length = length;
    }

    @Override
    public ByteBuffer getBuffer()
    {
        return null;
    }

    @Override
    public void markAsWritten()
    {

    }

    @Override
    public void markAsFlushed(Exception e)
    {

    }

    @Override
    public void markAsReplicated(Exception e)
    {

    }

    @Override
    public boolean wasWritten()
    {
        return false;
    }

    @Override
    public CompletableFuture<Void> whenFlushed()
    {
        return null;
    }

    @Override
    public CompletableFuture<Void> whenWrittenOnAllReplicas()
    {
        return null;
    }

    @Override
    public CompletableFuture<Void> whenWrittenOnAReplica()
    {
        return null;
    }
}
