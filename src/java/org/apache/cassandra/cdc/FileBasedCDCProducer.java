package org.apache.cassandra.cdc;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.cassandra.db.Mutation;

public class FileBasedCDCProducer implements CDCProducer
{
    public CompletableFuture<Void> init(Map<String, Object> options)
    {
        return null;
    }

    public CompletableFuture<Void> send(Mutation mutation, MutationCDCInfo info)
    {
        return null;
    }

    public void close() throws Exception
    {

    }
}
