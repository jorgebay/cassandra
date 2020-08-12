package org.apache.cassandra.cdc;

import java.io.IOException;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.CDCWriteException;
import org.apache.cassandra.locator.InetAddressAndPort;

public interface CDCServiceMBean
{
    void init() throws IOException;

    /**
     * Publishes the mutation locally using the producer, acting as leader of the CDC write.
     */
    void publish(Mutation mutation) throws CDCWriteException;

    /**
     * Uses a replica in the query plan as leader of the mutation, without using the local producer.
     * <p>For coordinator-only nodes, the local CDC producer should not be configured and one of the replicas of the
     * mutation plan is used as leader instead.</p>
     */
    void delegatePublishing(InetAddressAndPort leader, Mutation mutation) throws CDCWriteException;

    void shutdown();
}
