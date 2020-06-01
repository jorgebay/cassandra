package org.apache.cassandra.cdc;

import java.io.IOException;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.CDCWriteException;

public interface CDCServiceMBean
{
    void init() throws IOException;

    void send(Mutation mutation) throws CDCWriteException;

    void shutdown();
}
