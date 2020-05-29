package org.apache.cassandra.cdc;

enum State
{
    NOT_INITIALIZED,
    OK,
    UNHEALTHY,
    SHUTDOWN
}
