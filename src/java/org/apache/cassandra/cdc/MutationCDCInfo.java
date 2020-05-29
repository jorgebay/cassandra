package org.apache.cassandra.cdc;

/**
 * Represents information about the mutation to be provided to CDC producers.
 */
public interface MutationCDCInfo
{
    /**
     * Gets the source of the mutation event.
     */
    EventSource getSource();
}