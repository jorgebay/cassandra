package org.apache.cassandra.cdc;

import java.util.UUID;

/**
 * Represents information about the mutation to be provided to CDC producers.
 */
public interface MutationCDCInfo
{
    /**
     * Gets the source of the mutation event.
     */
    EventSource getSource();

    /**
     * Gets the schema version at the time the mutation was reported.
     */
    UUID getSchemaVersion();
}