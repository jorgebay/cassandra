package org.apache.cassandra.cdc;

import org.apache.cassandra.db.Mutation;

/**
 * Internal service that encapsulates hinting from the {@link CDCService}.
 */
interface CDCHintService
{
    /**
     * Initializes the {@link CDCHintService}.
     * @param producer The producer used to send back the hints.
     * @param healthCheck The health check used to report the different changes in the service.
     */
    void init(CDCProducer producer, CDCHealthCheckService healthCheck);

    /**
     * Attempts to save it to the underlying store.
     * @return true when it succeeds to store it.
     */
    boolean storeHint(Mutation mutation);

    /**
     * Cancels any scheduled sending to the producer.
     */
    void shutdown();
}
