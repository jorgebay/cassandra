package org.apache.cassandra.cdc;

import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.cassandra.db.Mutation;

/**
 * Internal service to encapsulate health check logic from the {@link CDCService}.
 *
 * Every health status change is reported back to the {@code stateChangeHandler}.
 */
interface CDCHealthCheckService
{
    /**
     * Initializes the health check service.
     */
    void init(double maxFailureRatio, Consumer<State> stateChangeHandler, Supplier<Double> failureRatioSupplier);

    /**
     * When invoked, it marks the service as unhealthy.
     */
    void reportHintsReachedMax();

    /**
     * When invoked, it assesses the health of the service.
     */
    void reportHintSentToProducer();

    /**
     * When invoked, it assesses the health of the service after an error.
     */
    void reportSendError(Mutation mutation, Exception ex);

    void shutdown();
}
