package org.apache.cassandra.cdc;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.cassandra.db.Mutation;

class DefaultHealthCheckService implements CDCHealthCheckService
{
    private double maxFailureRatio;
    private Consumer<State> stateChangeHandler;
    private Supplier<Double> failureRatioSupplier;
    private final AtomicReference<State> status = new AtomicReference<>(State.NOT_INITIALIZED);

    @Override
    public void init(double maxFailureRatio, Consumer<State> stateChangeHandler, Supplier<Double> failureRatioSupplier)
    {
        this.maxFailureRatio = maxFailureRatio;
        this.stateChangeHandler = stateChangeHandler;
        this.failureRatioSupplier = failureRatioSupplier;
        status.set(State.OK);
    }

    @Override
    public void reportHintsReachedMax()
    {
        // Update state only when it was ok
        status.compareAndSet(State.OK, State.UNHEALTHY);
    }

    @Override
    public void reportHintSentToProducer()
    {
        if (status.get() != State.UNHEALTHY)
        {
            // The quick check determined that the rest is unnecessary
            return;
        }

        double ratio = failureRatioSupplier.get();
        if (ratio < maxFailureRatio && status.compareAndSet(State.UNHEALTHY, State.OK))
        {
            stateChangeHandler.accept(State.OK);
        }
    }

    @Override
    public void reportSendError(Mutation mutation, Exception ex)
    {
        if (status.get() != State.OK)
        {
            // The quick check determined that the rest is unnecessary
            return;
        }

        double ratio = failureRatioSupplier.get();
        if (ratio >= maxFailureRatio && status.compareAndSet(State.OK, State.UNHEALTHY))
        {
            stateChangeHandler.accept(State.UNHEALTHY);
        }
    }

    @Override
    public void shutdown()
    {
        status.set(State.SHUTDOWN);
        stateChangeHandler.accept(State.SHUTDOWN);
    }
}
