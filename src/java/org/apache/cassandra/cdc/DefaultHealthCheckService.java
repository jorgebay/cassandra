/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
        if (status.compareAndSet(State.OK, State.UNHEALTHY))
        {
            stateChangeHandler.accept(State.UNHEALTHY);
        }
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
