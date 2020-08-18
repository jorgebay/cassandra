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
