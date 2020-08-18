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

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.metrics.MetricNameFactory;

/**
 * Represents a publisher of mutations events for Change Data Capture (CDC).
 */
public interface CDCProducer extends AutoCloseable
{
    /**
     * Initializes the {@link CDCProducer}.
     * It will be invoked once in the lifetime of the instance before any calls to send.
     * @param options The passthrough options.
     * @param factory The name factory that can be used to create custom metrics for the CDC Service.
     */
    CompletableFuture<Void> init(Map<String, Object> options, MetricNameFactory factory);

    /**
     * Publishes the mutation event.
     * It will only be invoked for mutations that should be tracked by the CDC.
     */
    CompletableFuture<Void> send(Mutation mutation, MutationCDCInfo info);
}
