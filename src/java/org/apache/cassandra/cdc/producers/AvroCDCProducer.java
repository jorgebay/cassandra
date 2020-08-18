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
package org.apache.cassandra.cdc.producers;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.cassandra.cdc.CDCProducer;
import org.apache.cassandra.cdc.MutationCDCInfo;
import org.apache.cassandra.cdc.producers.files.AvroFileTableWriter;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.metrics.MetricNameFactory;

/**
 * The Avro CDC Producer writes segments per table, using Avro Object Container Files.
 *
 * When a table schema changes, it creates a new segment with the table schema in the header.
 */
public class AvroCDCProducer implements CDCProducer
{
    private static final CompletableFuture<Void> completedFuture = CompletableFuture.completedFuture(null);
    private final AvroFileTableWriter writer = new AvroFileTableWriter();

    @Override
    public CompletableFuture<Void> init(Map<String, Object> options, MetricNameFactory factory)
    {
        //TODO: Use settings
        writer.init(2L, AvroFileTableWriter.Acks.NONE);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> send(Mutation mutation, MutationCDCInfo info)
    {
        Collection<PartitionUpdate> updates = mutation.getPartitionUpdates();
        CompletableFuture<?>[] futures = new CompletableFuture<?>[updates.size()];
        int index = 0;

        for (PartitionUpdate pkUpdate : updates)
        {
            futures[index++] = (pkUpdate.metadata().params.cdc) ? writer.append(pkUpdate, info.getSchemaVersion())
                                                                : completedFuture;
        }

        return CompletableFuture.allOf(futures);
    }

    public void close() throws Exception
    {
        writer.close();
    }
}
