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

import java.io.IOException;
import java.util.UUID;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.CDCWriteException;
import org.apache.cassandra.locator.InetAddressAndPort;

public interface CDCServiceMBean
{
    void init() throws IOException;

    /**
     * Publishes the mutation locally using the producer, acting as leader of the CDC write.
     */
    void publish(Mutation mutation) throws CDCWriteException;

    /**
     * Uses a replica in the query plan as leader of the mutation, without using the local producer.
     * <p>For coordinator-only nodes, the local CDC producer should not be configured and one of the replicas of the
     * mutation plan is used as leader instead.</p>
     */
    void delegatePublishing(InetAddressAndPort leader, Mutation mutation) throws CDCWriteException;

    void storeAsReplica(UUID leaderHostId, Chunk chunk) throws CDCWriteException;

    /**
     * When supported, it returns the instance used to serialize and deserialize chunks for replication.
     * @return The instance when supported, otherwise {@code null}.
     */
    ChunkSerializer getChunkSerializer();

    void shutdown();
}
