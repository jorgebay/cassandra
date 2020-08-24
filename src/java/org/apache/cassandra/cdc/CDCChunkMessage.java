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

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.UUIDSerializer;

/**
 * For CDC producers that support it (namely {@link org.apache.cassandra.cdc.producers.AvroCDCProducer}), it represents
 * an internode mesage for a CDC chunk to be replicated.
 */
public class CDCChunkMessage
{
    private final UUID leaderHostId;
    private final Chunk chunk;

    public CDCChunkMessage(UUID leaderHostId, Chunk chunk)
    {
        this.leaderHostId = leaderHostId;
        this.chunk = chunk;
    }

    public static final CDCChunkMessageSerializer serializer = new CDCChunkMessageSerializer();

    public UUID getLeaderHostId()
    {
        return leaderHostId;
    }

    public Chunk getChunk()
    {
        return chunk;
    }

    public static class CDCChunkMessageSerializer implements IVersionedSerializer<CDCChunkMessage>
    {
        public void serialize(CDCChunkMessage m, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(m.getLeaderHostId(), out, version);
            ChunkSerializer chunkSerializer = CDCService.instance.getChunkSerializer();
            chunkSerializer.serialize(m.getChunk(), out);
        }

        public CDCChunkMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            throw new RuntimeException("Not implemented");
        }

        public long serializedSize(CDCChunkMessage m, int version)
        {
            throw new RuntimeException("Not implemented");
        }
    }
}