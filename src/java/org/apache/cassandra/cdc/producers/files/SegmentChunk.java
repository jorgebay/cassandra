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
package org.apache.cassandra.cdc.producers.files;

import java.nio.ByteBuffer;

import org.apache.cassandra.cdc.Chunk;
import org.apache.cassandra.cdc.ChunkSerializer;
import org.apache.cassandra.cdc.producers.files.Segment.SegmentId;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * A {@link Chunk} representation for replication of file segment chunks.
 */
public class SegmentChunk implements Chunk
{
    public static final ChunkSerializer serializer = new Serializer();

    private final SegmentId segmentId;
    private final ByteBuffer data;

    SegmentChunk(SegmentId segmentId, ByteBuffer data)
    {
        this.segmentId = segmentId;
        this.data = data;
    }

    public ByteBuffer getData()
    {
        return data;
    }

    public SegmentId getSegmentId()
    {
        return segmentId;
    }

    static class Serializer implements ChunkSerializer
    {
        @Override
        public Chunk deserialize(DataInputPlus in)
        {
            throw new RuntimeException("Not implemented");
        }

        @Override
        public void serialize(Chunk chunk, DataOutputPlus out)
        {
            throw new RuntimeException("Not implemented");
        }
    }
}
