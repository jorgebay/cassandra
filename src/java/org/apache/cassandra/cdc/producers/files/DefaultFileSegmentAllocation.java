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
import java.util.concurrent.CompletableFuture;

class DefaultFileSegmentAllocation implements FileSegmentAllocation
{
    private final int start;
    private final int length;
    private volatile boolean isWritten;

    DefaultFileSegmentAllocation(int start, int length)
    {
        this.start = start;
        this.length = length;
    }

    @Override
    public ByteBuffer getBuffer()
    {
        return null;
    }

    @Override
    public void markAsWritten()
    {
        isWritten = true;
    }

    @Override
    public void markAsFlushed(Exception e)
    {

    }

    @Override
    public void markAsReplicated(Exception e)
    {

    }

    @Override
    public boolean wasWritten()
    {
        return isWritten;
    }

    @Override
    public CompletableFuture<Void> whenFlushed()
    {
        return null;
    }

    @Override
    public CompletableFuture<Void> whenWrittenOnAllReplicas()
    {
        return null;
    }

    @Override
    public CompletableFuture<Void> whenWrittenOnAReplica()
    {
        return null;
    }

    @Override
    public int getLength()
    {
        return length;
    }
}
