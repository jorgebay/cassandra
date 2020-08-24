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


import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cdc.CDCProducer;
import org.apache.cassandra.exceptions.CDCWriteException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.concurrent.SimpleCondition;
import static org.apache.cassandra.utils.MonotonicClock.approxTime;

final class ReplicationRequestCallback implements RequestCallback
{
    private static final Logger logger = LoggerFactory.getLogger(CDCProducer.class);
    private final int blockFor;
    private final int totalSent;

    enum Outcome
    {
        SUCCESS
        {
            Exception toException() { return null; }
        },
        FAILURE
        {
            Exception toException() { return new CDCWriteException("Followers failed to acknowledge with write"); }
        },
        INTERRUPTED
        {
            Exception toException() { return new CDCWriteException("Followers write was interrupted"); }
        },
        TIMEOUT
        {
            Exception toException()
            {
                return new CDCWriteException("Followers failed to acknowledge with write before timeout elapsed");
            }
        };

        abstract Exception toException();
    }

    private final long start = approxTime.now();
    private final SimpleCondition condition = new SimpleCondition();
    private final AtomicInteger successCounter = new AtomicInteger();
    private final AtomicInteger receivedCounter = new AtomicInteger();
    private volatile Outcome outcome = Outcome.FAILURE;

    ReplicationRequestCallback(int blockFor, int totalSent)
    {
        this.blockFor = blockFor;
        this.totalSent = totalSent;
    }

    Outcome await()
    {
        boolean timedOut;
        try
        {
            timedOut = !condition.awaitUntil(Verb.CDC_CHUNK_REQ.expiresAtNanos(start));
        }
        catch (InterruptedException e)
        {
            logger.warn("CDC chunk dispatch was interrupted", e);
            return Outcome.INTERRUPTED;
        }

        return timedOut ? Outcome.TIMEOUT : outcome;
    }

    @Override
    public boolean invokeOnFailure()
    {
        return true;
    }

    @Override
    public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
    {
        markAsReceived();
    }

    @Override
    public void onResponse(Message msg)
    {
        int count = successCounter.incrementAndGet();
        if (count == blockFor)
        {
            outcome = Outcome.SUCCESS;
            condition.signalAll();
        }
        markAsReceived();
    }

    private void markAsReceived()
    {
        int count = receivedCounter.incrementAndGet();
        if (count == totalSent)
        {
            condition.signalAll();
        }
    }

    @Override
    public boolean supportsBackPressure()
    {
        return true;
    }
}
