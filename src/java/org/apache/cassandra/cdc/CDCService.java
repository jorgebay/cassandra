package org.apache.cassandra.cdc;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.CDCWriteException;
import org.apache.cassandra.metrics.CDCServiceMetrics;
import org.apache.cassandra.utils.MBeanWrapper;

/**
 * Contains logic for Change Data Capture (CDC) functionality.
 *
 * Wraps the {@link CDCProducer}, adding error handling and metrics.
 */
public final class CDCService
{
    private static final Logger logger = LoggerFactory.getLogger(CDCService.class);
    private static final String MBEAN_NAME = "org.apache.cassandra.cdc:type=CDCService";
    private static final long logTimePeriod = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);

    private final CDCProducer producer;
    private final CDCHealthCheckService healthCheck;
    private final CDCHintService hintService;
    // An instant view of the state of the CDC Producer
    private volatile State stateInstant = State.NOT_INITIALIZED;
    private final AtomicLong lastLatencyLogTime = new AtomicLong();
    private final AtomicLong latencyLogCounter = new AtomicLong();

    // TODO: Use conf
    private static final int MAX_HINTS = 100;
    private static final boolean USE_HINTS = true;
    private static final double MAX_FAILURE_RATIO = 0.3;
    private static final long LATENCY_ERROR_MS = 1000;
    private static final long LATENCY_WARNING_NANOS = 100_1000_1000L;

    CDCService(CDCProducer producer, CDCHealthCheckService healthCheck, CDCHintService hintService)
    {
        this.producer = producer;
        this.healthCheck = healthCheck;
        this.hintService = hintService;
    }

    public void init() throws IOException
    {
        try
        {
            registerMBean();

            producer.init(Collections.emptyMap()).get();
            healthCheck.init(MAX_FAILURE_RATIO, this::onStateChanged, this::getFailureRatio);
            hintService.init(producer, healthCheck);
        }
        catch (InterruptedException e)
        {
            throw new IOException("CDC Producer initialization was interrupted", e);
        }
        catch (ExecutionException e)
        {
            throw new IOException("CDC Producer could not be initialized", e);
        }

        stateInstant = State.OK;
    }

    private void onStateChanged(State state)
    {
        stateInstant = state;
    }

    /**
     * Gets the failure ratio of the producer send attempts.
     * @return The ratio or {@code Double.NaN} when there isn't enough information
     */
    private double getFailureRatio()
    {
        return Double.NaN;
    }

    private void registerMBean()
    {
        MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);
    }

    /**
     * Sends the mutation to the CDC.
     */
    public void send(Mutation mutation) throws CDCWriteException
    {
        // A cheap check whether the CDC Producer can be used
        // State might change after storing this value but the outcome should be as expected
        final State initialState = stateInstant;

        if (initialState == State.NOT_INITIALIZED)
        {
            throw new CDCWriteException("CDC Service can't send the mutation before initialization");
        }

        if (initialState == State.SHUTDOWN)
        {
            throw new CDCWriteException("CDC Service can't send after shutdown");
        }

        if (initialState == State.OK)
        {
            Exception ex = sendToProducer(mutation);
            if (ex == null) {
                // Succeeded
                return;
            }

            healthCheck.reportSendError(mutation, ex);
        }

        if (!USE_HINTS)
        {
            throw new CDCWriteException("CDC Producer failed to acknowledge the mutation");
        }

        if (!hintService.storeHint(mutation))
        {
            // Hints failed to be stored
            throw new CDCWriteException(String.format("CDC Service failure after %s hints stored", MAX_HINTS));
        }
    }

    //TODO: Remove and include log somewhere else
    private void handleSendError(Exception ex)
    {
        logger.debug("Mutation could not be acknowledge by the CDC producer", ex);
    }

    private Exception sendToProducer(Mutation mutation) {
        final long start = System.nanoTime();
        CDCServiceMetrics.producerMessagesInFlight.inc();
        try
        {
            CompletableFuture<Void> future = producer.send(mutation, DefaultMutationCDCInfo.clientRequestInfo);
            future.get(LATENCY_ERROR_MS, TimeUnit.MILLISECONDS);
            long latencyNanos = System.nanoTime() - start;
            CDCServiceMetrics.producerLatency.addNano(latencyNanos);
            maybeLogLatency(latencyNanos);
            return null;
        }
        catch (TimeoutException e)
        {
            CDCServiceMetrics.producerTimedOut.mark();
            CDCServiceMetrics.producerFailures.mark();
            // We are ignoring the response from the producer, so it better to ack the latency capped
            // at a maximum now that to don't record it
            CDCServiceMetrics.producerLatency.addNano(System.nanoTime() - start);
            return e;
        }
        catch (Exception e)
        {
            CDCServiceMetrics.producerFailures.mark();
            CDCServiceMetrics.producerLatency.addNano(System.nanoTime() - start);
            return e;
        }
        finally
        {
            CDCServiceMetrics.producerMessagesInFlight.dec();
        }
    }

    private void maybeLogLatency(long latencyNanos)
    {
        if (latencyNanos < LATENCY_WARNING_NANOS)
        {
            return;
        }

        long lastLog = lastLatencyLogTime.get();
        long now = System.currentTimeMillis();

        if (lastLog + logTimePeriod < now && lastLatencyLogTime.compareAndSet(lastLog, now))
        {
            long count = latencyLogCounter.getAndSet(0L);
            long latencyMillis = TimeUnit.MILLISECONDS.convert(latencyNanos, TimeUnit.NANOSECONDS);
            String message = String.format("CDC Producer took %dms (warn threshold is %dms)", latencyMillis,
                                           LATENCY_WARNING_NANOS);
            if (count > 0)
            {
                message += String.format(", while recording other %d mutations also exceeded the threshold", count);
            }
            logger.warn(message);
        }
        else
        {
            latencyLogCounter.incrementAndGet();
        }
    }

    public void shutdown()
    {
        stateInstant = State.SHUTDOWN;
        try
        {
            producer.close();
        }
        catch (Exception e)
        {
            logger.debug("There was an error trying to close the CDC producer", e);
        }
        hintService.shutdown();
        healthCheck.shutdown();
    }
}
