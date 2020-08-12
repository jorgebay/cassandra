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
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.CDCServiceMetrics;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.utils.MBeanWrapper;

/**
 * Contains logic for Change Data Capture (CDC) functionality.
 *
 * Wraps the {@link CDCProducer}, adding error handling and metrics.
 */
public final class CDCService implements CDCServiceMBean
{
    private static final Logger logger = LoggerFactory.getLogger(CDCService.class);
    private static final String MBEAN_NAME = "org.apache.cassandra.cdc:type=CDCService";

    private final CDCConfig config;
    private final CDCProducer producer;
    private final CDCHealthCheckService healthCheck;
    private final CDCHintService hintService;
    // An instant view of the state of the CDC Producer
    private volatile State stateInstant = State.NOT_INITIALIZED;
    private final AtomicLong lastLatencyLogTime = new AtomicLong();
    private final AtomicLong latencyLogCounter = new AtomicLong();

    CDCService(CDCConfig config, CDCProducer producer, CDCHealthCheckService healthCheck, CDCHintService hintService)
    {
        this.config = config;
        this.producer = producer;
        this.healthCheck = healthCheck;
        this.hintService = hintService;
    }

    public void init() throws IOException
    {
        registerMBean();

        try
        {
            //TODO: pass options
            producer.init(Collections.emptyMap(), CDCServiceMetrics.factory).get();
        }
        catch (InterruptedException e)
        {
            throw new IOException("CDC Producer initialization was interrupted", e);
        }
        catch (ExecutionException e)
        {
            throw new IOException("CDC Producer could not be initialized", e);
        }

        healthCheck.init(config.getMaxFailureRatio(), this::onStateChanged, this::getFailureRatio);
        hintService.init(producer, healthCheck);
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
        // Allow multiple registrations for unit tests
        if (!MBeanWrapper.instance.isRegistered(MBEAN_NAME))
        {
            MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);
        }
    }

    /**
     * Sends the mutation to the CDC.
     */
    @Override
    public void publish(Mutation mutation) throws CDCWriteException
    {
        if (!mutation.trackedByCDC())
        {
            return;
        }

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

            logger.debug("Mutation could not be acknowledge by the CDC producer", ex);
            healthCheck.reportSendError(mutation, ex);
        }

        if (!config.useHints())
        {
            throw new CDCWriteException("CDC Producer failed to acknowledge the mutation");
        }

        if (!hintService.storeHint(mutation))
        {
            // Hints failed to be stored
            throw new CDCWriteException(String.format("CDC Service failure after %s hints stored", config.getMaxHints()));
        }
    }

    @Override
    public void delegatePublishing(InetAddressAndPort leader, Mutation mutation) throws CDCWriteException{
        if (!mutation.trackedByCDC())
        {
            return;
        }

        //TODO: Send message to leader
    }

    private Exception sendToProducer(Mutation mutation) {
        final long start = System.nanoTime();
        CDCServiceMetrics.producerMessagesInFlight.inc();
        try
        {
            DefaultMutationCDCInfo info = new DefaultMutationCDCInfo(EventSource.CLIENT_REQUEST,
                                                                     Schema.instance.getVersion());
            CompletableFuture<Void> future = producer.send(mutation, info);
            future.get(config.getLatencyErrorMs(), TimeUnit.MILLISECONDS);
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

            if (e instanceof ExecutionException && e.getCause() instanceof Exception)
            {
                // Try to unwrap
                return (Exception) e.getCause();
            }

            return e;
        }
        finally
        {
            CDCServiceMetrics.producerMessagesInFlight.dec();
        }
    }

    private void maybeLogLatency(long latencyNanos)
    {
        if (latencyNanos < config.getLatencyWarningNanos())
        {
            return;
        }

        // Log from time to time
        long lastLog = lastLatencyLogTime.get();
        long now = System.currentTimeMillis();

        if (lastLog + config.getLogTimePeriodMs() < now && lastLatencyLogTime.compareAndSet(lastLog, now))
        {
            String message = String.format("CDC Producer took %dms (warn threshold is %dms)",
                                           TimeUnit.MILLISECONDS.convert(latencyNanos, TimeUnit.NANOSECONDS),
                                           TimeUnit.MILLISECONDS.convert(config.getLatencyWarningNanos(),
                                                                         TimeUnit.NANOSECONDS));
            long count = latencyLogCounter.getAndSet(0L);
            if (count > 0)
            {
                message += String.format(", %d mutations also exceeded the threshold since last warning", count);
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
