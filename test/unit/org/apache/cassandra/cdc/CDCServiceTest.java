package org.apache.cassandra.cdc;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.CDCWriteException;
import org.apache.cassandra.metrics.CDCServiceMetrics;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.mockito.ArgumentCaptor;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

public class CDCServiceTest
{
    private static Mutation sampleMutation;
    private InMemoryAppender logAppender;

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        TableMetadata table = TableMetadata.builder("ks1", "tbl1")
                                           .partitioner(Murmur3Partitioner.instance)
                                           .params(TableParams.builder().cdc(true).build())
                                           .addPartitionKeyColumn("key0", UTF8Type.instance).build();
        PartitionUpdate pUpdate =
            PartitionUpdate.emptyUpdate(table, table.partitioner.decorateKey(ByteBufferUtil.bytes("key0")));
        sampleMutation = new Mutation(pUpdate);
        System.setProperty("org.apache.cassandra.disable_mbean_registration", "true");
    }

    @AfterClass
    public static void teardown()
    {
        System.setProperty("org.apache.cassandra.disable_mbean_registration", "");
    }

    @Before
    public void beforeEach()
    {
        logAppender = new InMemoryAppender();
        Logger logger = (Logger) LoggerFactory.getLogger(CDCService.class);
        logger.addAppender(logAppender);
        logAppender.start();
    }

    @After
    public void afterEach()
    {
        Logger logger = (Logger) LoggerFactory.getLogger(CDCService.class);
        logger.detachAppender(logAppender);
    }

    @Test
    public void shouldInitializeTheProducer() throws IOException
    {
        ServiceBuilder builder = new ServiceBuilder();
        CDCService service = builder.build();
        service.init();

        verify(builder.producer, times(1)).init(any(), any());
    }

    @Test
    public void shouldThrowWhenSendIsCalledAfterShutdown() throws IOException
    {
        CDCService service = new ServiceBuilder().buildInit();
        service.shutdown();

        assertThrows(() -> service.publish(sampleMutation), "CDC Service can't send after shutdown");
    }

    @Test
    public void shouldCallProducerSend() throws IOException
    {
        ServiceBuilder builder = new ServiceBuilder();
        CDCService service = builder.buildInit();
        when(builder.producer.send(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

        service.publish(sampleMutation);
        verify(builder.producer, times(1)).send(eq(sampleMutation), any());
    }

    @Test
    public void shouldNotCallProducerWhenCdcIsDisabledForTable() throws IOException
    {
        ServiceBuilder builder = new ServiceBuilder();
        CDCService service = builder.buildInit();
        when(builder.producer.send(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

        TableMetadata table = TableMetadata.builder("ks1", "tbl1")
                                           .partitioner(Murmur3Partitioner.instance)
                                           // With CDC=false
                                           .params(TableParams.builder().cdc(false).build())
                                           .addPartitionKeyColumn("key0", UTF8Type.instance).build();
        PartitionUpdate pUpdate = PartitionUpdate.emptyUpdate(table, table.partitioner.decorateKey(ByteBufferUtil.bytes("key0")));
        Mutation mutation = new Mutation(pUpdate);
        service.publish(mutation);
        verify(builder.producer, times(0)).send(any(), any());
    }

    @Test
    public void shouldThrowOnErrorWhenHintsDisabled() throws IOException
    {
        testThrow(new ServiceBuilder(), "CDC Producer failed to acknowledge the mutation");
    }

    @Test
    public void shouldThrowOnErrorWhenHintCanNotBeStored() throws IOException
    {
        ServiceBuilder builder = new ServiceBuilder().withHintsEnabled();
        when(builder.hintService.storeHint(any())).thenReturn(false);
        testThrow(builder, "CDC Service failure after");
    }

    private void testThrow(ServiceBuilder builder, String errorMessage) throws IOException
    {
        CDCService service = builder.buildInit();
        Exception ex = new Exception("Test error");
        when(builder.producer.send(any(), any())).thenReturn(failedFuture(ex));

        assertThrows(() -> service.publish(sampleMutation), errorMessage);
        verify(builder.producer, times(1)).send(eq(sampleMutation), any());

        // Verify exception is reported back to the health check service
        verify(builder.healthCheck, times(1)).reportSendError(eq(sampleMutation), eq(ex));
    }

    @Test
    public void shouldNotThrowWhenHintsCanBeStored() throws IOException
    {
        ServiceBuilder builder = new ServiceBuilder().withHintsEnabled();
        when(builder.hintService.storeHint(any())).thenReturn(true);
        CDCService service = builder.buildInit();
        Exception ex = new Exception("Test error");
        when(builder.producer.send(any(), any())).thenReturn(failedFuture(ex));

        service.publish(sampleMutation);
        verify(builder.producer, times(1)).send(eq(sampleMutation), any());
        verify(builder.healthCheck, times(1)).reportSendError(eq(sampleMutation), eq(ex));
    }

    @Test
    public void shouldLogLatencyWarningOncePerPeriod() throws IOException
    {
        ServiceBuilder builder = new ServiceBuilder();
        CDCService service = builder.buildInit();
        when(builder.producer.send(any(), any())).then(i -> delayedFuture(10));
        when(builder.config.getLatencyWarningNanos()).thenReturn(NANOSECONDS.convert(1L, MILLISECONDS));
        when(builder.config.getLogTimePeriodMs()).thenReturn(5000L);
        when(builder.config.getLatencyErrorMs()).thenReturn(Long.MAX_VALUE);
        AtomicReference<Throwable> result = new AtomicReference<>();
        Runnable task = () -> service.publish(sampleMutation);

        final int length = 10;
        List<Thread> threads = IntStream.range(0, length).boxed()
                                        .map(i -> new Thread(task)).collect(toList());
        threads.forEach(t -> {
            t.setUncaughtExceptionHandler((t1, e) -> result.set(e));
            t.start();
        });

        threads.forEach(thread -> {
            try
            {
                thread.join();
            }
            catch (InterruptedException e)
            {
                result.set(e);
            }
        });

        verify(builder.producer, times(length)).send(eq(sampleMutation), any());

        // Verify o error
        assertThat(result.get(), equalTo(null));

        // Verify single warning
        List<ILoggingEvent> events = logAppender.events.stream()
                                                       .filter(l -> l.getLevel() == Level.WARN)
                                                       .collect(Collectors.toList());
        assertThat(events.size(), equalTo(1));
        assertThat(events.get(0).getMessage(), containsString("CDC Producer took"));
    }

    @Test
    public void testSendMetricsCountsInFlight() throws IOException
    {
        assertThat(CDCServiceMetrics.producerMessagesInFlight.getCount(), equalTo(0L));
        ServiceBuilder builder = new ServiceBuilder();
        CDCService service = builder.buildInit();
        CompletableFuture<Void> future = new CompletableFuture<>();
        AtomicReference<Throwable> exReference = new AtomicReference<>();
        AtomicLong result = new AtomicLong();
        when(builder.producer.send(any(), any())).thenReturn(future);

        final long length = 3;
        List<Thread> threads = IntStream.range(0, (int) length).boxed()
                                        .map(i -> new Thread(() -> service.publish(sampleMutation))).collect(toList());
        threads.forEach(t -> {
            t.setUncaughtExceptionHandler((t1, e) -> exReference.set(e));
            t.start();
        });

        Thread threadThatWaits = new Thread(() -> {
            for (int i = 0; i < 10; i++)
            {
                long count = CDCServiceMetrics.producerMessagesInFlight.getCount();
                result.set(count);
                if (count == length)
                {
                    break;
                }
                try
                {
                    Thread.sleep(20);
                }
                catch (InterruptedException e)
                {
                    exReference.set(e);
                }
            }

            future.complete(null);
        });
        threadThatWaits.start();

        threads.add(threadThatWaits);

        threads.forEach(thread -> {
            try
            {
                thread.join(2000);
            }
            catch (InterruptedException e)
            {
                exReference.set(e);
            }
        });

        verify(builder.producer, times((int) length)).send(eq(sampleMutation), any());
        assertThat(exReference.get(), equalTo(null));
        assertThat(result.get(), equalTo(length));
    }

    @Test
    public void testSendMarksTimeout() throws IOException
    {
        ServiceBuilder builder = new ServiceBuilder();
        CDCService service = builder.buildInit();
        when(builder.producer.send(any(), any())).then(i -> delayedFuture(20));
        when(builder.config.getLatencyWarningNanos()).thenReturn(NANOSECONDS.convert(1L, MILLISECONDS));
        when(builder.config.getLogTimePeriodMs()).thenReturn(5000L);
        when(builder.config.getLatencyErrorMs()).thenReturn(10L);

        long initialCount = CDCServiceMetrics.producerTimedOut.getCount();
        assertThrows(() -> service.publish(sampleMutation), null);
        assertThat(CDCServiceMetrics.producerTimedOut.getCount(), equalTo(initialCount + 1));
    }

    @Test
    public void shouldMarkFailures() throws IOException
    {
        long initialCount = CDCServiceMetrics.producerFailures.getCount();
        testThrow(new ServiceBuilder(), null);
        assertThat(CDCServiceMetrics.producerFailures.getCount(), equalTo(initialCount + 1));
    }

    @Test
    public void shouldRejectSendingAfterConsideredUnhealthy() throws IOException
    {
        ServiceBuilder builder = new ServiceBuilder().captureStateHandler()
                                                     .withProducerSendVoid()
                                                     .withHintsEnabled();

        CDCService service = builder.buildInit();
        Consumer<State> handler = builder.valueCapture.getValue();

        service.publish(sampleMutation);
        // Mark it as unhealthy
        handler.accept(State.UNHEALTHY);
        assertThrows(() -> service.publish(sampleMutation), "CDC Service failure after");
    }

    @Test
    public void shouldContinueSendingAfterConsideredHealthy() throws IOException
    {
        ServiceBuilder builder = new ServiceBuilder().captureStateHandler()
                                                     .withProducerSendVoid()
                                                     .withHintsEnabled();

        CDCService service = builder.buildInit();
        Consumer<State> handler = builder.valueCapture.getValue();

        // First successfull send
        service.publish(sampleMutation);

        handler.accept(State.UNHEALTHY);
        assertThrows(() -> service.publish(sampleMutation), "CDC Service failure after");

        // Mark healthy back again
        handler.accept(State.OK);

        service.publish(sampleMutation);
        verify(builder.producer, times(2)).send(eq(sampleMutation), any());
    }

    private static CompletableFuture<Void> delayedFuture(long millis)
    {
        CompletableFuture<Void> future = new CompletableFuture<>();
        // Complete the future in the background
        Executors.newCachedThreadPool().submit(() -> {
            Thread.sleep(millis);
            future.complete(null);
            return null;
        });
        return future;
    }

    private static class ServiceBuilder
    {
        CDCConfig config = mock(CDCConfig.class);
        CDCProducer producer = mock(CDCProducer.class);
        CDCHealthCheckService healthCheck = mock(CDCHealthCheckService.class);
        CDCHintService hintService = mock(CDCHintService.class);
        ArgumentCaptor<Consumer<State>> valueCapture;

        ServiceBuilder()
        {
            when(producer.init(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

            // Use sanish config defaults
            when(config.getLatencyWarningNanos()).thenReturn(NANOSECONDS.convert(100L, MILLISECONDS));
            when(config.getLogTimePeriodMs()).thenReturn(MILLISECONDS.convert(1, TimeUnit.DAYS));
            when(config.getLatencyErrorMs()).thenReturn(MILLISECONDS.convert(2, TimeUnit.SECONDS));
        }

        @SuppressWarnings("unchecked")
        ServiceBuilder captureStateHandler()
        {
            valueCapture = ArgumentCaptor.forClass(Consumer.class);
            doNothing().when(healthCheck).init(anyDouble(), valueCapture.capture(), any());
            return this;
        }

        ServiceBuilder withProducerSendVoid()
        {
            when(producer.send(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
            return this;
        }

        ServiceBuilder withHintsEnabled()
        {
            when(config.useHints()).thenReturn(true);
            return this;
        }

        CDCService build()
        {
            return new CDCService(config, producer, healthCheck, hintService);
        }

        /**
         * builds an initialized instance
         */
        CDCService buildInit() throws IOException
        {
            CDCService builder = build();
            builder.init();
            return builder;
        }
    }

    private static void assertThrows(Runnable f, String message)
    {
        try
        {
            f.run();
            fail("Expected CDCWriteException to be thrown");
        }
        catch (CDCWriteException ex)
        {
            if (message != null)
            {
                assertThat(ex.getMessage(), containsString(message));
            }
        }
    }

    private static CompletableFuture<Void> failedFuture(Exception ex)
    {
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.completeExceptionally(ex);
        return future;
    }

    private static class InMemoryAppender extends AppenderBase<ILoggingEvent>
    {
        private final List<ILoggingEvent> events = newArrayList();

        private InMemoryAppender()
        {
            start();
        }

        @Override
        protected synchronized void append(ILoggingEvent event)
        {
            events.add(event);
        }
    }
}
