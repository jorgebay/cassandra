package org.apache.cassandra.cdc;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
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
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.CDCWriteException;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

public class CDCServiceTest
{
    private static final Mutation sampleMutation = null;
    private InMemoryAppender logAppender;

    @BeforeClass
    public static void setup()
    {
        System.setProperty("org.apache.cassandra.disable_mbean_registration", "true");
    }

    @AfterClass
    public static void teardown()
    {
        System.setProperty("org.apache.cassandra.disable_mbean_registration", "");
    }

    @Before
    public void beforeEach() {
        logAppender = new InMemoryAppender();
        Logger logger = (Logger) LoggerFactory.getLogger(CDCService.class);
        logger.addAppender(logAppender);
        logAppender.start();
    }

    @After
    public void afterEach() {
        Logger logger = (Logger) LoggerFactory.getLogger(CDCService.class);
        logger.detachAppender(logAppender);
    }

    @Test
    public void testProducerIsInitialized() throws IOException
    {
        CDCServiceFactory builder = new CDCServiceFactory();
        CDCService service = builder.build();
        service.init();

        verify(builder.producer, times(1)).init(any(), any());
    }

    @Test
    public void testSendThrowsAfterShutdown() throws IOException
    {
        CDCService service = new CDCServiceFactory().buildInit();
        service.shutdown();

        assertThrows(() -> service.send(null), "CDC Service can't send after shutdown");
    }

    @Test
    public void testSendReturnsWhenSuceeds() throws IOException
    {
        CDCServiceFactory builder = new CDCServiceFactory();
        CDCService service = builder.buildInit();
        when(builder.producer.send(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

        service.send(sampleMutation);
        verify(builder.producer, times(1)).send(eq(sampleMutation), any());
    }

    @Test
    public void testSendThrowsWhenErrorAndHintsDisabledAndCallsHealthCheck() throws IOException
    {
        CDCServiceFactory builder = new CDCServiceFactory();
        CDCService service = builder.buildInit();
        Exception ex = new Exception("Test error");
        when(builder.producer.send(any(), any())).thenReturn(failedFuture(ex));

        assertThrows(() -> service.send(sampleMutation), "CDC Producer failed to acknowledge the mutation");
        verify(builder.producer, times(1)).send(eq(sampleMutation), any());

        // Verify exception is reported back to the health check service
        verify(builder.healthCheck, times(1)).reportSendError(eq(sampleMutation), eq(ex));
    }

    @Test
    public void testSendLogsLatencyOncePerPeriod() throws IOException
    {
        CDCServiceFactory builder = new CDCServiceFactory();
        CDCService service = builder.buildInit();
        CompletableFuture<Void> future = new CompletableFuture<>();
        when(builder.producer.send(any(), any())).thenReturn(future);
        when(builder.config.getLatencyWarningNanos())
            .thenReturn(TimeUnit.NANOSECONDS.convert(1L, TimeUnit.MILLISECONDS));
        when(builder.config.getLogTimePeriodMs()).thenReturn(1000L);
        when(builder.config.getLatencyErrorMs()).thenReturn(Long.MAX_VALUE);
        AtomicReference<Throwable> result = new AtomicReference<>();
        Runnable task = () -> service.send(sampleMutation);

        // Complete the future in the background in 10ms aprox
        Executors.newCachedThreadPool().submit(() -> {
            Thread.sleep(10);
            future.complete(null);
            return null;
        });

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

    private static class CDCServiceFactory
    {
        CDCConfig config = mock(CDCConfig.class);
        CDCProducer producer = mock(CDCProducer.class);
        CDCHealthCheckService healthCheck = mock(CDCHealthCheckService.class);
        CDCHintService hintService = mock(CDCHintService.class);

        CDCServiceFactory()
        {
            when(producer.init(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

            // Use sanish config defaults
            when(config.getLatencyWarningNanos()).thenReturn(TimeUnit.NANOSECONDS.convert(100L, TimeUnit.MILLISECONDS));
            when(config.getLogTimePeriodMs()).thenReturn(TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS));
            when(config.getLatencyErrorMs()).thenReturn(TimeUnit.MILLISECONDS.convert(2, TimeUnit.SECONDS));
        }

        CDCService build()
        {
            return new CDCService(config, producer, healthCheck, hintService);
        }

        /** builds an initialized instance */
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
