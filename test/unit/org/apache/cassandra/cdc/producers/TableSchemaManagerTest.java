package org.apache.cassandra.cdc.producers;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.cassandra.cdc.producers.AsyncCDCProducer.TableSchemaManager;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assume.assumeThat;

public class TableSchemaManagerTest
{
    @Test
    public void shouldCallCreateWhenThereIsANewVersion() throws ExecutionException, InterruptedException
    {
        AtomicInteger counter = new AtomicInteger();
        TableSchemaManager m = newInstance(counter);

        CompletableFuture<UUID> result1 = m.ensureCreated(UUID.randomUUID(), newTable(1));
        assertThat(counter.get(), equalTo(1));

        CompletableFuture<UUID> result2 = m.ensureCreated(UUID.randomUUID(), newTable(2));
        assertThat(counter.get(), equalTo(2));

        assertThat(result1.get(), not(equalTo(result2.get())));
    }

    @Test
    public void shouldNotCallCreateWhenTheVersionMatches()
    {
        AtomicInteger counter = new AtomicInteger();
        TableSchemaManager m = newInstance(counter);
        TableMetadata table = newTable(1);
        UUID id = UUID.randomUUID();

        m.ensureCreated(id, table);
        assertThat(counter.get(), equalTo(1));
        m.ensureCreated(id, table);
        assertThat(counter.get(), equalTo(1));
    }

    @Test
    public void shouldNotCallCreateWhenTheVersionMatchesInParallel()
    {
        AtomicInteger counter = new AtomicInteger();
        TableSchemaManager m = newInstance(counter);
        TableMetadata table = newTable(1);
        UUID id = UUID.randomUUID();

        invokeParallel(() -> m.ensureCreated(id, table), 8);
        assertThat(counter.get(), equalTo(1));
    }

    @Test
    public void shouldNotCallCreateWhenThereIsANewVersionAndTableIsTheSame() throws ExecutionException, InterruptedException
    {
        AtomicInteger counter = new AtomicInteger();
        TableSchemaManager m = newInstance(counter);
        TableMetadata table = newTable(1);
        UUID id1 = UUID.randomUUID();

        CompletableFuture<UUID> result1 = m.ensureCreated(id1, table);
        assertThat(counter.get(), equalTo(1));

        // Use a different schema version with same table
        CompletableFuture<UUID> result2 = m.ensureCreated(UUID.randomUUID(), table);

        // Create was invoked only once
        assertThat(counter.get(), equalTo(1));
        assertThat(result1.get(), equalTo(id1));
        assertThat(result2.get(), equalTo(id1));
    }

    @Test
    public void shouldNotCallCreateWhenThereIsANewVersionAndTableIsTheSameInParallel()
    {
        AtomicInteger counter = new AtomicInteger();
        TableSchemaManager m = newInstance(counter);
        TableMetadata table = newTable(1);
        final int length = 16;

        // Use a different schema version with same table
        List<CompletableFuture<UUID>> results = invokeParallel(() -> m.ensureCreated(UUID.randomUUID(), table), length);

        // Create was invoked only once
        assertThat(counter.get(), equalTo(1));

        // All futures are the same instance (from a single call to send)
        CompletableFuture<?> firstFuture = results.get(0);
        for (int i = 1; i < results.size(); i++)
        {
            assertThat(results.get(i), sameInstance(firstFuture));
        }
    }

    @Test
    public void shouldCallCreateWhenThereIsANewVersionAndTableHashCodeCollidesButNotEqual()
    {
        String name1 = "Aa";
        String name2 = "BB";
        assumeThat(name1.hashCode(), equalTo(name2.hashCode()));

        // Create 2 tables with same hash code
        TableId tableId = TableId.generate();
        TableMetadata table1 = TableMetadata.builder("ks1", name1, tableId)
                                            .addPartitionKeyColumn("key1", UTF8Type.instance).build();
        TableMetadata table2 = TableMetadata.builder("ks1", name2, tableId)
                                            .addPartitionKeyColumn("key1", UTF8Type.instance).build();

        assertThat(table1.hashCode(), equalTo(table2.hashCode()));
        assertNotEquals(table1, table2);

        AtomicInteger counter = new AtomicInteger();
        TableSchemaManager m = newInstance(counter);
        m.ensureCreated(UUID.randomUUID(), table1);
        m.ensureCreated(UUID.randomUUID(), table2);

        // Create was invoked once per table
        assertThat(counter.get(), equalTo(2));
    }

    @Test
    public void shouldCallCreateWhenThereIsANewVersionAndTableHashCodeCollidesButNotEqualInParallel()
    {
        String name1 = "Aa";
        String name2 = "BB";
        assumeThat(name1.hashCode(), equalTo(name2.hashCode()));

        // Create 2 tables with same hash code
        TableId tableId = TableId.generate();
        TableMetadata[] tables = {
            TableMetadata.builder("ks1", name1, tableId).addPartitionKeyColumn("key1", UTF8Type.instance).build(),
            TableMetadata.builder("ks1", name2, tableId).addPartitionKeyColumn("key1", UTF8Type.instance).build()};
        UUID[] ids = { UUID.randomUUID(), UUID.randomUUID() };

        assertThat(tables[0].hashCode(), equalTo(tables[1].hashCode()));

        AtomicInteger counter = new AtomicInteger();
        TableSchemaManager m = newInstance(counter);

        invokeParallel(() -> {
            int index = ThreadLocalRandom.current().nextInt(0, 2);
            return m.ensureCreated(ids[index], tables[index]);
        }, 32);

        // Create was invoked once per table
        assertThat(counter.get(), equalTo(2));
    }

    private static TableSchemaManager newInstance(AtomicInteger counter)
    {
        return new TableSchemaManager((a, b) -> {
            counter.incrementAndGet();
            CompletableFuture<Void> f = new CompletableFuture<>();
            // Complete async
            ExecutorService executor = Executors.newFixedThreadPool(1);
            executor.submit(() -> f.complete(null));
            return f;
        });
    }

    private static TableMetadata newTable(int cols)
    {
        TableMetadata.Builder builder = TableMetadata.builder("ks1", "table1").addPartitionKeyColumn("key1", UTF8Type.instance);

        for (int i = 0; i < cols; i++)
        {
            builder.addRegularColumn(String.format("col%d", i+1), UTF8Type.instance);
        }

        return builder.build();
    }

    private static <T> List<T> invokeParallel(Callable<T> task, int times)
    {
        ExecutorService executor = Executors.newFixedThreadPool(times);
        List<Callable<T>> list = Collections.nCopies(times, task);

        List<Future<T>> futures = null;

        try
        {
            futures = executor.invokeAll(list);
        }
        catch (InterruptedException e)
        {
            fail("Tasks interrupted");
        }

        return futures.stream()
                      .map(tFuture -> {
                          try
                          {
                              return tFuture.get();
                          }
                          catch (Exception e)
                          {
                              throw new RuntimeException("Future could not be got", e);
                          }
                      })
                      .collect(Collectors.toList());
    }
}
