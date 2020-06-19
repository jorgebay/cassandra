package org.apache.cassandra.cdc.producers;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.cdc.CDCProducer;
import org.apache.cassandra.cdc.MutationCDCInfo;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.metrics.MetricNameFactory;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Pair;

/**
 * Represents a schema-aware CDC async producer.
 *
 * <p>Implementors will get the following guarantees:</p>
 * <ul>
 * <li>For a given table, {@link #createTableSchemaAsync} will be invoked before calling
 * {@link #send(PartitionUpdate, UUID)} with a partition update for that table (awaiting for the creation
 * future to complete).</li>
 * <li>In the same way, when table metadata change is first detected, {@link #createTableSchemaAsync} will be
 * invoked before calling {@link #send(PartitionUpdate, UUID)}</li>
 * </ul>
 */
public abstract class AsyncCDCProducer implements CDCProducer
{
    private static final CompletableFuture<Void> completedFuture = CompletableFuture.completedFuture(null);
    private final ConcurrentHashMap<TableId, TableSchemaManager> tableSchemaManager = new ConcurrentHashMap<>();

    public abstract CompletableFuture<Void> init(Map<String, Object> options, MetricNameFactory factory);

    /**
     * Asynchronously publishes the schema of the table.
     */
    protected abstract CompletableFuture<Void> createTableSchemaAsync(TableMetadata table, UUID schemaVersion);

    /**
     * Asynchronously publishes the partition update.
     *
     * It will be invoked after {@link AsyncCDCProducer#createTableSchemaAsync} was invoked for a given table and
     * creation future was completed.
     */
    protected abstract CompletableFuture<Void> send(PartitionUpdate update, UUID schemaVersion);

    public abstract void close() throws Exception;

    @Override
    public CompletableFuture<Void> send(Mutation mutation, MutationCDCInfo info)
    {
        Collection<PartitionUpdate> updates = mutation.getPartitionUpdates();
        CompletableFuture<?>[] futures = new CompletableFuture<?>[updates.size()];
        int index = 0;

        for (PartitionUpdate pkUpdate : updates)
        {
            futures[index++] = (pkUpdate.metadata().params.cdc) ? sendInternal(pkUpdate, info.getSchemaVersion())
                                                                : completedFuture;
        }

        return CompletableFuture.allOf(futures);
    }

    private CompletableFuture<Void> sendInternal(PartitionUpdate update, UUID schemaVersion)
    {
        TableSchemaManager schemaManager = tableSchemaManager.computeIfAbsent(
            update.metadata().id, k -> new TableSchemaManager(this::createTableSchemaAsync));

        return schemaManager.ensureCreated(schemaVersion, update.metadata())
                            // Invoke send() with the version that later affected this table,
                            // not the most recent version number.
                            .thenCompose(originalSchemaVersion -> send(update, originalSchemaVersion));
    }

    /**
     * Contains logic to handle different table schema versions.
     */
    @VisibleForTesting
    static class TableSchemaManager
    {
        private final Function2<CompletableFuture<Void>, TableMetadata, UUID> createHandler;

        @FunctionalInterface
        interface Function2<Result, One, Two> {
            public Result apply(One one, Two two);
        }

        TableSchemaManager(Function2<CompletableFuture<Void>, TableMetadata, UUID> createHandler)
        {
            this.createHandler = createHandler;
        }

        private final ConcurrentHashMap<Pair<UUID, Integer>, CompletableFuture<UUID>> creators =
            new ConcurrentHashMap<>();
        private final ConcurrentHashMap<UUID, Integer> tableHashes = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<Integer, TableCreationId> creatorsByHashCode =
            new ConcurrentHashMap<>();

        /**
         * Ensures that {@link TableSchemaManager#createHandler} is invoked once per different table version.
         *
         * Reuses previous table schema when the version changed but it didn't affect the table uses both
         * hashCode + equality to guard against collisions.
         *
         * @return The UUID of the schema version that caused a changed in the table.
         */
        CompletableFuture<UUID> ensureCreated(UUID schemaVersion, TableMetadata table)
        {
            // Quick check without calculating the hashcode each time
            Integer tableHashCode = tableHashes.computeIfAbsent(schemaVersion, k -> table.hashCode());

            // Get or put the creation future
            return creators.computeIfAbsent(Pair.create(schemaVersion, tableHashCode), k -> {
                // Try to reuse the table schema for a given table hash code
                TableCreationId schemaId = creatorsByHashCode.computeIfAbsent(tableHashCode, k2 -> {
                    // Create for a given hash code
                    CompletableFuture<UUID> future = create(table, schemaVersion);
                    return new TableCreationId(table, future);
                });

                // Handle table hashcode collisions
                return !table.equals(schemaId.table)
                       ? create(table, schemaVersion)
                       : schemaId.future;
            });
        }

        private CompletableFuture<UUID> create(TableMetadata table, UUID schemaVersion)
        {
            return this.createHandler.apply(table, schemaVersion)
                                     .thenApply(v -> schemaVersion);
        }

        private static class TableCreationId
        {
            private final TableMetadata table;
            private final CompletableFuture<UUID> future;

            TableCreationId(TableMetadata table, CompletableFuture<UUID> future)
            {
                this.table = table;
                this.future = future;
            }
        }
    }
}
