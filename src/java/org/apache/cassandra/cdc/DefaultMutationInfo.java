package org.apache.cassandra.cdc;

import java.util.UUID;

class DefaultMutationCDCInfo implements MutationCDCInfo
{
    private final EventSource source;
    private final UUID schemaVersion;

    DefaultMutationCDCInfo(EventSource source, UUID schemaVersion)
    {
        this.source = source;
        this.schemaVersion = schemaVersion;
    }

    @Override
    public EventSource getSource()
    {
        return source;
    }

    @Override
    public UUID getSchemaVersion()
    {
        return schemaVersion;
    }
}
