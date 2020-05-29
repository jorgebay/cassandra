package org.apache.cassandra.cdc;

class DefaultMutationCDCInfo implements MutationCDCInfo
{
    static final DefaultMutationCDCInfo clientRequestInfo = new DefaultMutationCDCInfo(EventSource.CLIENT_REQUEST);
    private final EventSource source;

    DefaultMutationCDCInfo(EventSource source)
    {
        this.source = source;
    }

    public EventSource getSource()
    {
        return source;
    }
}
