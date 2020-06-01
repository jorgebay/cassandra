package org.apache.cassandra.cdc;

/**
 * Contains the config properties for CDC.
 */
interface CDCConfig
{
    int getMaxHints();

    boolean useHints();

    double getMaxFailureRatio();

    long getLatencyErrorMs();

    long getLatencyWarningNanos();
}
