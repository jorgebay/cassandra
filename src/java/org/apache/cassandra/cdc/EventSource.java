package org.apache.cassandra.cdc;

/**
 * Source of CDC events.
 */
public enum EventSource
{
    /**
     * The CDC event was triggered by a client request.
     */
    CLIENT_REQUEST,

    /**
     * The CDC event was triggered by a range read using a snapshot utility.
     */
    SNAPSHOT,

    /**
     * The CDC event was originated from a client request, stored in a hint (due to producer failure) and
     * later read from the hint.
     */
    HINT_READ
}
