package com.graphaware.relcount.common.internal.node;

import com.graphaware.propertycontainer.dto.common.relationship.HasTypeAndDirection;

import java.util.Map;

/**
 * Internal {@link org.neo4j.graphdb.Node} wrapper responsible for caching relationship counts on that node.
 *
 * @param <CACHED> type of the cached counts.
 */
public interface RelationshipCountCachingNode<CACHED extends HasTypeAndDirection> {

    /**
     * ID of the wrapped Neo4j {@link org.neo4j.graphdb.Node}.
     *
     * @return ID.
     */
    long getId();

    /**
     * Get all relationship counts cached on the node. No aggregation is performed, this is the raw data as stored.
     *
     * @return cached relationship counts (key = relationship representation, value = count). Implementations can choose
     *         for the map to be sorted so that it can be iterated in order (e.g. specific to general).
     */
    Map<CACHED, Integer> getCachedCounts();

    /**
     * Increment a cached relationship count on the node by a delta.
     *
     * @param relationship representation of a relationship.
     * @param delta        by how many to increment.
     */
    void incrementCount(CACHED relationship, int delta);

    /**
     * Decrement a cached relationship count on the node by delta.
     * Delete the cached relationship count if the count is 0 after the decrement.
     *
     * @param relationship representation of a relationship.
     * @param delta        by how many to decrement.
     * @throws com.graphaware.framework.NeedsInitializationException
     *          if a count reaches below 0.
     */
    void decrementCount(CACHED relationship, int delta);

    /**
     * Delete a cached count on the node.
     *
     * @param relationship representation of the relationship to delete.
     */
    void deleteCount(CACHED relationship);
}
