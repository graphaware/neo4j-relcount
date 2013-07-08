package com.graphaware.neo4j.relcount.common.manager;

import com.graphaware.neo4j.dto.common.relationship.HasTypeAndDirection;
import org.neo4j.graphdb.Node;

/**
 * Internal component responsible for counting relationships for a {@link Node}.
 *
 * @param <T> type of relationship representation that can be used to query relationship counts on nodes.
 */
public interface RelationshipCountManager<T extends HasTypeAndDirection> {

    /**
     * Get a relationship count for a node.
     *
     * @param relationship representation of the relationship for which to get count.
     * @param node         for which to get relationship counts.
     * @return count, 0 if there are no such relationships.
     */
    int getRelationshipCount(T relationship, Node node);
}
