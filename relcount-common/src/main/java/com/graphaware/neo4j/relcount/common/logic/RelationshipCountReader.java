package com.graphaware.neo4j.relcount.common.logic;

import com.graphaware.neo4j.dto.common.relationship.HasTypeAndDirection;
import com.graphaware.neo4j.relcount.common.counter.UnableToCountException;
import org.neo4j.graphdb.Node;

/**
 * Internal component responsible reading relationship counts for a {@link Node} in some way.
 *
 * @param <DESCRIPTION> type of relationship description that can be used to query relationship counts for nodes.
 */
public interface RelationshipCountReader<DESCRIPTION extends HasTypeAndDirection> {

    /**
     * Get a relationship count for a node.
     *
     * @param description of the relationship for which to get count.
     * @param node        for which to get relationship count.
     * @return count, 0 if there are no such relationships.
     * @throws UnableToCountException if unable to count relationships with the described characteristics.
     *                                Implementations can choose to guarantee this exception is never thrown.
     */
    int getRelationshipCount(DESCRIPTION description, Node node) throws UnableToCountException;
}
