package com.graphaware.relcount.common.internal.node;

import com.graphaware.propertycontainer.dto.common.relationship.HasTypeAndDirection;
import com.graphaware.relcount.common.counter.UnableToCountException;
import org.neo4j.graphdb.Node;

/**
 * Internal {@link Node} wrapper responsible for getting the {@link Node}'s relationship counts in some way.
 *
 * @param <DESCRIPTION> type of relationship description that can be used to query relationship counts.
 */
public interface RelationshipCountingNode<DESCRIPTION extends HasTypeAndDirection> {

    /**
     * ID of the wrapped Neo4j {@link org.neo4j.graphdb.Node}.
     *
     * @return ID.
     */
    long getId();

    /**
     * Get a relationship count for the wrapped node.
     *
     * @param description of the relationship for which to get count.
     * @return count, 0 if there are no such relationships.
     * @throws UnableToCountException if unable to count relationships with the described characteristics.
     *                                Implementations can choose to guarantee this exception is never thrown.
     */
    int getRelationshipCount(DESCRIPTION description) throws UnableToCountException;
}
