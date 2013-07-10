package com.graphaware.neo4j.relcount.common.manager;

import com.graphaware.neo4j.dto.common.relationship.HasTypeAndDirection;
import org.neo4j.graphdb.Node;

import java.util.Map;

/**
 * Internal component responsible for counting relationships for a {@link Node}.
 *
 * @param <DESCRIPTION> type of relationship description that can be used to query relationship counts on nodes.
 */
public interface RelationshipCountManager<DESCRIPTION extends HasTypeAndDirection> {

    /**
     * Get a relationship count for a node.
     *
     * @param description of the relationship for which to get count.
     * @param node        for which to get relationship count.
     * @return count, 0 if there are no such relationships.
     */
    int getRelationshipCount(DESCRIPTION description, Node node);

    /**
     * Get all relationship counts for a node.
     *
     * @param description description of the relationship for which to get count. Can be used to guide search,
     *                    but can well be ignored. Thus, there is no guarantee that all returned candidates match the description.
     * @param node        for which to get relationship count.
     * @return relationship counts (key = candidate relationship, value = count).
     */
    Map<DESCRIPTION, Integer> getRelationshipCounts(DESCRIPTION description, Node node);
}
