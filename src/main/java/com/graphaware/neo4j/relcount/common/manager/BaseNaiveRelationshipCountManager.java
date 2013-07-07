package com.graphaware.neo4j.relcount.common.manager;

import com.graphaware.neo4j.dto.common.relationship.HasDirectionAndType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * Naive {@link RelationshipCountManager} that counts matching relationships by inspecting all {@link org.neo4j.graphdb.Node}'s {@link org.neo4j.graphdb.Relationship}s.
 */
public abstract class BaseNaiveRelationshipCountManager<T extends HasDirectionAndType> implements RelationshipCountManager<T> {

    /**
     * {@inheritDoc}
     */
    @Override
    public int getRelationshipCount(T relationship, Node node) {
        int count = 0;

        for (Relationship candidate : node.getRelationships(relationship.getDirection(), relationship.getType())) {
            if (relationship.matches(candidate, node)) {
                count++;
            }
        }

        return count;
    }
}
