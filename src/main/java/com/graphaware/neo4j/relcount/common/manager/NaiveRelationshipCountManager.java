package com.graphaware.neo4j.relcount.common.manager;

import com.graphaware.neo4j.dto.common.relationship.HasDirectionAndType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * Naive {@link RelationshipCountManager} that counts matching relationships by inspecting all {@link Node}'s {@link Relationship}s.
 */
public class NaiveRelationshipCountManager implements RelationshipCountManager<HasDirectionAndType> {

    /**
     * {@inheritDoc}
     */
    @Override
    public int getRelationshipCount(HasDirectionAndType relationship, Node node) {
        int count = 0;

        for (Relationship candidate : node.getRelationships(relationship.getDirection(), relationship.getType())) {
            if (relationship.matches(candidate, node)) {
                count++;
            }
        }

        return count;
    }
}
