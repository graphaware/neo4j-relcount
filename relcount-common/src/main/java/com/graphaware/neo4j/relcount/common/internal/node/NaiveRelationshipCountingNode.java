package com.graphaware.neo4j.relcount.common.internal.node;

import com.graphaware.neo4j.dto.common.relationship.HasTypeAndDirection;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import static org.neo4j.graphdb.Direction.BOTH;

/**
 * Base-class for naive {@link RelationshipCountingNode} implementations that count matching relationships by
 * iterating through all {@link org.neo4j.graphdb.Node}'s {@link org.neo4j.graphdb.Relationship}s.
 *
 * @param <CANDIDATE>   type of candidate relationship representation.
 * @param <DESCRIPTION> type of relationship description that can be used to query relationship counts for nodes.
 */
public abstract class NaiveRelationshipCountingNode<CANDIDATE extends HasTypeAndDirection, DESCRIPTION extends HasTypeAndDirection> {

    protected final Node node;

    /**
     * Construct a new relationship counting node.
     *
     * @param node wrapped Neo4j node on which to count relationships.
     */
    protected NaiveRelationshipCountingNode(Node node) {
        this.node = node;
    }

    /**
     * @see {@link com.graphaware.neo4j.relcount.common.internal.node.RelationshipCountingNode#getId()}
     */
    public long getId() {
        return node.getId();
    }

    /**
     * @see {@link RelationshipCountingNode#getRelationshipCount(com.graphaware.neo4j.dto.common.relationship.HasTypeAndDirection)}
     */
    public int getRelationshipCount(DESCRIPTION description) {
        int result = 0;

        for (Relationship candidateRelationship : node.getRelationships(description.getDirection(), description.getType())) {
            CANDIDATE candidate = newCandidate(candidateRelationship);
            if (candidateMatchesDescription(candidate, description)) {
                result = result + relationshipWeight(candidateRelationship);

                //double count self-relationships if looking for BOTH
                if (BOTH.equals(description.getDirection()) && BOTH.equals(candidate.getDirection())) {
                    result = result + relationshipWeight(candidateRelationship);
                }
            }
        }

        return result;
    }

    /**
     * Create a candidate relationship representation from a Neo4j {@link Relationship}.
     *
     * @param relationship from Neo4j.
     * @return representation of the candidate relationship.
     */
    protected abstract CANDIDATE newCandidate(Relationship relationship);

    /**
     * Does the given candidate match the relationship description?
     *
     * @param candidate   candidate that could correspond to the given relationship description.
     * @param description of the relationships being counted.
     * @return true iff the candidate matches the description and should thus be taken into account.
     */
    protected abstract boolean candidateMatchesDescription(CANDIDATE candidate, DESCRIPTION description);

    /**
     * Get the weight of a relationship, i.e. how much it counts for?
     *
     * @param relationship to get weight for.
     * @return relationship weight, 1 by default, can be overridden by subclasses. Should be positive.
     */
    protected int relationshipWeight(Relationship relationship) {
        return 1;
    }
}
