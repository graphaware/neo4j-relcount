package com.graphaware.neo4j.relcount.common.logic;

import com.graphaware.neo4j.dto.common.relationship.HasTypeAndDirection;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.HashMap;
import java.util.Map;

import static org.neo4j.graphdb.Direction.BOTH;

/**
 * Abstract base-class for naive {@link RelationshipCountReader} implementations that count matching relationships by
 * iterating through all {@link org.neo4j.graphdb.Node}'s {@link org.neo4j.graphdb.Relationship}s.
 *
 * @param <DESCRIPTION> type of relationship description that can be used to query relationship counts for nodes.
 */
public abstract class NaiveRelationshipCountReader<CANDIDATE extends  HasTypeAndDirection, DESCRIPTION extends HasTypeAndDirection> extends BaseRelationshipCountReader<CANDIDATE, DESCRIPTION> {

    /**
     * {@inheritDoc}
     * <p/>
     * Gets all relationship counts by iterating over all node's relationships. Uses the description to guide the search,
     * i.e. to tell Neo4j to only return relationships of the specified type and direction.
     */
    @Override
    public Map<CANDIDATE, Integer> getCandidates(DESCRIPTION description, Node node) {
        Map<CANDIDATE, Integer> result = new HashMap<>();

        for (Relationship candidateRelationship : node.getRelationships(description.getDirection(), description.getType())) {
            CANDIDATE candidate = newCandidate(candidateRelationship, node);
            if (!result.containsKey(candidate)) {
                result.put(candidate, 0);
            }
            result.put(candidate, result.get(candidate) + relationshipWeight(candidateRelationship, node));

            //double count self-relationships if looking for BOTH
            if (BOTH.equals(description.getDirection()) && BOTH.equals(candidate.getDirection())) {
                result.put(candidate, result.get(candidate) + relationshipWeight(candidateRelationship, node));
            }
        }

        return result;
    }

    /**
     * Create a candidate relationship representation from a Neo4j {@link Relationship}.
     *
     * @param relationship from Neo4j.
     * @param pointOfView  Node, whose point of view the candidate's direction will be determined.
     * @return representation of the candidate relationship.
     */
    protected abstract CANDIDATE newCandidate(Relationship relationship, Node pointOfView);

    /**
     * Get a relationship's weight.
     *
     * @param relationship to find weight for.
     * @param pointOfView  node whose point of view we are currently looking at the relationship.
     * @return the relationship weight. Should be positive.
     */
    protected abstract int relationshipWeight(Relationship relationship, Node pointOfView);
}
