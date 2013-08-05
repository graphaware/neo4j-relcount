package com.graphaware.relcount.full.strategy;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * A singleton implementation of {@link RelationshipWeighingStrategy} that gives each relationship a weight of 1.
 */
public final class OneForEachRelationship implements RelationshipWeighingStrategy {

    private static final OneForEachRelationship INSTANCE = new OneForEachRelationship();

    private OneForEachRelationship() {
    }

    public static OneForEachRelationship getInstance() {
        return INSTANCE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getRelationshipWeight(Relationship relationship, Node pointOfView) {
        return 1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return this.getClass().getCanonicalName().hashCode();
    }
}
