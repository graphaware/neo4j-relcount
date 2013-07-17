package com.graphaware.neo4j.relcount.full.strategy;

import com.graphaware.neo4j.tx.event.strategy.InclusionStrategies;

/**
 *
 */
public interface RelationshipCountStrategies extends InclusionStrategies {

    RelationshipPropertiesExtractionStrategy getRelationshipPropertiesExtractionStrategy();

    RelationshipWeighingStrategy getRelationshipWeighingStrategy();

    int getCompactionThreshold();
}
