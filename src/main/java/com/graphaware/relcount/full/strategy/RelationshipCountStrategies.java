package com.graphaware.relcount.full.strategy;


import com.graphaware.tx.event.improved.strategy.InclusionStrategies;

/**
 * Container for strategies and configuration related to relationship counting.
 */
public interface RelationshipCountStrategies extends InclusionStrategies {

    /**
     * @return contained relationship properties extraction strategy.
     */
    RelationshipPropertiesExtractionStrategy getRelationshipPropertiesExtractionStrategy();

    /**
     * @return contained relationship weighing strategy.
     */
    RelationshipWeighingStrategy getRelationshipWeighingStrategy();

    /**
     * @return compaction threshold.
     */
    int getCompactionThreshold();
}
