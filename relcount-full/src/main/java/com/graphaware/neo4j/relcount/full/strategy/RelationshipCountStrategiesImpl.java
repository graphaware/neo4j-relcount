package com.graphaware.neo4j.relcount.full.strategy;

import com.graphaware.neo4j.framework.strategy.IncludeAllRelationshipProperties;
import com.graphaware.neo4j.framework.strategy.IncludeAllRelationships;
import com.graphaware.neo4j.tx.event.strategy.*;

/**
 * {@link RelationshipCountStrategies}, providing static factory method for a default configuration and "with"
 * methods for fluently overriding these with custom strategies.
 */
public class RelationshipCountStrategiesImpl extends BaseInclusionStrategies<RelationshipCountStrategiesImpl> implements RelationshipCountStrategies {

    private static final int DEFAULT_COMPACTION_THRESHOLD = 20;

    private final RelationshipPropertiesExtractionStrategy relationshipPropertiesExtractionStrategy;
    private final RelationshipWeighingStrategy relationshipWeighingStrategy;
    private final int compactionThreshold;

    /**
     * Create default strategies.
     *
     * @return default strategies.
     */
    public static RelationshipCountStrategiesImpl defaultStrategies() {
        return new RelationshipCountStrategiesImpl(
                IncludeNoNodes.getInstance(),
                IncludeNoNodeProperties.getInstance(),
                IncludeAllRelationships.getInstance(),
                IncludeAllRelationshipProperties.getInstance(),
                ExtractAllRelationshipProperties.getInstance(),
                OneForEachRelationship.getInstance(),
                DEFAULT_COMPACTION_THRESHOLD
        );
    }

    /**
     * Constructor.
     *
     * @param nodeInclusionStrategy         strategy.
     * @param nodePropertyInclusionStrategy strategy.
     * @param relationshipInclusionStrategy strategy.
     * @param relationshipPropertyInclusionStrategy
     *                                      strategy.
     * @param relationshipPropertiesExtractionStrategy
     *                                      strategy.
     * @param relationshipWeighingStrategy  strategy.
     */
    private RelationshipCountStrategiesImpl(NodeInclusionStrategy nodeInclusionStrategy, NodePropertyInclusionStrategy nodePropertyInclusionStrategy, RelationshipInclusionStrategy relationshipInclusionStrategy, RelationshipPropertyInclusionStrategy relationshipPropertyInclusionStrategy, RelationshipPropertiesExtractionStrategy relationshipPropertiesExtractionStrategy, RelationshipWeighingStrategy relationshipWeighingStrategy, int compactionThreshold) {
        super(nodeInclusionStrategy, nodePropertyInclusionStrategy, relationshipInclusionStrategy, relationshipPropertyInclusionStrategy);
        this.relationshipPropertiesExtractionStrategy = relationshipPropertiesExtractionStrategy;
        this.relationshipWeighingStrategy = relationshipWeighingStrategy;
        this.compactionThreshold = compactionThreshold;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected RelationshipCountStrategiesImpl newInstance(NodeInclusionStrategy nodeInclusionStrategy, NodePropertyInclusionStrategy nodePropertyInclusionStrategy, RelationshipInclusionStrategy relationshipInclusionStrategy, RelationshipPropertyInclusionStrategy relationshipPropertyInclusionStrategy) {
        return new RelationshipCountStrategiesImpl(nodeInclusionStrategy, nodePropertyInclusionStrategy, relationshipInclusionStrategy, relationshipPropertyInclusionStrategy, getRelationshipPropertiesExtractionStrategy(), getRelationshipWeighingStrategy(), getCompactionThreshold());
    }

    /**
     * Reconfigure this instance to use a custom relationship properties extraction strategy.
     *
     * @param relationshipPropertiesExtractionStrategy
     *         to use.
     * @return reconfigured strategies.
     */
    public RelationshipCountStrategiesImpl with(RelationshipPropertiesExtractionStrategy relationshipPropertiesExtractionStrategy) {
        return new RelationshipCountStrategiesImpl(getNodeInclusionStrategy(), getNodePropertyInclusionStrategy(), getRelationshipInclusionStrategy(), getRelationshipPropertyInclusionStrategy(), relationshipPropertiesExtractionStrategy, getRelationshipWeighingStrategy(), getCompactionThreshold());
    }

    /**
     * Reconfigure this instance to use a custom relationship weighing strategy.
     *
     * @param relationshipWeighingStrategy to use.
     * @return reconfigured strategies.
     */
    public RelationshipCountStrategiesImpl with(RelationshipWeighingStrategy relationshipWeighingStrategy) {
        return new RelationshipCountStrategiesImpl(getNodeInclusionStrategy(), getNodePropertyInclusionStrategy(), getRelationshipInclusionStrategy(), getRelationshipPropertyInclusionStrategy(), getRelationshipPropertiesExtractionStrategy(), relationshipWeighingStrategy, getCompactionThreshold());
    }

    /**
     * Reconfigure this instance to use a custom compaction threshold.
     *
     * @param compactionThreshold to use.
     * @return reconfigured strategies.
     */
    public RelationshipCountStrategiesImpl with(int compactionThreshold) {
        return new RelationshipCountStrategiesImpl(getNodeInclusionStrategy(), getNodePropertyInclusionStrategy(), getRelationshipInclusionStrategy(), getRelationshipPropertyInclusionStrategy(), getRelationshipPropertiesExtractionStrategy(), getRelationshipWeighingStrategy(), compactionThreshold);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RelationshipPropertiesExtractionStrategy getRelationshipPropertiesExtractionStrategy() {
        return relationshipPropertiesExtractionStrategy;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RelationshipWeighingStrategy getRelationshipWeighingStrategy() {
        return relationshipWeighingStrategy;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getCompactionThreshold() {
        return compactionThreshold;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        RelationshipCountStrategiesImpl that = (RelationshipCountStrategiesImpl) o;

        if (compactionThreshold != that.compactionThreshold) return false;
        if (!relationshipPropertiesExtractionStrategy.equals(that.relationshipPropertiesExtractionStrategy))
            return false;
        if (!relationshipWeighingStrategy.equals(that.relationshipWeighingStrategy)) return false;

        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + relationshipPropertiesExtractionStrategy.hashCode();
        result = 31 * result + relationshipWeighingStrategy.hashCode();
        result = 31 * result + compactionThreshold;
        return result;
    }
}
