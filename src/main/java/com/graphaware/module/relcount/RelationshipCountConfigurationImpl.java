package com.graphaware.module.relcount;

import com.graphaware.common.policy.InclusionPolicies;
import com.graphaware.common.policy.none.IncludeNoNodeProperties;
import com.graphaware.common.policy.none.IncludeNoNodes;
import com.graphaware.module.relcount.cache.DegreeCachingStrategy;
import com.graphaware.module.relcount.cache.SingleNodePropertyDegreeCachingStrategy;
import com.graphaware.module.relcount.compact.CompactionStrategy;
import com.graphaware.module.relcount.compact.ThresholdBasedCompactionStrategy;
import com.graphaware.module.relcount.count.OneForEach;
import com.graphaware.module.relcount.count.WeighingStrategy;
import com.graphaware.runtime.config.BaseTxDrivenModuleConfiguration;
import com.graphaware.runtime.policy.all.IncludeAllBusinessRelationshipProperties;
import com.graphaware.runtime.policy.all.IncludeAllBusinessRelationships;

/**
 * {@link RelationshipCountConfiguration}, providing static factory method for a default configuration and "with"
 * methods for fluently overriding these with custom strategies.
 */
public class RelationshipCountConfigurationImpl extends BaseTxDrivenModuleConfiguration<RelationshipCountConfigurationImpl> implements RelationshipCountConfiguration {

    private static final int DEFAULT_COMPACTION_THRESHOLD = 20;

    private final DegreeCachingStrategy degreeCachingStrategy;
    private final CompactionStrategy compactionStrategy;
    private final WeighingStrategy weighingStrategy;

    /**
     * Create default strategies.
     *
     * @return default strategies.
     */
    public static RelationshipCountConfigurationImpl defaultConfiguration() {
        return new RelationshipCountConfigurationImpl(
                new InclusionPolicies(
                        IncludeNoNodes.getInstance(),
                        IncludeNoNodeProperties.getInstance(),
                        IncludeAllBusinessRelationships.getInstance(),
                        IncludeAllBusinessRelationshipProperties.getInstance()),
                new SingleNodePropertyDegreeCachingStrategy(),
                new ThresholdBasedCompactionStrategy(DEFAULT_COMPACTION_THRESHOLD),
                OneForEach.getInstance()
        );
    }

    /**
     * Constructor.
     *
     * @param inclusionPolicies   strategies for what to include.
     * @param degreeCachingStrategy strategy for caching degrees.
     * @param compactionStrategy    strategy for compacting cached counts.
     * @param weighingStrategy      strategy for weighing relationships.
     */
    protected RelationshipCountConfigurationImpl(InclusionPolicies inclusionPolicies, DegreeCachingStrategy degreeCachingStrategy, CompactionStrategy compactionStrategy, WeighingStrategy weighingStrategy) {
        super(inclusionPolicies);
        this.degreeCachingStrategy = degreeCachingStrategy;
        this.compactionStrategy = compactionStrategy;
        this.weighingStrategy = weighingStrategy;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected RelationshipCountConfigurationImpl newInstance(InclusionPolicies inclusionPolicies) {
        return new RelationshipCountConfigurationImpl(inclusionPolicies, getDegreeCachingStrategy(), getCompactionStrategy(), getWeighingStrategy());
    }

    /**
     * Reconfigure this instance to use a custom degree caching strategy.
     *
     * @param degreeCachingStrategy to use.
     * @return reconfigured strategies.
     */
    public RelationshipCountConfigurationImpl with(DegreeCachingStrategy degreeCachingStrategy) {
        return new RelationshipCountConfigurationImpl(getInclusionPolicies(), degreeCachingStrategy, getCompactionStrategy(), getWeighingStrategy());
    }

    /**
     * Reconfigure this instance to use a custom compaction strategy.
     *
     * @param compactionStrategy to use.
     * @return reconfigured strategies.
     */
    public RelationshipCountConfigurationImpl with(CompactionStrategy compactionStrategy) {
        return new RelationshipCountConfigurationImpl(getInclusionPolicies(), getDegreeCachingStrategy(), compactionStrategy, getWeighingStrategy());
    }

    /**
     * Reconfigure this instance to use a {@link ThresholdBasedCompactionStrategy} with a different threshold.
     *
     * @param threshold to use.
     * @return reconfigured strategies.
     */
    public RelationshipCountConfigurationImpl withThreshold(int threshold) {
        return new RelationshipCountConfigurationImpl(getInclusionPolicies(), getDegreeCachingStrategy(), new ThresholdBasedCompactionStrategy(threshold), getWeighingStrategy());
    }

    /**
     * Reconfigure this instance to use a custom relationship weighing strategy.
     *
     * @param weighingStrategy to use.
     * @return reconfigured strategies.
     */
    public RelationshipCountConfigurationImpl with(WeighingStrategy weighingStrategy) {
        return new RelationshipCountConfigurationImpl(getInclusionPolicies(), getDegreeCachingStrategy(), getCompactionStrategy(), weighingStrategy);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DegreeCachingStrategy getDegreeCachingStrategy() {
        return degreeCachingStrategy;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompactionStrategy getCompactionStrategy() {
        return compactionStrategy;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public WeighingStrategy getWeighingStrategy() {
        return weighingStrategy;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        RelationshipCountConfigurationImpl that = (RelationshipCountConfigurationImpl) o;

        if (!compactionStrategy.equals(that.compactionStrategy)) return false;
        if (!degreeCachingStrategy.equals(that.degreeCachingStrategy)) return false;
        if (!weighingStrategy.equals(that.weighingStrategy)) return false;

        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + degreeCachingStrategy.hashCode();
        result = 31 * result + compactionStrategy.hashCode();
        result = 31 * result + weighingStrategy.hashCode();
        return result;
    }
}
