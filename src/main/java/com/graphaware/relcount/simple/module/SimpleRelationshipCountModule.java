package com.graphaware.relcount.simple.module;

import com.graphaware.relcount.common.internal.cache.BatchFriendlyRelationshipCountCache;
import com.graphaware.relcount.common.module.RelationshipCountModule;
import com.graphaware.relcount.simple.internal.cache.SimpleRelationshipCountCache;
import com.graphaware.tx.event.improved.strategy.IncludeAllRelationships;
import com.graphaware.tx.event.improved.strategy.InclusionStrategies;
import com.graphaware.tx.event.improved.strategy.InclusionStrategiesImpl;
import com.graphaware.tx.event.improved.strategy.RelationshipInclusionStrategy;

/**
 * {@link com.graphaware.framework.GraphAwareModule} providing caching capabilities for simple relationship counting.
 * "Simple" means it only cares about {@link org.neo4j.graphdb.RelationshipType}s and {@link org.neo4j.graphdb.Direction}s,
 * properties are ignored.
 * <p/>
 * Once registered with {@link com.graphaware.framework.GraphAwareFramework}, per-type-and-direction relationship
 * counts will be cached on nodes properties. {@link com.graphaware.relcount.simple.counter.SimpleCachedRelationshipCounter} can then be used to
 * count relationships by querying these cached counts.
 */
public class SimpleRelationshipCountModule extends RelationshipCountModule {

    /**
     * ID of this module used to identify metadata written by this module.
     */
    public static final String SIMPLE_RELCOUNT_ID = "SRC";

    private final BatchFriendlyRelationshipCountCache relationshipCountCache;
    private final InclusionStrategies inclusionStrategies;

    /**
     * Create a new module with default configuration.
     */
    public SimpleRelationshipCountModule() {
        this(IncludeAllRelationships.getInstance());
    }

    /**
     * Create a new module with a custom strategy for including relationships. This is good for applications with many
     * relationships types but only a few that need to be counted quickly. {@link com.graphaware.relcount.simple.counter.SimpleCachedRelationshipCounter}
     * counting queries for relationships not included by this strategy will naturally return 0.
     *
     * @param relationshipInclusionStrategy relationship inclusion strategy.
     */
    public SimpleRelationshipCountModule(RelationshipInclusionStrategy relationshipInclusionStrategy) {
        super(SIMPLE_RELCOUNT_ID);

        relationshipCountCache = new SimpleRelationshipCountCache(SIMPLE_RELCOUNT_ID);

        inclusionStrategies = InclusionStrategiesImpl.none().with(relationshipInclusionStrategy);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected BatchFriendlyRelationshipCountCache getRelationshipCountCache() {
        return relationshipCountCache;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InclusionStrategies getInclusionStrategies() {
        return inclusionStrategies;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown() {
        //do nothing
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String asString() {
        return super.asString() + ";" + inclusionStrategies.asString();
    }
}
