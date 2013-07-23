package com.graphaware.neo4j.relcount.simple.module;

import com.graphaware.neo4j.framework.strategy.IncludeAllRelationships;
import com.graphaware.neo4j.framework.strategy.InclusionStrategiesImpl;
import com.graphaware.neo4j.relcount.common.logic.RelationshipCountCache;
import com.graphaware.neo4j.relcount.common.module.RelationshipCountModule;
import com.graphaware.neo4j.relcount.simple.logic.SimpleRelationshipCountCache;
import com.graphaware.neo4j.tx.event.strategy.InclusionStrategies;
import com.graphaware.neo4j.tx.event.strategy.RelationshipInclusionStrategy;

/**
 * {@link com.graphaware.neo4j.framework.GraphAwareModule} providing caching capabilities for simple relationship counting.
 * "Simple" means it only cares about {@link org.neo4j.graphdb.RelationshipType}s and {@link org.neo4j.graphdb.Direction}s,
 * properties are ignored.
 * <p/>
 * Once registered with {@link com.graphaware.neo4j.framework.GraphAwareFramework}, per-type-and-direction relationship
 * counts will be cached on nodes properties. {@link com.graphaware.neo4j.relcount.simple.api.SimpleCachedRelationshipCounter} can then be used to
 * count relationships by querying these cached counts.
 */
public class SimpleRelationshipCountModule extends RelationshipCountModule {

    /**
     * ID of this module used to identify metadata written by this module.
     */
    public static final String SIMPLE_RELCOUNT_ID = "SRC";

    private final RelationshipCountCache relationshipCountCache;
    private final InclusionStrategies inclusionStrategies;

    /**
     * Create a new module with default configuration.
     */
    public SimpleRelationshipCountModule() {
        this(IncludeAllRelationships.getInstance());
    }

    /**
     * Create a new module with a custom strategy for including relationships. This is good for applications with many
     * relationships types but only a few that need to be counted quickly. {@link com.graphaware.neo4j.relcount.simple.api.SimpleCachedRelationshipCounter}
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
    protected RelationshipCountCache getRelationshipCountCache() {
        return relationshipCountCache;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InclusionStrategies getInclusionStrategies() {
        return inclusionStrategies;
    }
}
