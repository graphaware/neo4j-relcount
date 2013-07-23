package com.graphaware.neo4j.relcount.full.module;

import com.graphaware.neo4j.framework.GraphAwareModule;
import com.graphaware.neo4j.relcount.common.logic.RelationshipCountCache;
import com.graphaware.neo4j.relcount.common.module.RelationshipCountModule;
import com.graphaware.neo4j.relcount.full.api.FullCachedRelationshipCounter;
import com.graphaware.neo4j.relcount.full.api.FullFallingBackRelationshipCounter;
import com.graphaware.neo4j.relcount.full.api.FullNaiveRelationshipCounter;
import com.graphaware.neo4j.relcount.full.api.FullRelationshipCounter;
import com.graphaware.neo4j.relcount.full.logic.FullRelationshipCountCache;
import com.graphaware.neo4j.relcount.full.strategy.RelationshipCountStrategies;
import com.graphaware.neo4j.relcount.full.strategy.RelationshipCountStrategiesImpl;
import com.graphaware.neo4j.tx.event.strategy.InclusionStrategies;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.RelationshipType;

/**
 * {@link com.graphaware.neo4j.framework.GraphAwareModule} providing caching capabilities for full relationship counting.
 * "Full" means it cares about {@link org.neo4j.graphdb.RelationshipType}s, {@link org.neo4j.graphdb.Direction}s,
 * and properties.
 * <p/>
 * Once registered with {@link com.graphaware.neo4j.framework.GraphAwareFramework}, relationship
 * counts will be cached on nodes properties. {@link FullCachedRelationshipCounter} or {@link FullFallingBackRelationshipCounter} can then be used to
 * count relationships by querying these cached counts.
 */
public class FullRelationshipCountModule extends RelationshipCountModule implements GraphAwareModule {

    /**
     * Default ID of this module used to identify metadata written by this module.
     */
    public static final String FULL_RELCOUNT_DEFAULT_ID = "FRC";

    private final RelationshipCountStrategies relationshipCountStrategies;
    private final RelationshipCountCache relationshipCountCache;

    /**
     * Create a module with default ID and configuration. Use this constructor when you wish to register a single
     * instance of the module with {@link com.graphaware.neo4j.framework.GraphAwareFramework} and you are happy with
     * the default configuration (see {@link com.graphaware.neo4j.relcount.full.strategy.RelationshipCountStrategiesImpl#defaultStrategies()}).
     */
    public FullRelationshipCountModule() {
        this(FULL_RELCOUNT_DEFAULT_ID, RelationshipCountStrategiesImpl.defaultStrategies());
    }

    /**
     * Create a module with default ID and custom configuration. Use this constructor when you wish to register a single
     * instance of the module with {@link com.graphaware.neo4j.framework.GraphAwareFramework} and you want to provide
     * custom {@link RelationshipCountStrategies}. This could be the case, for instance, when you would like to exclude
     * certain {@link org.neo4j.graphdb.Relationship}s from being counted at all ({@link com.graphaware.neo4j.tx.event.strategy.RelationshipInclusionStrategy}),
     * certain properties from being considered at all ({@link com.graphaware.neo4j.tx.event.strategy.RelationshipPropertyInclusionStrategy}),
     * derive custom properties ({@link com.graphaware.neo4j.relcount.full.strategy.RelationshipPropertiesExtractionStrategy}),
     * weigh each relationship differently ({@link com.graphaware.neo4j.relcount.full.strategy.RelationshipWeighingStrategy},
     * or use a custom threshold for compaction.
     */
    public FullRelationshipCountModule(RelationshipCountStrategies relationshipCountStrategies) {
        this(FULL_RELCOUNT_DEFAULT_ID, relationshipCountStrategies);
    }

    /**
     * Create a module with a custom ID and configuration. Use this constructor when you wish to register a multiple
     * instances of the module with {@link com.graphaware.neo4j.framework.GraphAwareFramework} and you want to provide
     * custom {@link RelationshipCountStrategies} for each one of them. This could be the case, for instance, when you
     * would like to keep two different kinds of relationships, weighted and unweighted.
     */
    public FullRelationshipCountModule(String id, RelationshipCountStrategies relationshipCountStrategies) {
        super(id);
        this.relationshipCountStrategies = relationshipCountStrategies;
        this.relationshipCountCache = new FullRelationshipCountCache(id, relationshipCountStrategies);
    }

    /**
     * Construct a {@link FullFallingBackRelationshipCounter} that will count the relationships cached by this module.
     *
     * @param type      of the relationships to count.
     * @param direction of the relationships to count.
     * @return counter.
     */
    public FullRelationshipCounter fallingBackCounter(RelationshipType type, Direction direction) {
        return new FullFallingBackRelationshipCounter(type, direction, getId(), relationshipCountStrategies, getConfig());
    }

    /**
     * Construct a {@link FullCachedRelationshipCounter} that will count the relationships cached by this module.
     *
     * @param type      of the relationships to count.
     * @param direction of the relationships to count.
     * @return counter.
     */
    public FullRelationshipCounter cachedCounter(RelationshipType type, Direction direction) {
        return new FullCachedRelationshipCounter(type, direction, getId(), getConfig());
    }

    /**
     * Construct a {@link FullNaiveRelationshipCounter} that will count relationships using the same configuration as
     * this module.
     *
     * @param type      of the relationships to count.
     * @param direction of the relationships to count.
     * @return counter.
     */
    public FullRelationshipCounter naiveCounter(RelationshipType type, Direction direction) {
        return new FullNaiveRelationshipCounter(type, direction, relationshipCountStrategies);
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
        return relationshipCountStrategies;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown() {
        ((FullRelationshipCountCache) relationshipCountCache).shutdown();
    }
}
