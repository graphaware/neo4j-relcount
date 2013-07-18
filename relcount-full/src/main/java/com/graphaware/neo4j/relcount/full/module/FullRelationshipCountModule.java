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

import static com.graphaware.neo4j.relcount.full.Constants.FULL_RELCOUNT_DEFAULT_ID;

/**
 *
 */
public class FullRelationshipCountModule extends RelationshipCountModule implements GraphAwareModule {

    private final RelationshipCountStrategies relationshipCountStrategies;
    private final RelationshipCountCache relationshipCountCache;

    public FullRelationshipCountModule() {
        this(FULL_RELCOUNT_DEFAULT_ID);
    }

    public FullRelationshipCountModule(String id) {
        this(id, RelationshipCountStrategiesImpl.defaultStrategies());
    }

    public FullRelationshipCountModule(RelationshipCountStrategies relationshipCountStrategies) {
        this(FULL_RELCOUNT_DEFAULT_ID, relationshipCountStrategies);
    }

    public FullRelationshipCountModule(String id, RelationshipCountStrategies relationshipCountStrategies) {
        super(id);
        this.relationshipCountStrategies = relationshipCountStrategies;
        this.relationshipCountCache = new FullRelationshipCountCache(id, relationshipCountStrategies);
    }

    public FullRelationshipCounter fallingBackCounter(RelationshipType type, Direction direction) {
        return new FullFallingBackRelationshipCounter(type, direction, getId(), relationshipCountStrategies, getConfig());
    }

    public FullRelationshipCounter cachedCounter(RelationshipType type, Direction direction) {
        return new FullCachedRelationshipCounter(type, direction, getId(), getConfig());
    }

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
}
