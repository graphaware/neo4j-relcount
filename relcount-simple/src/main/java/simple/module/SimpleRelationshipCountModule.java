package simple.module;

import com.graphaware.neo4j.relcount.common.logic.RelationshipCountCache;
import com.graphaware.neo4j.relcount.common.module.RelationshipCountModule;
import com.graphaware.neo4j.tx.event.strategy.IncludeAllRelationships;
import com.graphaware.neo4j.tx.event.strategy.InclusionStrategies;
import com.graphaware.neo4j.tx.event.strategy.InclusionStrategiesImpl;
import com.graphaware.neo4j.tx.event.strategy.RelationshipInclusionStrategy;
import simple.logic.SimpleRelationshipCountCache;

import static simple.Constants.SIMPLE_RELCOUNT_ID;

/**
 *
 */
public class SimpleRelationshipCountModule extends RelationshipCountModule {

    private final RelationshipCountCache relationshipCountCache;
    private final InclusionStrategies inclusionStrategies;

    public SimpleRelationshipCountModule() {
        this(IncludeAllRelationships.getInstance());
    }

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
