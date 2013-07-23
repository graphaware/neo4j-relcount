package com.graphaware.neo4j.relcount.common.module;

import com.graphaware.neo4j.framework.GraphAwareModule;
import com.graphaware.neo4j.framework.config.BaseFrameworkConfigured;
import com.graphaware.neo4j.framework.config.FrameworkConfiguration;
import com.graphaware.neo4j.framework.config.FrameworkConfigured;
import com.graphaware.neo4j.misc.Change;
import com.graphaware.neo4j.relcount.common.logic.RelationshipCountCache;
import com.graphaware.neo4j.tx.event.api.ImprovedTransactionData;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

/**
 * Base-class for {@link GraphAwareModule}s that wish to provide caching capabilities for relationship counting.
 */
public abstract class RelationshipCountModule extends BaseFrameworkConfigured implements GraphAwareModule, FrameworkConfigured {

    private final String id;

    /**
     * Create a module.
     *
     * @param id of this module. Should be a short meaningful String.
     */
    public RelationshipCountModule(String id) {
        this.id = id;
    }

    /**
     * Get the {@link RelationshipCountCache} used by this module.
     *
     * @return relationship count cache.
     */
    protected abstract RelationshipCountCache getRelationshipCountCache();

    /**
     * {@inheritDoc}
     */
    @Override
    public String getId() {
        return id;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(GraphDatabaseService database) {
        getRelationshipCountCache().buildCachedCounts(database);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reinitialize(GraphDatabaseService database) {
        getRelationshipCountCache().clearCachedCounts(database);
        initialize(database);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void beforeCommit(ImprovedTransactionData transactionData) {
        handleCreatedRelationships(transactionData);
        handleDeletedRelationships(transactionData);
        handleChangedRelationships(transactionData);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configurationChanged(FrameworkConfiguration configuration) {
        super.configurationChanged(configuration);
        getRelationshipCountCache().configurationChanged(configuration);
    }

    //All explicit directions below are just for the case where we're dealing with a self-relationship (same start
    //and end node). It doesn't matter which one goes where, as long as both are present).

    private void handleCreatedRelationships(ImprovedTransactionData data) {
        for (Relationship createdRelationship : data.getAllCreatedRelationships()) {
            getRelationshipCountCache().handleCreatedRelationship(createdRelationship, createdRelationship.getStartNode(), INCOMING);
            getRelationshipCountCache().handleCreatedRelationship(createdRelationship, createdRelationship.getEndNode(), OUTGOING);
        }
    }

    private void handleDeletedRelationships(ImprovedTransactionData data) {
        for (Relationship deletedRelationship : data.getAllDeletedRelationships()) {
            Node startNode = deletedRelationship.getStartNode();
            if (!data.hasBeenDeleted(startNode)) {
                getRelationshipCountCache().handleDeletedRelationship(deletedRelationship, startNode, INCOMING);
            }

            Node endNode = deletedRelationship.getEndNode();
            if (!data.hasBeenDeleted(endNode)) {
                getRelationshipCountCache().handleDeletedRelationship(deletedRelationship, endNode, Direction.OUTGOING);
            }
        }
    }

    private void handleChangedRelationships(ImprovedTransactionData data) {
        for (Change<Relationship> changedRelationship : data.getAllChangedRelationships()) {
            Relationship current = changedRelationship.getCurrent();
            Relationship previous = changedRelationship.getPrevious();

            getRelationshipCountCache().handleDeletedRelationship(previous, previous.getStartNode(), Direction.INCOMING);
            getRelationshipCountCache().handleDeletedRelationship(previous, previous.getEndNode(), Direction.OUTGOING);
            getRelationshipCountCache().handleCreatedRelationship(current, current.getStartNode(), Direction.INCOMING);
            getRelationshipCountCache().handleCreatedRelationship(current, current.getEndNode(), Direction.OUTGOING);
        }
    }
}
