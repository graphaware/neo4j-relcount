package com.graphaware.neo4j.relcount.common.module;

import com.graphaware.neo4j.common.Change;
import com.graphaware.neo4j.framework.GraphAwareModule;
import com.graphaware.neo4j.relcount.common.logic.RelationshipCountCache;
import com.graphaware.neo4j.tx.event.api.ImprovedTransactionData;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

/**
 * Abstract base-class for {@link GraphAwareModule}s that with to provide caching capabilities for relationship counting.
 */
public abstract class RelationshipCountModule implements GraphAwareModule {

    private static final String DEFAULT_ID = "RC";

    private final String id;

    /**
     * Create a module with {@link #DEFAULT_ID}.
     */
    public RelationshipCountModule() {
        this.id = DEFAULT_ID;
    }

    /**
     * Create a module with a specific ID. Use this if you want to use multiple modules at the same time,
     * each (perhaps) with a different configuration.
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
        getRelationshipCountCache().rebuildCachedCounts(database);
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
