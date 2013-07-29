package com.graphaware.neo4j.relcount.common.module;

import com.graphaware.neo4j.framework.GraphAwareModule;
import com.graphaware.neo4j.framework.config.BaseFrameworkConfigured;
import com.graphaware.neo4j.framework.config.FrameworkConfiguration;
import com.graphaware.neo4j.framework.config.FrameworkConfigured;
import com.graphaware.neo4j.misc.Change;
import com.graphaware.neo4j.relcount.common.internal.cache.BatchFriendlyRelationshipCountCache;
import com.graphaware.neo4j.relcount.common.internal.cache.RelationshipCountCache;
import com.graphaware.neo4j.tx.batch.IterableInputBatchExecutor;
import com.graphaware.neo4j.tx.batch.UnitOfWork;
import com.graphaware.neo4j.tx.event.api.ImprovedTransactionData;
import com.graphaware.neo4j.tx.event.propertycontainer.filtered.FilteredRelationship;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.Collection;

import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

/**
 * Base-class for {@link GraphAwareModule}s that wish to provide caching capabilities for relationship counting.
 */
public abstract class RelationshipCountModule extends BaseFrameworkConfigured implements GraphAwareModule, FrameworkConfigured {

    public static final int BATCH_THRESHOLD = 50;
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
    protected abstract BatchFriendlyRelationshipCountCache getRelationshipCountCache();

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
        buildCachedCounts(database);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reinitialize(GraphDatabaseService database) {
        clearCachedCounts(database);
        initialize(database);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void beforeCommit(ImprovedTransactionData transactionData) {
        if (isBatch(transactionData)) {
            getRelationshipCountCache().startBatchMode();
        }

        handleCreatedRelationships(transactionData);
        handleDeletedRelationships(transactionData);
        handleChangedRelationships(transactionData);

        if (isBatch(transactionData)) {
            getRelationshipCountCache().endBatchMode();
        }
    }

    private boolean isBatch(ImprovedTransactionData transactionData) {
        return transactionData.getAllCreatedRelationships().size() > BATCH_THRESHOLD
                || transactionData.getAllDeletedRelationships().size() > BATCH_THRESHOLD
                || transactionData.getAllChangedRelationships().size() > BATCH_THRESHOLD;
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
        Collection<Relationship> allCreatedRelationships = data.getAllCreatedRelationships();

        for (Relationship createdRelationship : allCreatedRelationships) {
            getRelationshipCountCache().handleCreatedRelationship(createdRelationship, createdRelationship.getStartNode(), INCOMING);
            getRelationshipCountCache().handleCreatedRelationship(createdRelationship, createdRelationship.getEndNode(), OUTGOING);
        }
    }

    private void handleDeletedRelationships(ImprovedTransactionData data) {
        Collection<Relationship> allDeletedRelationships = data.getAllDeletedRelationships();

        for (Relationship deletedRelationship : allDeletedRelationships) {
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
        Collection<Change<Relationship>> allChangedRelationships = data.getAllChangedRelationships();

        for (Change<Relationship> changedRelationship : allChangedRelationships) {
            Relationship current = changedRelationship.getCurrent();
            Relationship previous = changedRelationship.getPrevious();

            getRelationshipCountCache().handleDeletedRelationship(previous, previous.getStartNode(), Direction.INCOMING);
            getRelationshipCountCache().handleDeletedRelationship(previous, previous.getEndNode(), Direction.OUTGOING);
            getRelationshipCountCache().handleCreatedRelationship(current, current.getStartNode(), Direction.INCOMING);
            getRelationshipCountCache().handleCreatedRelationship(current, current.getEndNode(), Direction.OUTGOING);
        }

    }

    /**
     * Clear all cached counts. NOTE: This is a potentially very expensive operation as it traverses the
     * entire graph! Use with care.
     *
     * @param databaseService to perform the operation on.
     */
    private void clearCachedCounts(GraphDatabaseService databaseService) {
        new IterableInputBatchExecutor<>(
                databaseService,
                500,
                GlobalGraphOperations.at(databaseService).getAllNodes(),
                new UnitOfWork<Node>() {
                    @Override
                    public void execute(GraphDatabaseService database, Node node) {
                        for (String key : node.getPropertyKeys()) {
                            if (key.startsWith(getConfig().createPrefix(id))) {
                                node.removeProperty(key);
                            }
                        }
                    }
                }
        ).execute();
    }

    /**
     * Clear and rebuild all cached counts. NOTE: This is a potentially very expensive operation as it traverses the
     * entire graph! Use with care.
     *
     * @param databaseService to perform the operation on.
     */
    private void buildCachedCounts(GraphDatabaseService databaseService) {
        new IterableInputBatchExecutor<>(
                databaseService,
                100,
                GlobalGraphOperations.at(databaseService).getAllNodes(),
                new UnitOfWork<Node>() {
                    @Override
                    public void execute(GraphDatabaseService database, Node node) {
                        getRelationshipCountCache().startBatchMode();

                        for (Relationship relationship : node.getRelationships(OUTGOING)) {
                            Relationship filtered = new FilteredRelationship(relationship, getInclusionStrategies());
                            getRelationshipCountCache().handleCreatedRelationship(filtered, filtered.getStartNode(), Direction.OUTGOING);
                        }

                        for (Relationship relationship : node.getRelationships(INCOMING)) {
                            Relationship filtered = new FilteredRelationship(relationship, getInclusionStrategies());
                            getRelationshipCountCache().handleCreatedRelationship(filtered, filtered.getEndNode(), Direction.INCOMING);
                        }

                        getRelationshipCountCache().endBatchMode();
                    }
                }).execute();

    }
}
