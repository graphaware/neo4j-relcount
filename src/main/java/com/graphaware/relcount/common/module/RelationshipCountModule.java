package com.graphaware.relcount.common.module;

import com.graphaware.framework.GraphAwareModule;
import com.graphaware.framework.config.BaseFrameworkConfigured;
import com.graphaware.framework.config.FrameworkConfiguration;
import com.graphaware.framework.config.FrameworkConfigured;
import com.graphaware.relcount.common.internal.cache.RelationshipCountCache;
import com.graphaware.tx.event.batch.api.TransactionSimulatingBatchInserter;
import com.graphaware.tx.event.batch.propertycontainer.inserter.BatchInserterNode;
import com.graphaware.tx.event.improved.api.Change;
import com.graphaware.tx.event.improved.api.ImprovedTransactionData;
import com.graphaware.tx.event.improved.propertycontainer.filtered.FilteredNode;
import com.graphaware.tx.executor.batch.IterableInputBatchTransactionExecutor;
import com.graphaware.tx.executor.batch.UnitOfWork;
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
    public void initialize(TransactionSimulatingBatchInserter batchInserter) {
        buildCachedCounts(batchInserter);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reinitialize(TransactionSimulatingBatchInserter batchInserter) {
        clearCachedCounts(batchInserter);
        initialize(batchInserter);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void beforeCommit(ImprovedTransactionData transactionData) {
        getRelationshipCountCache().startCaching();

        try {
            handleCreatedRelationships(transactionData);
            handleDeletedRelationships(transactionData);
            handleChangedRelationships(transactionData);
        } finally {
            getRelationshipCountCache().endCaching();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configurationChanged(FrameworkConfiguration configuration) {
        super.configurationChanged(configuration);
        getRelationshipCountCache().configurationChanged(configuration);
    }

    //All explicit directions below are just for the case where we're dealing with a loop (same start
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
        new IterableInputBatchTransactionExecutor<>(
                databaseService,
                500,
                GlobalGraphOperations.at(databaseService).getAllNodes(),
                new UnitOfWork<Node>() {
                    @Override
                    public void execute(GraphDatabaseService database, Node node, int batchNumber, int stepNumber) {
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
     * Clear all cached counts. NOTE: This is a potentially very expensive operation as it traverses the
     * entire graph! Use with care.
     *
     * @param batchInserter to perform the operation on.
     */
    private void clearCachedCounts(TransactionSimulatingBatchInserter batchInserter) {
        for (long nodeId : batchInserter.getAllNodes()) {
            for (String key : batchInserter.getNodeProperties(nodeId).keySet()) {
                if (key.startsWith(getConfig().createPrefix(id))) {
                    batchInserter.removeNodeProperty(nodeId, key);
                }
            }
        }
    }

    /**
     * Clear and rebuild all cached counts. NOTE: This is a potentially very expensive operation as it traverses the
     * entire graph! Use with care.
     *
     * @param databaseService to perform the operation on.
     */
    private void buildCachedCounts(GraphDatabaseService databaseService) {
        new IterableInputBatchTransactionExecutor<>(
                databaseService,
                100,
                GlobalGraphOperations.at(databaseService).getAllNodes(),
                new UnitOfWork<Node>() {
                    @Override
                    public void execute(GraphDatabaseService database, Node node, int batchNumber, int stepNumber) {
                        Node filteredNode = new FilteredNode(node, getInclusionStrategies());

                        buildCachedCounts(filteredNode);

                    }
                }).execute();
    }

    /**
     * Clear and rebuild all cached counts. NOTE: This is a potentially very expensive operation as it traverses the
     * entire graph! Use with care.
     *
     * @param batchInserter to perform the operation on.
     */
    private void buildCachedCounts(TransactionSimulatingBatchInserter batchInserter) {
        for (long nodeId : batchInserter.getAllNodes()) {
            Node filteredNode = new FilteredNode(new BatchInserterNode(nodeId, batchInserter), getInclusionStrategies());

            buildCachedCounts(filteredNode);
        }
    }

    /**
     * Build cached counts for a node.
     *
     * @param filteredNode filtered node.
     */
    private void buildCachedCounts(Node filteredNode) {
        getRelationshipCountCache().startCaching();

        for (Relationship relationship : filteredNode.getRelationships()) {
            getRelationshipCountCache().handleCreatedRelationship(relationship, filteredNode, Direction.OUTGOING);

            if (relationship.getStartNode().getId() == relationship.getEndNode().getId()) {
                getRelationshipCountCache().handleCreatedRelationship(relationship, filteredNode, Direction.INCOMING);
            }
        }

        getRelationshipCountCache().endCaching();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String asString() {
        return id;
    }
}
