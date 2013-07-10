/*
 * Copyright (c) 2013 GraphAware
 *
 * This file is part of GraphAware.
 *
 * GraphAware is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details. You should have received a copy of
 * the GNU General Public License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

package com.graphaware.neo4j.relcount.common.handler;

import com.graphaware.neo4j.common.Change;
import com.graphaware.neo4j.common.Constants;
import com.graphaware.neo4j.tx.batch.IterableInputBatchExecutor;
import com.graphaware.neo4j.tx.batch.UnitOfWork;
import com.graphaware.neo4j.tx.event.api.FilteredLazyTransactionData;
import com.graphaware.neo4j.tx.event.api.ImprovedTransactionData;
import com.graphaware.neo4j.tx.event.strategy.IncludeAllNodes;
import com.graphaware.neo4j.tx.event.strategy.RelationshipInclusionStrategy;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

/**
 * {@link org.neo4j.graphdb.event.TransactionEventHandler} responsible for caching relationship counts on nodes.
 */
public abstract class RelationshipCountCachingTransactionEventHandler extends TransactionEventHandler.Adapter<Void> {

    private final RelationshipInclusionStrategy inclusionStrategy;

    /**
     * Construct a new event handler.
     *
     * @param inclusionStrategy strategy for selecting relationships to care about.
     */
    protected RelationshipCountCachingTransactionEventHandler(RelationshipInclusionStrategy inclusionStrategy) {
        this.inclusionStrategy = inclusionStrategy;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Void beforeCommit(TransactionData data) throws Exception {
        super.beforeCommit(data);

        FilteredLazyTransactionData transactionData = new FilteredLazyTransactionData(data);
        transactionData.addRelationshipInclusionStrategy(inclusionStrategy);
        transactionData.addNodeInclusionStrategy(IncludeAllNodes.getInstance());

        handleCreatedRelationships(transactionData);
        handleDeletedRelationships(transactionData);
        handleChangedRelationships(transactionData);

        return null;
    }

    private void handleCreatedRelationships(ImprovedTransactionData data) {
        for (Relationship createdRelationship : data.getAllCreatedRelationships()) {
            handleCreatedRelationship(createdRelationship, createdRelationship.getStartNode(), INCOMING);
            handleCreatedRelationship(createdRelationship, createdRelationship.getEndNode(), OUTGOING);
        }
    }

    private void handleDeletedRelationships(ImprovedTransactionData data) {
        for (Relationship deletedRelationship : data.getAllDeletedRelationships()) {
            Node startNode = deletedRelationship.getStartNode();
            if (!data.hasBeenDeleted(startNode)) {
                handleDeletedRelationship(deletedRelationship, startNode, INCOMING);
            }

            Node endNode = deletedRelationship.getEndNode();
            if (!data.hasBeenDeleted(endNode)) {
                handleDeletedRelationship(deletedRelationship, endNode, OUTGOING);
            }
        }
    }

    private void handleChangedRelationships(ImprovedTransactionData data) {
        for (Change<Relationship> changedRelationship : data.getAllChangedRelationships()) {
            Relationship current = changedRelationship.getCurrent();
            Relationship previous = changedRelationship.getPrevious();

            handleDeletedRelationship(previous, previous.getStartNode(), INCOMING);
            handleDeletedRelationship(previous, previous.getEndNode(), OUTGOING);
            handleCreatedRelationship(current, current.getStartNode(), INCOMING);
            handleCreatedRelationship(current, current.getEndNode(), OUTGOING);
        }
    }

    /**
     * Handle (i.e. cache) a created relationship.
     *
     * @param relationship     the has been created.
     * @param pointOfView      node whose point of view the created relationships is being handled, i.e. the one on which
     *                         the relationship count should be cached.
     * @param defaultDirection in case the relationship direction would be resolved to {@link Direction#BOTH}, what
     *                         should it actually be resolved to? This must be {@link Direction#OUTGOING} or {@link Direction#INCOMING},
     *                         never cache {@link Direction#BOTH}! (because its meaning would be unclear - is it just the
     *                         cyclical relationships, or all? Also, there would be trouble during compaction and eventually,
     *                         incoming and outgoing relationships could be compacted to BOTH, so it would be impossible
     *                         to find only incoming or outgoing.
     */
    protected abstract void handleCreatedRelationship(Relationship relationship, Node pointOfView, Direction defaultDirection);

    /**
     * Handle (i.e. cache) a deleted relationship.
     *
     * @param relationship     the has been deleted.
     * @param pointOfView      node whose point of view the deleted relationships is being handled, i.e. the one on which
     *                         the relationship count should be cached.
     * @param defaultDirection in case the relationship direction would be resolved to {@link Direction#BOTH}, what
     *                         should it actually be resolved to? This must be {@link Direction#OUTGOING} or {@link Direction#INCOMING},
     *                         never cache {@link Direction#BOTH}! (because its meaning would be unclear - is it just the
     *                         cyclical relationships, or all? Also, there would be trouble during compaction and eventually,
     *                         incoming and outgoing relationships could be compacted to BOTH, so it would be impossible
     *                         to find only incoming or outgoing.
     */
    protected abstract void handleDeletedRelationship(Relationship relationship, Node pointOfView, Direction defaultDirection);

    /**
     * Clear and rebuild all cached counts. NOTE: This is a potentially very expensive operation as it traverses the
     * entire graph! Use with care.
     *
     * @param databaseService to perform the operation on.
     */
    public void rebuildCachedCounts(GraphDatabaseService databaseService) {
        clearCachedCounts(databaseService);

        new IterableInputBatchExecutor<>(
                databaseService,
                1000,
                GlobalGraphOperations.at(databaseService).getAllRelationships(),
                new UnitOfWork<Relationship>() {
                    @Override
                    public void execute(GraphDatabaseService database, Relationship relationship) {
                        handleCreatedRelationship(relationship, relationship.getStartNode(), INCOMING);
                        handleCreatedRelationship(relationship, relationship.getEndNode(), OUTGOING);
                    }
                }).execute();

    }

    /**
     * Clear all cached counts. NOTE: This is a potentially very expensive operation as it traverses the
     * entire graph! Use with care.
     *
     * @param databaseService to perform the operation on.
     */
    public void clearCachedCounts(GraphDatabaseService databaseService) {
        new IterableInputBatchExecutor<>(
                databaseService,
                1000,
                GlobalGraphOperations.at(databaseService).getAllNodes(),
                new UnitOfWork<Node>() {
                    @Override
                    public void execute(GraphDatabaseService database, Node node) {
                        for (String key : node.getPropertyKeys()) {
                            if (key.startsWith(Constants.GA_REL_PREFIX)) {
                                node.removeProperty(key);
                            }
                        }
                    }
                }
        ).execute();
    }
}
