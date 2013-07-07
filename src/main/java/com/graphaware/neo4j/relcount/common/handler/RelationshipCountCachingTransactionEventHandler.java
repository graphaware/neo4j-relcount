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
import com.graphaware.neo4j.tx.event.api.FilteredLazyTransactionData;
import com.graphaware.neo4j.tx.event.api.ImprovedTransactionData;
import com.graphaware.neo4j.tx.event.strategy.IncludeAllNodes;
import com.graphaware.neo4j.tx.event.strategy.RelationshipInclusionStrategy;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;

import static com.graphaware.neo4j.common.Constants.GA_REL_PREFIX;

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
            if (include(createdRelationship)) {
                handleCreatedRelationship(createdRelationship, createdRelationship.getStartNode());
                handleCreatedRelationship(createdRelationship, createdRelationship.getEndNode());
            }
        }
    }

    private void handleDeletedRelationships(ImprovedTransactionData data) {
        for (Relationship deletedRelationship : data.getAllDeletedRelationships()) {
            if (include(deletedRelationship)) {
                Node startNode = deletedRelationship.getStartNode();
                if (!data.hasBeenDeleted(startNode)) {
                    handleDeletedRelationship(deletedRelationship, startNode);
                }

                Node endNode = deletedRelationship.getEndNode();
                if (!data.hasBeenDeleted(endNode)) {
                    handleDeletedRelationship(deletedRelationship, endNode);
                }
            }
        }
    }

    private void handleChangedRelationships(ImprovedTransactionData data) {
        for (Change<Relationship> changedRelationship : data.getAllChangedRelationships()) {
            Relationship current = changedRelationship.getCurrent();
            Relationship previous = changedRelationship.getPrevious();

            if (include(previous)) {
                handleDeletedRelationship(previous, previous.getStartNode());
                handleDeletedRelationship(previous, previous.getEndNode());
            }

            if (include(current)) {
                handleCreatedRelationship(current, current.getStartNode());
                handleCreatedRelationship(current, current.getEndNode());
            }
        }
    }

    /**
     * Handle (i.e. cache) a created relationship.
     *
     * @param relationship the has been created.
     * @param pointOfView  node whose point of view the created relationships is being handled, i.e. the one on which
     *                     the relationship count should be cached.
     */
    protected abstract void handleCreatedRelationship(Relationship relationship, Node pointOfView);

    /**
     * Handle (i.e. cache) a deleted relationship.
     *
     * @param relationship the has been deleted.
     * @param pointOfView  node whose point of view the deleted relationships is being handled, i.e. the one on which
     *                     the relationship count should be cached.
     */
    protected abstract void handleDeletedRelationship(Relationship relationship, Node pointOfView);

    //todo move away into some kind of AllBusinessRelationshipsStrategy
    private boolean include(Relationship relationship) {
        return !relationship.getType().name().startsWith(GA_REL_PREFIX);
    }
}
