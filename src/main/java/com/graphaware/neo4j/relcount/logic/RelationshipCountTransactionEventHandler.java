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

package com.graphaware.neo4j.relcount.logic;

import com.graphaware.neo4j.relcount.representation.ComparableProperties;
import com.graphaware.neo4j.relcount.representation.ComparableRelationship;
import com.graphaware.neo4j.representation.property.SimpleProperties;
import org.apache.log4j.Logger;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;

import java.util.Collection;
import java.util.Map;

import static com.graphaware.neo4j.utils.iterable.IterableUtils.contains;
import static com.graphaware.neo4j.utils.tx.mutate.ChangeUtils.extractChangedRelationships;
import static com.graphaware.neo4j.utils.tx.mutate.DeleteUtils.extractDeletedRelationships;

/**
 * {@link org.neo4j.graphdb.event.TransactionEventHandler} responsible for caching relationship counts on nodes.
 */
public class RelationshipCountTransactionEventHandler extends TransactionEventHandler.Adapter<Void> {
    private static final Logger LOG = Logger.getLogger(RelationshipCountTransactionEventHandler.class);

    private final RelationshipCountManager countManager;
    private final RelationshipCountCompactor countCompactor;
    private final RelationshipInclusionStrategy inclusionStrategy;
    private final PropertyExtractionStrategy extractionStrategy;

    /**
     * Construct a new event handler.
     *
     * @param countManager       count manager.
     * @param countCompactor     cached count compactor.
     * @param inclusionStrategy  strategy for selecting relationships to care about.
     * @param extractionStrategy strategy for representing relationships.
     */
    public RelationshipCountTransactionEventHandler(RelationshipCountManager countManager, RelationshipCountCompactor countCompactor, RelationshipInclusionStrategy inclusionStrategy, PropertyExtractionStrategy extractionStrategy) {
        this.countManager = countManager;
        this.countCompactor = countCompactor;
        this.inclusionStrategy = inclusionStrategy;
        this.extractionStrategy = extractionStrategy;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Void beforeCommit(TransactionData data) throws Exception {
        super.beforeCommit(data);

        handleCreatedRelationships(data);
        handleDeletedRelationships(data);
        handleChangedRelationships(data);

        return null;
    }

    private void handleCreatedRelationships(TransactionData data) {
        for (Relationship createdRelationship : data.createdRelationships()) {
            if (include(createdRelationship)) {
                handleCreatedRelationship(createdRelationship, createdRelationship.getStartNode());
                handleCreatedRelationship(createdRelationship, createdRelationship.getEndNode());
            }
        }
    }

    private void handleDeletedRelationships(TransactionData data) {
        Collection<Relationship> deletedRelationships = extractDeletedRelationships(data);

        for (Relationship deletedRelationship : deletedRelationships) {
            if (include(deletedRelationship)) {
                Node startNode = deletedRelationship.getStartNode();
                if (!hasBeenDeleted(startNode, data)) {
                    handleDeletedRelationship(deletedRelationship, startNode);
                }

                Node endNode = deletedRelationship.getEndNode();
                if (!hasBeenDeleted(endNode, data)) {
                    handleDeletedRelationship(deletedRelationship, endNode);
                }
            }
        }
    }

    private void handleChangedRelationships(TransactionData data) {
        Map<Relationship, Relationship> changedRelationships = extractChangedRelationships(data);

        for (Map.Entry<Relationship, Relationship> changedRelationship : changedRelationships.entrySet()) {
            Relationship newRelationship = changedRelationship.getKey();
            Relationship oldRelationship = changedRelationship.getValue();

            if (include(oldRelationship)) {
                handleDeletedRelationship(oldRelationship, oldRelationship.getStartNode());
                handleDeletedRelationship(oldRelationship, oldRelationship.getEndNode());
            }

            if (include(newRelationship)) {
                handleCreatedRelationship(newRelationship, oldRelationship.getStartNode());
                handleCreatedRelationship(newRelationship, oldRelationship.getEndNode());
            }
        }
    }

    private void handleCreatedRelationship(Relationship relationship, Node pointOfView) {
        Node otherNode = relationship.getOtherNode(pointOfView);

        Map<String, String> realProperties = new SimpleProperties(relationship).getProperties();
        Map<String, String> extractedProperties = extractionStrategy.extractProperties(realProperties, otherNode);

        ComparableRelationship createdRelationship = new ComparableRelationship(relationship, pointOfView, new ComparableProperties(extractedProperties));

        if (countManager.incrementCount(createdRelationship, pointOfView)) {
            countCompactor.compactRelationshipCounts(createdRelationship, pointOfView);
        }
    }

    private void handleDeletedRelationship(Relationship relationship, Node pointOfView) {
        ComparableRelationship deletedRelationship = new ComparableRelationship(relationship, pointOfView, new ComparableProperties(relationship));

        if (!countManager.decrementCount(deletedRelationship, pointOfView)) {
            LOG.warn(relationship.toString() + " was out of sync on node " + pointOfView.getId());
        }
    }

    private boolean include(Relationship relationship) {
        return inclusionStrategy.include(relationship);
    }

    private boolean hasBeenDeleted(Node node, TransactionData data) {
        return contains(data.deletedNodes(), node);
    }
}
