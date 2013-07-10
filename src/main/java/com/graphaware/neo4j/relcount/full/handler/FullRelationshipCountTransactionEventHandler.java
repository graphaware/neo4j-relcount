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

package com.graphaware.neo4j.relcount.full.handler;

import com.graphaware.neo4j.relcount.common.handler.RelationshipCountCachingTransactionEventHandler;
import com.graphaware.neo4j.relcount.full.compactor.RelationshipCountCompactor;
import com.graphaware.neo4j.relcount.full.dto.property.CandidateLiteralProperties;
import com.graphaware.neo4j.relcount.full.dto.relationship.CandidateLiteralRelationship;
import com.graphaware.neo4j.relcount.full.manager.FullCachingRelationshipCountManager;
import com.graphaware.neo4j.tx.event.strategy.RelationshipInclusionStrategy;
import com.graphaware.neo4j.tx.event.strategy.RelationshipPropertiesExtractionStrategy;
import org.apache.log4j.Logger;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.Map;

/**
 * {@link org.neo4j.graphdb.event.TransactionEventHandler} responsible for caching relationship counts on nodes.
 */
public class FullRelationshipCountTransactionEventHandler extends RelationshipCountCachingTransactionEventHandler {
    private static final Logger LOG = Logger.getLogger(FullRelationshipCountTransactionEventHandler.class);

    private final FullCachingRelationshipCountManager countManager;
    private final RelationshipCountCompactor countCompactor;
    private final RelationshipPropertiesExtractionStrategy extractionStrategy;

    /**
     * Construct a new event handler.
     *
     * @param countManager       count manager.
     * @param countCompactor     cached count compactor.
     * @param inclusionStrategy  strategy for selecting relationships to care about.
     * @param extractionStrategy strategy for representing relationships.
     */
    public FullRelationshipCountTransactionEventHandler(FullCachingRelationshipCountManager countManager, RelationshipCountCompactor countCompactor, RelationshipInclusionStrategy inclusionStrategy, RelationshipPropertiesExtractionStrategy extractionStrategy) {
        super(inclusionStrategy);
        this.countManager = countManager;
        this.countCompactor = countCompactor;
        this.extractionStrategy = extractionStrategy;
    }

    protected void handleCreatedRelationship(Relationship relationship, Node pointOfView) {
        Map<String, String> extractedProperties = extractionStrategy.extractProperties(relationship, pointOfView);

        CandidateLiteralRelationship createdRelationship = new CandidateLiteralRelationship(relationship, pointOfView, new CandidateLiteralProperties(extractedProperties));

        if (countManager.incrementCount(createdRelationship, pointOfView)) {
            countCompactor.compactRelationshipCounts(pointOfView); //todo async
        }
    }

    protected void handleDeletedRelationship(Relationship relationship, Node pointOfView) {
        CandidateLiteralRelationship deletedRelationship = new CandidateLiteralRelationship(relationship, pointOfView, new CandidateLiteralProperties(relationship));

        if (!countManager.decrementCount(deletedRelationship, pointOfView)) {
            LOG.warn(deletedRelationship.toString() + " was out of sync on node " + pointOfView.getId());
        }
    }
}
