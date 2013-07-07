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

package com.graphaware.neo4j.relcount.simple;


import com.graphaware.neo4j.relcount.simple.logic.SimpleCachingRelationshipCountManager;
import com.graphaware.neo4j.relcount.simple.logic.SimpleCachingRelationshipCountManagerImpl;
import com.graphaware.neo4j.relcount.simple.logic.SimpleRelationshipCountTransactionEventHandler;
import com.graphaware.neo4j.tx.event.strategy.IncludeAllRelationships;
import com.graphaware.neo4j.tx.event.strategy.RelationshipInclusionStrategy;

/**
 * Factory for configuring and creating {@link com.graphaware.neo4j.relcount.full.logic.FullRelationshipCountTransactionEventHandler}.
 */
public class SimpleRelationshipCountTransactionEventHandlerFactory {

    /**
     * Create a {@link com.graphaware.neo4j.relcount.full.logic.FullRelationshipCountTransactionEventHandler} with default configuration.
     *
     * @return product.
     */
    public SimpleRelationshipCountTransactionEventHandler create() {
        SimpleCachingRelationshipCountManager countManager = defaultRelationshipCountManager();
        RelationshipInclusionStrategy relationshipInclusionStrategy = defaultIncludeAllRelationshipsStrategy();

        return new SimpleRelationshipCountTransactionEventHandler(countManager, relationshipInclusionStrategy);
    }

    /**
     * Create a {@link com.graphaware.neo4j.relcount.full.logic.FullRelationshipCountTransactionEventHandler} with specific strategy for selecting which relationships
     * will be counted.
     *
     * @param relationshipInclusionStrategy strategy.
     * @return product.
     */
    public SimpleRelationshipCountTransactionEventHandler create(RelationshipInclusionStrategy relationshipInclusionStrategy) {
        SimpleCachingRelationshipCountManager countManager = defaultRelationshipCountManager();

        return new SimpleRelationshipCountTransactionEventHandler(countManager, relationshipInclusionStrategy);
    }

    //defaults

    private RelationshipInclusionStrategy defaultIncludeAllRelationshipsStrategy() {
        return IncludeAllRelationships.getInstance();
    }

    private SimpleCachingRelationshipCountManager defaultRelationshipCountManager() {
        return new SimpleCachingRelationshipCountManagerImpl();
    }
}
