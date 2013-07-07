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

package com.graphaware.neo4j.relcount.full;


import com.graphaware.neo4j.relcount.full.logic.FullCachingRelationshipCountManager;
import com.graphaware.neo4j.relcount.full.logic.FullRelationshipCountTransactionEventHandler;
import com.graphaware.neo4j.relcount.full.logic.RelationshipCountCompactor;
import com.graphaware.neo4j.relcount.full.logic.ThresholdBasedRelationshipCountCompactor;
import com.graphaware.neo4j.tx.event.strategy.ExtractAllRelationshipProperties;
import com.graphaware.neo4j.tx.event.strategy.IncludeAllRelationships;
import com.graphaware.neo4j.tx.event.strategy.RelationshipInclusionStrategy;
import com.graphaware.neo4j.tx.event.strategy.RelationshipPropertiesExtractionStrategy;

/**
 * Factory for configuring and creating {@link com.graphaware.neo4j.relcount.full.logic.FullRelationshipCountTransactionEventHandler}.
 */
public class FullRelationshipCountTransactionEventHandlerFactory {

    /**
     * Create a {@link com.graphaware.neo4j.relcount.full.logic.FullRelationshipCountTransactionEventHandler} with default configuration.
     *
     * @return product.
     */
    public FullRelationshipCountTransactionEventHandler create() {
        FullCachingRelationshipCountManager countManager = defaultRelationshipCountManager();
        RelationshipCountCompactor countCompactor = defaultRelationshipCountCompactor(countManager);
        RelationshipInclusionStrategy relationshipInclusionStrategy = defaultIncludeAllRelationshipsStrategy();
        RelationshipPropertiesExtractionStrategy propertyExtractionStrategy = defaultPropertiesExtractionStrategy();

        return new FullRelationshipCountTransactionEventHandler(countManager, countCompactor, relationshipInclusionStrategy, propertyExtractionStrategy);
    }

    /**
     * Create a {@link com.graphaware.neo4j.relcount.full.logic.FullRelationshipCountTransactionEventHandler} with specific compaction threshold.
     *
     * @param threshold threshold.
     * @return product.
     */
    public FullRelationshipCountTransactionEventHandler create(int threshold) {
        FullCachingRelationshipCountManager countManager = defaultRelationshipCountManager();
        RelationshipCountCompactor countCompactor = defaultThresholdBasedRelationshipCountCompactor(threshold, countManager);
        RelationshipInclusionStrategy relationshipInclusionStrategy = defaultIncludeAllRelationshipsStrategy();
        RelationshipPropertiesExtractionStrategy propertyExtractionStrategy = defaultPropertiesExtractionStrategy();

        return new FullRelationshipCountTransactionEventHandler(countManager, countCompactor, relationshipInclusionStrategy, propertyExtractionStrategy);
    }

    /**
     * Create a {@link com.graphaware.neo4j.relcount.full.logic.FullRelationshipCountTransactionEventHandler} with specific strategy for selecting which relationships
     * will be counted.
     *
     * @param relationshipInclusionStrategy strategy.
     * @return product.
     */
    public FullRelationshipCountTransactionEventHandler create(RelationshipInclusionStrategy relationshipInclusionStrategy) {
        FullCachingRelationshipCountManager countManager = defaultRelationshipCountManager();
        RelationshipCountCompactor countCompactor = defaultRelationshipCountCompactor(countManager);
        RelationshipPropertiesExtractionStrategy propertyExtractionStrategy = defaultPropertiesExtractionStrategy();

        return new FullRelationshipCountTransactionEventHandler(countManager, countCompactor, relationshipInclusionStrategy, propertyExtractionStrategy);
    }

    /**
     * Create a {@link com.graphaware.neo4j.relcount.full.logic.FullRelationshipCountTransactionEventHandler} with specific strategy for extracting properties.
     *
     * @param propertyExtractionStrategy strategy.
     * @return product.
     */
    public FullRelationshipCountTransactionEventHandler create(RelationshipPropertiesExtractionStrategy propertyExtractionStrategy) {
        FullCachingRelationshipCountManager countManager = defaultRelationshipCountManager();
        RelationshipCountCompactor countCompactor = defaultRelationshipCountCompactor(countManager);
        RelationshipInclusionStrategy relationshipInclusionStrategy = defaultIncludeAllRelationshipsStrategy();

        return new FullRelationshipCountTransactionEventHandler(countManager, countCompactor, relationshipInclusionStrategy, propertyExtractionStrategy);
    }

    private ThresholdBasedRelationshipCountCompactor defaultRelationshipCountCompactor(FullCachingRelationshipCountManager countManager) {
        return new ThresholdBasedRelationshipCountCompactor(countManager);
    }

    /**
     * Create a {@link com.graphaware.neo4j.relcount.full.logic.FullRelationshipCountTransactionEventHandler} with specific compaction threshold
     * and specific strategy for property extraction.
     *
     * @param threshold                     threshold.
     * @param relationshipInclusionStrategy strategy.
     * @return product.
     */
    public FullRelationshipCountTransactionEventHandler create(int threshold, RelationshipInclusionStrategy relationshipInclusionStrategy) {
        FullCachingRelationshipCountManager countManager = defaultRelationshipCountManager();
        RelationshipCountCompactor countCompactor = defaultThresholdBasedRelationshipCountCompactor(threshold, countManager);
        RelationshipPropertiesExtractionStrategy propertyExtractionStrategy = defaultPropertiesExtractionStrategy();

        return new FullRelationshipCountTransactionEventHandler(countManager, countCompactor, relationshipInclusionStrategy, propertyExtractionStrategy);
    }

    /**
     * Create a {@link com.graphaware.neo4j.relcount.full.logic.FullRelationshipCountTransactionEventHandler} with specific compaction threshold
     * and specific strategy for selecting which relationships will be counted.
     *
     * @param threshold                  threshold.
     * @param propertyExtractionStrategy strategy.
     * @return product.
     */
    public FullRelationshipCountTransactionEventHandler create(int threshold, RelationshipPropertiesExtractionStrategy propertyExtractionStrategy) {
        FullCachingRelationshipCountManager countManager = defaultRelationshipCountManager();
        RelationshipCountCompactor countCompactor = defaultThresholdBasedRelationshipCountCompactor(threshold, countManager);
        RelationshipInclusionStrategy relationshipInclusionStrategy = defaultIncludeAllRelationshipsStrategy();

        return new FullRelationshipCountTransactionEventHandler(countManager, countCompactor, relationshipInclusionStrategy, propertyExtractionStrategy);
    }

    /**
     * Create a {@link com.graphaware.neo4j.relcount.full.logic.FullRelationshipCountTransactionEventHandler} with
     * specific strategy for selecting which relationships will be counted and specific strategy for property extraction.
     *
     * @param relationshipInclusionStrategy strategy.
     * @param propertyExtractionStrategy    strategy.
     * @return product.
     */
    public FullRelationshipCountTransactionEventHandler create(RelationshipInclusionStrategy relationshipInclusionStrategy, RelationshipPropertiesExtractionStrategy propertyExtractionStrategy) {
        FullCachingRelationshipCountManager countManager = defaultRelationshipCountManager();
        RelationshipCountCompactor countCompactor = defaultRelationshipCountCompactor(countManager);

        return new FullRelationshipCountTransactionEventHandler(countManager, countCompactor, relationshipInclusionStrategy, propertyExtractionStrategy);
    }

    /**
     * Create a {@link com.graphaware.neo4j.relcount.full.logic.FullRelationshipCountTransactionEventHandler} with with specific compaction threshold,
     * specific strategy for selecting which relationships will be counted, and specific strategy for property extraction.
     *
     * @param threshold                     threshold.
     * @param relationshipInclusionStrategy strategy.
     * @param propertyExtractionStrategy    strategy.
     * @return product.
     */
    public FullRelationshipCountTransactionEventHandler create(int threshold, RelationshipInclusionStrategy relationshipInclusionStrategy, RelationshipPropertiesExtractionStrategy propertyExtractionStrategy) {
        FullCachingRelationshipCountManager countManager = defaultRelationshipCountManager();
        RelationshipCountCompactor countCompactor = defaultThresholdBasedRelationshipCountCompactor(threshold, countManager);

        return new FullRelationshipCountTransactionEventHandler(countManager, countCompactor, relationshipInclusionStrategy, propertyExtractionStrategy);
    }

    //defaults

    private RelationshipCountCompactor defaultThresholdBasedRelationshipCountCompactor(int threshold, FullCachingRelationshipCountManager countManager) {
        return new ThresholdBasedRelationshipCountCompactor(threshold, countManager);
    }

    private RelationshipPropertiesExtractionStrategy defaultPropertiesExtractionStrategy() {
        return ExtractAllRelationshipProperties.getInstance();
    }

    private RelationshipInclusionStrategy defaultIncludeAllRelationshipsStrategy() {
        return IncludeAllRelationships.getInstance();
    }

    private FullCachingRelationshipCountManager defaultRelationshipCountManager() {
        return new FullCachingRelationshipCountManager();
    }
}
