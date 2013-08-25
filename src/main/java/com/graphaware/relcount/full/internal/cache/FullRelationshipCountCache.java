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

package com.graphaware.relcount.full.internal.cache;

import com.graphaware.framework.config.FrameworkConfigured;
import com.graphaware.propertycontainer.util.DirectionUtils;
import com.graphaware.relcount.common.internal.cache.BaseBatchFriendlyRelationshipCountCache;
import com.graphaware.relcount.common.internal.cache.BatchFriendlyRelationshipCountCache;
import com.graphaware.relcount.common.internal.node.RelationshipCountCachingNode;
import com.graphaware.relcount.full.internal.dto.relationship.CacheableRelationshipDescription;
import com.graphaware.relcount.full.internal.dto.relationship.CacheableRelationshipDescriptionImpl;
import com.graphaware.relcount.full.internal.node.FullRelationshipCountCachingNode;
import com.graphaware.relcount.full.internal.node.RelationshipCountCompactor;
import com.graphaware.relcount.full.internal.node.ThresholdBasedRelationshipCountCompactor;
import com.graphaware.relcount.full.strategy.RelationshipCountStrategies;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.Map;

/**
 * A full-blown implementation of {@link com.graphaware.relcount.common.internal.cache.RelationshipCountCache}.  It is "full" in
 * the sense that it cares about {@link org.neo4j.graphdb.RelationshipType}s, {@link org.neo4j.graphdb.Direction}s, and properties.
 */
public class FullRelationshipCountCache extends BaseBatchFriendlyRelationshipCountCache<CacheableRelationshipDescription> implements BatchFriendlyRelationshipCountCache, FrameworkConfigured {

    private final RelationshipCountStrategies relationshipCountStrategies;
    private final RelationshipCountCompactor relationshipCountCompactor;

    /**
     * Construct a new cache.
     *
     * @param id                          of the module this cache belongs to.
     * @param relationshipCountStrategies strategies for counting relationships. This class is specifically interested in the compaction threshold.
     */
    public FullRelationshipCountCache(String id, RelationshipCountStrategies relationshipCountStrategies) {
        super(id);
        this.relationshipCountStrategies = relationshipCountStrategies;
        this.relationshipCountCompactor = new ThresholdBasedRelationshipCountCompactor(relationshipCountStrategies.getCompactionThreshold());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected CacheableRelationshipDescription newCachedRelationship(Relationship relationship, Node pointOfView, Direction defaultDirection) {
        Map<String, String> extractedProperties = relationshipCountStrategies.getRelationshipPropertiesExtractionStrategy().extractProperties(relationship, pointOfView);
        return new CacheableRelationshipDescriptionImpl(relationship.getType(), DirectionUtils.resolveDirection(relationship, pointOfView, defaultDirection), extractedProperties);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected int relationshipWeight(Relationship relationship, Node pointOfView) {
        return relationshipCountStrategies.getRelationshipWeighingStrategy().getRelationshipWeight(relationship, pointOfView);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected RelationshipCountCachingNode<CacheableRelationshipDescription> newCachingNode(Node node) {
        return new FullRelationshipCountCachingNode(node, getConfig().createPrefix(id), getConfig().separator(), relationshipCountCompactor);
    }
}
