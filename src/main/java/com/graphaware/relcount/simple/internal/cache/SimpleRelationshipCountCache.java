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

package com.graphaware.relcount.simple.internal.cache;

import com.graphaware.propertycontainer.dto.common.relationship.SerializableTypeAndDirection;
import com.graphaware.propertycontainer.dto.string.relationship.SerializableDirectedRelationshipImpl;
import com.graphaware.propertycontainer.util.DirectionUtils;
import com.graphaware.relcount.common.internal.cache.BaseBatchFriendlyRelationshipCountCache;
import com.graphaware.relcount.common.internal.cache.BatchFriendlyRelationshipCountCache;
import com.graphaware.relcount.common.internal.node.RelationshipCountCachingNode;
import com.graphaware.relcount.simple.internal.node.SimpleRelationshipCountCachingNode;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * A simple implementation of {@link com.graphaware.relcount.common.internal.cache.RelationshipCountCache}. It is simple in
 * the sense that it only cares about {@link org.neo4j.graphdb.RelationshipType}s and {@link org.neo4j.graphdb.Direction}s;
 * it completely ignores {@link org.neo4j.graphdb.Relationship} properties.
 */
public class SimpleRelationshipCountCache extends BaseBatchFriendlyRelationshipCountCache<SerializableTypeAndDirection> implements BatchFriendlyRelationshipCountCache {

    /**
     * Construct a new cache.
     *
     * @param id of the module this cache belongs to.
     */
    public SimpleRelationshipCountCache(String id) {
        super(id);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected SerializableTypeAndDirection newCachedRelationship(Relationship relationship, Node pointOfView, Direction defaultDirection) {
        return new SerializableDirectedRelationshipImpl(relationship.getType(), DirectionUtils.resolveDirection(relationship, pointOfView, defaultDirection));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected RelationshipCountCachingNode<SerializableTypeAndDirection> newCachingNode(Node node) {
        return new SimpleRelationshipCountCachingNode(node, getConfig().createPrefix(id), getConfig().separator());
    }
}
