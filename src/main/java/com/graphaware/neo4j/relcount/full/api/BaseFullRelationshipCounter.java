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

package com.graphaware.neo4j.relcount.full.api;

import com.graphaware.neo4j.dto.common.relationship.HasDirectionAndType;
import com.graphaware.neo4j.dto.string.property.CopyMakingSerializableProperties;
import com.graphaware.neo4j.dto.string.relationship.CopyMakingDirectedSerializableRelationship;
import com.graphaware.neo4j.relcount.common.manager.RelationshipCountManager;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.RelationshipType;

import java.util.Map;

/**
 * Base class for {@link FullRelationshipCounter} implementations, allowing subclasses to choose which
 * {@link RelationshipCountManager} to use.
 */
public abstract class BaseFullRelationshipCounter extends CopyMakingDirectedSerializableRelationship<CopyMakingSerializableProperties, FullRelationshipCounter> implements FullRelationshipCounter {

    /**
     * Construct a new relationship counter.
     *
     * @param type      type of the relationships to count.
     * @param direction direction of the relationships to count.
     */
    protected BaseFullRelationshipCounter(RelationshipType type, Direction direction) {
        super(type, direction);
    }

    /**
     * Construct a new relationship counter.
     *
     * @param type       type.
     * @param direction  direction.
     * @param properties props.
     */
    protected BaseFullRelationshipCounter(RelationshipType type, Direction direction, CopyMakingSerializableProperties properties) {
        super(type, direction, properties);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected FullRelationshipCounter newRelationship(RelationshipType type, Direction direction, CopyMakingSerializableProperties properties) {
        return newRelationshipCounter(type, direction, properties);
    }

    /**
     * Create a new instance of the concrete implementation.
     *
     * @param type       type.
     * @param direction  direction.
     * @param properties props.
     * @return new instance.
     */
    protected abstract FullRelationshipCounter newRelationshipCounter(RelationshipType type, Direction direction, CopyMakingSerializableProperties properties);

    /**
     * {@inheritDoc}
     */
    @Override
    protected CopyMakingSerializableProperties newProperties(PropertyContainer propertyContainer) {
        return new CopyMakingSerializableProperties(propertyContainer);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected CopyMakingSerializableProperties newProperties(Map<String, String> properties) {
        return new CopyMakingSerializableProperties(properties);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int count(Node node) {
        return getRelationshipCountManager().getRelationshipCount(this, node);
    }

    /**
     * Return the {@link RelationshipCountManager} used this implementation.
     *
     * @return relationship count manager.
     */
    protected abstract RelationshipCountManager<HasDirectionAndType> getRelationshipCountManager();
}
