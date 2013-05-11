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

package com.graphaware.neo4j.relcount.api;

import com.graphaware.neo4j.relcount.logic.RelationshipCountManagerImpl;
import com.graphaware.neo4j.representation.property.SimpleCopyMakingProperties;
import com.graphaware.neo4j.representation.relationship.StringConvertibleRelationship;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.RelationshipType;

import java.util.Map;

/**
 * Default production implementation of {@link RelationshipCounter}.
 */
public class RelationshipCounterImpl extends StringConvertibleRelationship<SimpleCopyMakingProperties> implements RelationshipCounter {

    /**
     * Construct a new relationship counter.
     *
     * @param type      type of the relationships to count.
     * @param direction direction of the relationships to count.
     */
    public RelationshipCounterImpl(RelationshipType type, Direction direction) {
        super(type, direction);
    }

    /**
     * Construct a new relationship counter.
     *
     * @param type       type.
     * @param direction  direction.
     * @param properties props.
     */
    protected RelationshipCounterImpl(RelationshipType type, Direction direction, SimpleCopyMakingProperties properties) {
        super(type, direction, properties);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected SimpleCopyMakingProperties newProperties(String string) {
        return new SimpleCopyMakingProperties(string);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected SimpleCopyMakingProperties newProperties(PropertyContainer propertyContainer) {
        return new SimpleCopyMakingProperties(propertyContainer);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected SimpleCopyMakingProperties newProperties(Map<String, String> properties) {
        return new SimpleCopyMakingProperties(properties);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RelationshipCounter with(String key, String value) {
        return new RelationshipCounterImpl(getType(), getDirection(), getProperties().with(key, value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int count(Node node) {
        return new RelationshipCountManagerImpl().getRelationshipCount(this, node);
    }
}
