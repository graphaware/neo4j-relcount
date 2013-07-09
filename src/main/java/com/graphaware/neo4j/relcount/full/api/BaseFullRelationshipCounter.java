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

import com.graphaware.neo4j.dto.common.relationship.BaseDirectedRelationship;
import com.graphaware.neo4j.dto.string.property.CopyMakingSerializableProperties;
import com.graphaware.neo4j.dto.string.property.CopyMakingSerializablePropertiesImpl;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.RelationshipType;

import java.util.Map;

/**
 * A naive {@link com.graphaware.neo4j.relcount.full.api.FullRelationshipCounter} that counts matching relationships by inspecting all
 * {@link org.neo4j.graphdb.Node}'s {@link org.neo4j.graphdb.Relationship}s. It delegates the work to {@link com.graphaware.neo4j.relcount.full.manager.FullNaiveRelationshipCountManager}.
 * <p/>
 * Because relationships are counted on the fly (no caching performed), this can be used without any {@link org.neo4j.graphdb.event.TransactionEventHandler}s
 * and on already existing graphs.
 */
public abstract class BaseFullRelationshipCounter extends BaseDirectedRelationship<String, CopyMakingSerializableProperties<?>> {

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
     * Construct a counter.
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
    protected CopyMakingSerializableProperties newProperties(PropertyContainer propertyContainer) {
        return new CopyMakingSerializablePropertiesImpl(propertyContainer);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected CopyMakingSerializableProperties newProperties(Map<String, String> properties) {
        return new CopyMakingSerializablePropertiesImpl(properties);
    }
}
