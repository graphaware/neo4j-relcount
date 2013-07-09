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

package com.graphaware.neo4j.relcount.full.dto.relationship;

import com.graphaware.neo4j.dto.common.relationship.ImmutableDirectedRelationship;
import com.graphaware.neo4j.relcount.full.dto.property.CountableProperties;
import com.graphaware.neo4j.relcount.full.dto.property.GenerallyCountableProperties;
import org.neo4j.graphdb.*;

import java.util.Map;

/**
 *
 */
public class GenerallyCountableRelationship extends GeneralizingComparableSerializableRelationship<CountableRelationship, CountableProperties> implements CountableRelationship {

    @Override
    protected CountableRelationship newRelationship(RelationshipType type, Direction direction, CountableProperties properties) {
        return new GenerallyCountableRelationship(type, direction, properties);
    }

    @Override
    protected CountableProperties newProperties(PropertyContainer propertyContainer) {
        return new GenerallyCountableProperties(propertyContainer);
    }

    @Override
    protected CountableProperties newProperties(Map<String, String> properties) {
        return new GenerallyCountableProperties(properties);
    }

    public GenerallyCountableRelationship(Relationship relationship, Node pointOfView) {
        super(relationship, pointOfView);
    }

    public GenerallyCountableRelationship(Relationship relationship, Node pointOfView, CountableProperties properties) {
        super(relationship, pointOfView, properties);
    }

    public GenerallyCountableRelationship(RelationshipType type, Direction direction) {
        super(type, direction);
    }

    public GenerallyCountableRelationship(RelationshipType type, Direction direction, CountableProperties properties) {
        super(type, direction, properties);
    }

    public GenerallyCountableRelationship(RelationshipType type, Direction direction, Map<String, String> properties) {
        super(type, direction, properties);
    }

    public GenerallyCountableRelationship(String string) {
        super(string);
    }

    public GenerallyCountableRelationship(ImmutableDirectedRelationship<String, CountableProperties> relationship) {
        super(relationship);
    }
}
