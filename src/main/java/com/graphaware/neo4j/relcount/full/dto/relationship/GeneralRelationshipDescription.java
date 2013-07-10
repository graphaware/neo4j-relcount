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

import com.graphaware.neo4j.dto.common.relationship.HasTypeDirectionAndProperties;
import com.graphaware.neo4j.relcount.full.dto.property.GeneralPropertiesDescription;
import com.graphaware.neo4j.relcount.full.dto.property.PropertiesDescription;
import org.neo4j.graphdb.*;

import java.util.Map;

/**
 *
 */
public class GeneralRelationshipDescription extends BaseRelationshipDescription implements RelationshipDescription {

    @Override
    protected RelationshipDescription newRelationship(RelationshipType type, Direction direction, PropertiesDescription properties) {
        return new GeneralRelationshipDescription(type, direction, properties);
    }

    @Override
    protected PropertiesDescription newProperties(PropertyContainer propertyContainer) {
        return new GeneralPropertiesDescription(propertyContainer);
    }

    @Override
    protected PropertiesDescription newProperties(Map<String, String> properties) {
        return new GeneralPropertiesDescription(properties);
    }

    public GeneralRelationshipDescription(Relationship relationship, Node pointOfView) {
        super(relationship, pointOfView);
    }

    public GeneralRelationshipDescription(Relationship relationship, Node pointOfView, PropertiesDescription properties) {
        super(relationship, pointOfView, properties);
    }

    public GeneralRelationshipDescription(RelationshipType type, Direction direction) {
        super(type, direction);
    }

    public GeneralRelationshipDescription(RelationshipType type, Direction direction, PropertiesDescription properties) {
        super(type, direction, properties);
    }

    public GeneralRelationshipDescription(RelationshipType type, Direction direction, Map<String, String> properties) {
        super(type, direction, properties);
    }

    public GeneralRelationshipDescription(String string) {
        super(string);
    }

    public GeneralRelationshipDescription(HasTypeDirectionAndProperties<String, ?> relationship) {
        super(relationship);
    }
}
