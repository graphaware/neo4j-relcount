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
import com.graphaware.neo4j.relcount.full.dto.property.LiteralPropertiesDescription;
import com.graphaware.neo4j.relcount.full.dto.property.PropertiesDescription;
import org.neo4j.graphdb.*;

import java.util.*;

/**
 *
 */
public class LiteralRelationshipDescription extends BaseRelationshipDescription implements RelationshipDescription {

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<RelationshipDescription> generateOneMoreGeneral() {
        return Collections.<RelationshipDescription>singleton(new GeneralRelationshipDescription(this));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<RelationshipDescription> generateAllMoreGeneral() {
        Set<RelationshipDescription> result = new TreeSet<>();
        result.add(this);
        result.addAll(new GeneralRelationshipDescription(this).generateAllMoreGeneral());
        return result;
    }

    @Override
    protected RelationshipDescription newRelationship(RelationshipType type, Direction direction, PropertiesDescription properties) {
        return new LiteralRelationshipDescription(type, direction, properties);
    }

    @Override
    protected PropertiesDescription newProperties(PropertyContainer propertyContainer) {
        return new LiteralPropertiesDescription(propertyContainer);
    }

    @Override
    protected PropertiesDescription newProperties(Map<String, String> properties) {
        Map<String, String> withoutLiteral = new HashMap<>(properties);
        withoutLiteral.remove(LiteralPropertiesDescription.LITERAL);
        return new LiteralPropertiesDescription(withoutLiteral);
    }

    public LiteralRelationshipDescription(Relationship relationship, Node pointOfView) {
        super(relationship, pointOfView);
    }

    public LiteralRelationshipDescription(Relationship relationship, Node pointOfView, PropertiesDescription properties) {
        super(relationship, pointOfView, properties);
    }

    public LiteralRelationshipDescription(RelationshipType type, Direction direction) {
        super(type, direction);
    }

    public LiteralRelationshipDescription(RelationshipType type, Direction direction, PropertiesDescription properties) {
        super(type, direction, properties);
    }

    public LiteralRelationshipDescription(RelationshipType type, Direction direction, Map<String, String> properties) {
        super(type, direction, properties);
    }

    public LiteralRelationshipDescription(String string) {
        super(string);
    }

    public LiteralRelationshipDescription(HasTypeDirectionAndProperties<String, ?> relationship) {
        super(relationship);
    }
}
