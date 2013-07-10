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
import com.graphaware.neo4j.relcount.full.dto.property.CountableProperties;
import com.graphaware.neo4j.relcount.full.dto.property.LiterallyCountableProperties;
import org.neo4j.graphdb.*;

import java.util.*;

/**
 *
 */
public class LiterallyCountableRelationship extends GeneralizingComparableSerializableRelationship<CountableRelationship, CountableProperties> implements CountableRelationship {

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<CountableRelationship> generateOneMoreGeneral() {
        return Collections.<CountableRelationship>singleton(new GenerallyCountableRelationship(this));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<CountableRelationship> generateAllMoreGeneral() {
        Set<CountableRelationship> result = new TreeSet<>();
        result.add(this);
        result.addAll(new GenerallyCountableRelationship(this).generateAllMoreGeneral());
        return result;
    }

    @Override
    protected CountableRelationship newRelationship(RelationshipType type, Direction direction, CountableProperties properties) {
        return new LiterallyCountableRelationship(type, direction, properties);
    }

    @Override
    protected CountableProperties newProperties(PropertyContainer propertyContainer) {
        return new LiterallyCountableProperties(propertyContainer);
    }

    @Override
    protected CountableProperties newProperties(Map<String, String> properties) {
        Map<String, String> withoutLiteral = new HashMap<>(properties);
        withoutLiteral.remove(LiterallyCountableProperties.LITERAL);
        return new LiterallyCountableProperties(withoutLiteral);
    }

    public LiterallyCountableRelationship(Relationship relationship, Node pointOfView) {
        super(relationship, pointOfView);
    }

    public LiterallyCountableRelationship(Relationship relationship, Node pointOfView, CountableProperties properties) {
        super(relationship, pointOfView, properties);
    }

    public LiterallyCountableRelationship(RelationshipType type, Direction direction) {
        super(type, direction);
    }

    public LiterallyCountableRelationship(RelationshipType type, Direction direction, CountableProperties properties) {
        super(type, direction, properties);
    }

    public LiterallyCountableRelationship(RelationshipType type, Direction direction, Map<String, String> properties) {
        super(type, direction, properties);
    }

    public LiterallyCountableRelationship(String string) {
        super(string);
    }

    public LiterallyCountableRelationship(HasTypeDirectionAndProperties<String, ?> relationship) {
        super(relationship);
    }
}
