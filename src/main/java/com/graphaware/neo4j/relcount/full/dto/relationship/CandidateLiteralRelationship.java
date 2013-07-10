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
import com.graphaware.neo4j.relcount.full.dto.property.CandidateLiteralProperties;
import com.graphaware.neo4j.relcount.full.dto.property.CandidateProperties;
import org.neo4j.graphdb.*;

import java.util.*;

/**
 *
 */
public class CandidateLiteralRelationship extends BaseCandidateRelationship<CandidateRelationship, CandidateProperties> implements CandidateRelationship {

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<CandidateRelationship> generateOneMoreGeneral() {
        return Collections.<CandidateRelationship>singleton(new CandidateGeneralizedRelationship(this));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<CandidateRelationship> generateAllMoreGeneral() {
        Set<CandidateRelationship> result = new TreeSet<>();
        result.add(this);
        result.addAll(new CandidateGeneralizedRelationship(this).generateAllMoreGeneral());
        return result;
    }

    @Override
    protected CandidateRelationship newRelationship(RelationshipType type, Direction direction, CandidateProperties properties) {
        return new CandidateLiteralRelationship(type, direction, properties);
    }

    @Override
    protected CandidateProperties newProperties(PropertyContainer propertyContainer) {
        return new CandidateLiteralProperties(propertyContainer);
    }

    @Override
    protected CandidateProperties newProperties(Map<String, String> properties) {
        Map<String, String> withoutLiteral = new HashMap<>(properties);
        withoutLiteral.remove(CandidateLiteralProperties.LITERAL);
        return new CandidateLiteralProperties(withoutLiteral);
    }

    public CandidateLiteralRelationship(Relationship relationship, Node pointOfView) {
        super(relationship, pointOfView);
    }

    public CandidateLiteralRelationship(Relationship relationship, Node pointOfView, CandidateProperties properties) {
        super(relationship, pointOfView, properties);
    }

    public CandidateLiteralRelationship(RelationshipType type, Direction direction) {
        super(type, direction);
    }

    public CandidateLiteralRelationship(RelationshipType type, Direction direction, CandidateProperties properties) {
        super(type, direction, properties);
    }

    public CandidateLiteralRelationship(RelationshipType type, Direction direction, Map<String, String> properties) {
        super(type, direction, properties);
    }

    public CandidateLiteralRelationship(String string) {
        super(string);
    }

    public CandidateLiteralRelationship(HasTypeDirectionAndProperties<String, ?> relationship) {
        super(relationship);
    }
}
