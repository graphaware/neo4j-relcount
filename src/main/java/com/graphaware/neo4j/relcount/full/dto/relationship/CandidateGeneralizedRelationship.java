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
import com.graphaware.neo4j.relcount.full.dto.property.CandidateGeneralizedProperties;
import com.graphaware.neo4j.relcount.full.dto.property.CandidateProperties;
import org.neo4j.graphdb.*;

import java.util.Map;

/**
 *
 */
public class CandidateGeneralizedRelationship extends BaseCandidateRelationship<CandidateRelationship, CandidateProperties> implements CandidateRelationship {

    @Override
    protected CandidateRelationship newRelationship(RelationshipType type, Direction direction, CandidateProperties properties) {
        return new CandidateGeneralizedRelationship(type, direction, properties);
    }

    @Override
    protected CandidateProperties newProperties(PropertyContainer propertyContainer) {
        return new CandidateGeneralizedProperties(propertyContainer);
    }

    @Override
    protected CandidateProperties newProperties(Map<String, String> properties) {
        return new CandidateGeneralizedProperties(properties);
    }

    public CandidateGeneralizedRelationship(Relationship relationship, Node pointOfView) {
        super(relationship, pointOfView);
    }

    public CandidateGeneralizedRelationship(Relationship relationship, Node pointOfView, CandidateProperties properties) {
        super(relationship, pointOfView, properties);
    }

    public CandidateGeneralizedRelationship(RelationshipType type, Direction direction) {
        super(type, direction);
    }

    public CandidateGeneralizedRelationship(RelationshipType type, Direction direction, CandidateProperties properties) {
        super(type, direction, properties);
    }

    public CandidateGeneralizedRelationship(RelationshipType type, Direction direction, Map<String, String> properties) {
        super(type, direction, properties);
    }

    public CandidateGeneralizedRelationship(String string) {
        super(string);
    }

    public CandidateGeneralizedRelationship(ImmutableDirectedRelationship<String, CandidateProperties> relationship) {
        super(relationship);
    }
}
