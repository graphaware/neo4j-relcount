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

import com.graphaware.neo4j.dto.common.relationship.HasTypeAndDirection;
import com.graphaware.neo4j.dto.common.relationship.HasTypeDirectionAndProperties;
import com.graphaware.neo4j.dto.string.property.CopyMakingSerializableProperties;
import com.graphaware.neo4j.dto.string.relationship.BaseCopyMakingSerializableDirectedRelationship;
import com.graphaware.neo4j.dto.string.relationship.CopyMakingSerializableDirectedRelationship;
import com.graphaware.neo4j.relcount.full.dto.property.PartiallyComparableProperties;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import java.util.Map;

/**
 * Abstract base-class for {@link com.graphaware.neo4j.relcount.full.dto.property.PartiallyComparableProperties}, {@link com.graphaware.neo4j.dto.string.property.CopyMakingSerializableProperties} implementations.
 */
public abstract class PartiallyComparableSerializableRelationship<T extends CopyMakingSerializableDirectedRelationship<T, P>, P extends PartiallyComparableProperties & CopyMakingSerializableProperties<P>> extends BaseCopyMakingSerializableDirectedRelationship<P,T> {

    /**
     * {@inheritDoc}
     */
    public boolean isMoreGeneralThan(HasTypeDirectionAndProperties<String, ?> relationship) {
        return matches((HasTypeAndDirection) relationship)
                && getProperties().isMoreGeneralThan(relationship.getProperties());
    }

    /**
     * {@inheritDoc}
     */
    public boolean isStrictlyMoreGeneralThan(HasTypeDirectionAndProperties<String, ?> relationship) {
        return matches((HasTypeAndDirection) relationship)
                && getProperties().isStrictlyMoreGeneralThan(relationship.getProperties());
    }

    /**
     * {@inheritDoc}
     */
    public boolean isMoreSpecificThan(HasTypeDirectionAndProperties<String, ?> relationship) {
        return matches((HasTypeAndDirection) relationship)
                && getProperties().isMoreSpecificThan(relationship.getProperties());
    }

    /**
     * {@inheritDoc}
     */
    public boolean isStrictlyMoreSpecificThan(HasTypeDirectionAndProperties<String, ?> relationship) {
        return matches((HasTypeAndDirection) relationship)
                && getProperties().isStrictlyMoreSpecificThan(relationship.getProperties());
    }

    //constructors

    protected PartiallyComparableSerializableRelationship(Relationship relationship, Node pointOfView) {
        super(relationship, pointOfView);
    }

    protected PartiallyComparableSerializableRelationship(Relationship relationship, Node pointOfView, P properties) {
        super(relationship, pointOfView, properties);
    }

    protected PartiallyComparableSerializableRelationship(RelationshipType type, Direction direction) {
        super(type, direction);
    }

    protected PartiallyComparableSerializableRelationship(RelationshipType type, Direction direction, P properties) {
        super(type, direction, properties);
    }

    protected PartiallyComparableSerializableRelationship(RelationshipType type, Direction direction, Map<String, String> properties) {
        super(type, direction, properties);
    }

    protected PartiallyComparableSerializableRelationship(String string) {
        super(string);
    }

    protected PartiallyComparableSerializableRelationship(HasTypeDirectionAndProperties<String, ?>  relationship) {
        super(relationship);
    }
}
