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
import com.graphaware.neo4j.dto.string.property.CopyMakingSerializableProperties;
import com.graphaware.neo4j.dto.string.relationship.CopyMakingSerializableDirectedRelationship;
import com.graphaware.neo4j.relcount.full.dto.property.TotallyComparableProperties;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import java.util.Map;

/**
 *
 */
public abstract class TotallyComparableSerializableRelationship<R extends CopyMakingSerializableDirectedRelationship<R, P>, P extends TotallyComparableProperties & CopyMakingSerializableProperties<P>> extends PartiallyComparableSerializableRelationship<R,P> implements TotallyComparableRelationship<P> {

    /**
     * {@inheritDoc}
     */
    public int compareTo(TotallyComparableRelationship<TotallyComparableProperties> that) {
        if (equals(that)) {
            return 0;
        } else if (isMoreGeneralThan(that)) {
            return 1;
        } else if (isMoreSpecificThan(that)) {
            return -1;
        }

        return toString().compareTo(that.toString());
    }

    protected TotallyComparableSerializableRelationship(Relationship relationship, Node pointOfView) {
        super(relationship, pointOfView);
    }

    protected TotallyComparableSerializableRelationship(Relationship relationship, Node pointOfView, P properties) {
        super(relationship, pointOfView, properties);
    }

    protected TotallyComparableSerializableRelationship(RelationshipType type, Direction direction) {
        super(type, direction);
    }

    protected TotallyComparableSerializableRelationship(RelationshipType type, Direction direction, P properties) {
        super(type, direction, properties);
    }

    protected TotallyComparableSerializableRelationship(RelationshipType type, Direction direction, Map<String, String> properties) {
        super(type, direction, properties);
    }

    protected TotallyComparableSerializableRelationship(String string) {
        super(string);
    }

    protected TotallyComparableSerializableRelationship(HasTypeDirectionAndProperties<String, ?> relationship) {
        super(relationship);
    }
}
