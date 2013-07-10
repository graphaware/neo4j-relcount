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
import com.graphaware.neo4j.dto.string.relationship.BaseCopyMakingSerializableDirectedRelationship;
import com.graphaware.neo4j.dto.string.relationship.CopyMakingSerializableDirectedRelationship;
import com.graphaware.neo4j.relcount.full.dto.property.GeneralizingProperties;
import com.graphaware.neo4j.relcount.full.dto.property.TotallyComparableProperties;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 *
 */
public abstract class BaseCandidateRelationship<R extends CopyMakingSerializableDirectedRelationship<R, P>, P extends GeneralizingProperties<P> & TotallyComparableProperties> extends BaseCopyMakingSerializableDirectedRelationship<P,R> {

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

    /**
     * {@inheritDoc}
     */
    public Set<R> generateOneMoreGeneral() {
        return withDifferentProperties(getProperties().generateOneMoreGeneral());
    }

    /**
     * {@inheritDoc}
     */
    public Set<R> generateAllMoreGeneral() {
        return withDifferentProperties(getProperties().generateAllMoreGeneral());
    }

    private Set<R> withDifferentProperties(Set<P> propertySets) {
        Set<R> result = new TreeSet<>();

        for (P propertySet : propertySets) {
            result.add(newRelationship(getType(), getDirection(), propertySet));
        }

        return result;
    }

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

    protected BaseCandidateRelationship(Relationship relationship, Node pointOfView) {
        super(relationship, pointOfView);
    }

    protected BaseCandidateRelationship(Relationship relationship, Node pointOfView, P properties) {
        super(relationship, pointOfView, properties);
    }

    protected BaseCandidateRelationship(RelationshipType type, Direction direction) {
        super(type, direction);
    }

    protected BaseCandidateRelationship(RelationshipType type, Direction direction, P properties) {
        super(type, direction, properties);
    }

    protected BaseCandidateRelationship(RelationshipType type, Direction direction, Map<String, String> properties) {
        super(type, direction, properties);
    }

    protected BaseCandidateRelationship(String string) {
        super(string);
    }

    protected BaseCandidateRelationship(HasTypeDirectionAndProperties<String, ?> relationship) {
        super(relationship);
    }
}
