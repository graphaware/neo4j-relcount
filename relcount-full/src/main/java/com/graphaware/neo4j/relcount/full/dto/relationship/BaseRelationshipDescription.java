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
import com.graphaware.neo4j.relcount.full.dto.property.PropertiesDescription;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Abstract base-class for {@link RelationshipDescription} implementations.
 */
public abstract class BaseRelationshipDescription extends BaseCopyMakingSerializableDirectedRelationship<PropertiesDescription, RelationshipDescription> {

    /**
     * Construct a description. If the start node of this relationship is the same as the end node,
     * the direction will be resolved as {@link org.neo4j.graphdb.Direction#BOTH}.
     *
     * @param relationship Neo4j relationship to describe.
     * @param pointOfView  node which is looking at this relationship and thus determines its direction.
     */
    protected BaseRelationshipDescription(Relationship relationship, Node pointOfView) {
        super(relationship, pointOfView);
    }

    /**
     * Construct a description. Please note that using this constructor, the actual properties on the
     * relationship are ignored! The provided properties are used instead. If the start node of this relationship is the same as the end node,
     * the direction will be resolved as {@link org.neo4j.graphdb.Direction#BOTH}.
     *
     * @param relationship Neo4j relationship to describe.
     * @param pointOfView  node which is looking at this relationship and thus determines its direction.
     * @param properties   to use as if they were in the relationship.
     */
    protected BaseRelationshipDescription(Relationship relationship, Node pointOfView, Map<String, ?> properties) {
        super(relationship, pointOfView, properties);
    }

    /**
     * Construct a description.
     *
     * @param type       type.
     * @param direction  direction.
     * @param properties props.
     */
    protected BaseRelationshipDescription(RelationshipType type, Direction direction, Map<String, ?> properties) {
        super(type, direction, properties);
    }

    /**
     * Construct a description from a string.
     *
     * @param string string to construct description from. Must be of the form prefix + type#direction#key1#value1#key2#value2...
     *               (assuming the default {@link com.graphaware.neo4j.common.Constants#SEPARATOR}.
     * @param prefix of the string that should be removed before conversion.
     */
    protected BaseRelationshipDescription(String string, String prefix) {
        super(string, prefix);
    }

    /**
     * Construct a description from another one.
     *
     * @param relationship relationships representation.
     */
    protected BaseRelationshipDescription(HasTypeDirectionAndProperties<String, ?> relationship) {
        super(relationship);
    }

    /**
     * Is this instance more general than (or at least as general as) the given instance?
     *
     * @param relationship to compare.
     * @return true iff this instance is more general than or as general as the provided instance.
     */
    public boolean isMoreGeneralThan(RelationshipDescription relationship) {
        return matches((HasTypeAndDirection) relationship)
                && getProperties().isMoreGeneralThan(relationship.getProperties());
    }

    /**
     * Is this instance strictly more general than the given instance?
     *
     * @param relationship to compare.
     * @return true iff this instance is strictly more general than the provided instance.
     */
    public boolean isStrictlyMoreGeneralThan(RelationshipDescription relationship) {
        return matches((HasTypeAndDirection) relationship)
                && getProperties().isStrictlyMoreGeneralThan(relationship.getProperties());
    }

    /**
     * Is this instance more specific than (or at least as specific as) the given instance?
     *
     * @param relationship to compare.
     * @return true iff this instance is more specific than or as specific as the provided instance.
     */
    public boolean isMoreSpecificThan(RelationshipDescription relationship) {
        return matches((HasTypeAndDirection) relationship)
                && getProperties().isMoreSpecificThan(relationship.getProperties());
    }

    /**
     * Is this instance strictly more specific than the given instance?
     *
     * @param relationship to compare.
     * @return true iff this instance is strictly more specific than the provided instance.
     */
    public boolean isStrictlyMoreSpecificThan(RelationshipDescription relationship) {
        return matches((HasTypeAndDirection) relationship)
                && getProperties().isStrictlyMoreSpecificThan(relationship.getProperties());
    }

    /**
     * Generate items one step more general than (or as general as) this instance.
     *
     * @return set of one-level more/equally general instances, ordered by decreasing generality.
     */
    public Set<RelationshipDescription> generateOneMoreGeneral() {
        return withDifferentProperties(getProperties().generateOneMoreGeneral());
    }

    /**
     * Generate all items more general than (or as general as) this instance.
     *
     * @return set of all more/equally general instances, ordered by decreasing generality.
     */
    public Set<RelationshipDescription> generateAllMoreGeneral() {
        return withDifferentProperties(getProperties().generateAllMoreGeneral());
    }

    private Set<RelationshipDescription> withDifferentProperties(Set<PropertiesDescription> propertySets) {
        Set<RelationshipDescription> result = new TreeSet<>();

        for (PropertiesDescription propertySet : propertySets) {
            result.add(newRelationship(getType(), getDirection(), propertySet.getProperties()));
        }

        return result;
    }

    /**
     * @see {@link Comparable#compareTo(Object)}.
     */
    public int compareTo(RelationshipDescription that) {
        if (equals(that)) {
            return 0;
        } else if (isMoreGeneralThan(that)) {
            return 1;
        } else if (isMoreSpecificThan(that)) {
            return -1;
        }

        return toString().compareTo(that.toString());
    }
}
