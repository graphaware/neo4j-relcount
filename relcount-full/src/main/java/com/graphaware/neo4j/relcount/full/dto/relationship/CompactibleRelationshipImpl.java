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

import com.graphaware.neo4j.dto.common.property.ImmutableProperties;
import com.graphaware.neo4j.dto.common.relationship.HasTypeAndDirection;
import com.graphaware.neo4j.dto.common.relationship.HasTypeDirectionAndProperties;
import com.graphaware.neo4j.dto.string.relationship.BaseCopyMakingSerializableDirectedRelationship;
import com.graphaware.neo4j.relcount.full.dto.property.CompactibleProperties;
import com.graphaware.neo4j.relcount.full.dto.property.CompactiblePropertiesImpl;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static com.graphaware.neo4j.framework.config.FrameworkConfiguration.DEFAULT_SEPARATOR;

/**
 * Abstract base-class for {@link CompactibleRelationship} implementations.
 */
public class CompactibleRelationshipImpl extends BaseCopyMakingSerializableDirectedRelationship<CompactibleProperties, CompactibleRelationship> implements CompactibleRelationship {

    /**
     * Construct a description. Please note that using this constructor, the actual properties on the
     * relationship are ignored! The provided properties are used instead. If the start node of this relationship is the same as the end node,
     * the direction will be resolved as {@link org.neo4j.graphdb.Direction#BOTH}.
     *
     * @param relationship Neo4j relationship to describe.
     * @param pointOfView  node which is looking at this relationship and thus determines its direction.
     * @param properties   to use as if they were in the relationship.
     */
    public CompactibleRelationshipImpl(Relationship relationship, Node pointOfView, Map<String, ?> properties) {
        super(relationship, pointOfView, properties);
    }

    /**
     * Construct a description.
     *
     * @param type       type.
     * @param direction  direction.
     * @param properties props.
     */
    public CompactibleRelationshipImpl(RelationshipType type, Direction direction, Map<String, ?> properties) {
        super(type, direction, properties);
    }

    /**
     * Construct a description from a string.
     *
     * @param string    string to construct description from. Must be of the form prefix + type#direction#key1#value1#key2#value2...
     *                  (assuming # separator).
     * @param prefix    of the string that should be removed before conversion.
     * @param separator of information, ideally a single character, must not be null or empty.
     */
    public CompactibleRelationshipImpl(String string, String prefix, String separator) {
        super(string, prefix, separator);
    }

    /**
     * Is this instance more general than (or at least as general as) the given instance?
     *
     * @param relationship to compare.
     * @return true iff this instance is more general than or as general as the provided instance.
     */
    public boolean isMoreGeneralThan(HasTypeDirectionAndProperties<String, ? extends ImmutableProperties<String>> relationship) {
        return matches((HasTypeAndDirection) relationship)
                && getProperties().isMoreGeneralThan(relationship.getProperties());
    }

    /**
     * Is this instance strictly more general than the given instance?
     *
     * @param relationship to compare.
     * @return true iff this instance is strictly more general than the provided instance.
     */
    public boolean isStrictlyMoreGeneralThan(HasTypeDirectionAndProperties<String, ? extends ImmutableProperties<String>> relationship) {
        return matches((HasTypeAndDirection) relationship)
                && getProperties().isStrictlyMoreGeneralThan(relationship.getProperties());
    }

    /**
     * Is this instance more specific than (or at least as specific as) the given instance?
     *
     * @param relationship to compare.
     * @return true iff this instance is more specific than or as specific as the provided instance.
     */
    public boolean isMoreSpecificThan(HasTypeDirectionAndProperties<String, ? extends ImmutableProperties<String>> relationship) {
        return matches((HasTypeAndDirection) relationship)
                && getProperties().isMoreSpecificThan(relationship.getProperties());
    }

    /**
     * Is this instance strictly more specific than the given instance?
     *
     * @param relationship to compare.
     * @return true iff this instance is strictly more specific than the provided instance.
     */
    public boolean isStrictlyMoreSpecificThan(HasTypeDirectionAndProperties<String, ? extends ImmutableProperties<String>> relationship) {
        return matches((HasTypeAndDirection) relationship)
                && getProperties().isStrictlyMoreSpecificThan(relationship.getProperties());
    }

    /**
     * Generate items one step more general than (or as general as) this instance.
     *
     * @return set of one-level more/equally general instances, ordered by decreasing generality.
     */
    public Set<CompactibleRelationship> generateOneMoreGeneral() {
        return withDifferentProperties(getProperties().generateOneMoreGeneral());
    }

    /**
     * Generate all items more general than (or as general as) this instance.
     *
     * @return set of all more/equally general instances, ordered by decreasing generality.
     */
    public Set<CompactibleRelationship> generateAllMoreGeneral() {
        return withDifferentProperties(getProperties().generateAllMoreGeneral());
    }

    private Set<CompactibleRelationship> withDifferentProperties(Set<CompactibleProperties> propertySets) {
        Set<CompactibleRelationship> result = new TreeSet<>();

        for (CompactibleProperties propertySet : propertySets) {
            result.add(newRelationship(getType(), getDirection(), propertySet.getProperties()));
        }

        return result;
    }

    /**
     * @see {@link Comparable#compareTo(Object)}.
     */
    public int compareTo(CompactibleRelationship that) {
        if (equals(that)) {
            return 0;
        } else if (isMoreGeneralThan(that)) {
            return 1;
        } else if (isMoreSpecificThan(that)) {
            return -1;
        }

        //it's OK to use any separator here, as long as it's the same one
        return toString(DEFAULT_SEPARATOR).compareTo(that.toString(DEFAULT_SEPARATOR));
    }

    /**
     * Create a new instance of this relationship representation.
     *
     * @param type       type.
     * @param direction  direction.
     * @param properties props.
     * @return new instance.
     */
    @Override
    protected CompactibleRelationship newRelationship(RelationshipType type, Direction direction, Map<String, ?> properties) {
        return new CompactibleRelationshipImpl(type, direction, properties);
    }

    /**
     * Create properties representation for this relationship a map of properties.
     *
     * @param properties to create a representation from.
     * @return created properties.
     */
    @Override
    protected CompactibleProperties newProperties(Map<String, ?> properties) {
        return new CompactiblePropertiesImpl(properties);
    }

    /**
     * Is this instance mutually exclusive with the given other instance? This method is reflexive.
     *
     * @param other to check mutual exclusivity against.
     * @return true iff this and the other are mutually exclusive.
     */
    @Override
    public boolean isMutuallyExclusive(HasTypeDirectionAndProperties<String, ? extends ImmutableProperties<String>> other) {
        return !matches((HasTypeAndDirection) other) || getProperties().isMutuallyExclusive(other.getProperties());
    }
}
