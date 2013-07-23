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
 * Base-class for {@link CompactibleRelationship} implementations.
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
     * {@inheritDoc}
     */
    @Override
    public boolean isMoreGeneralThan(HasTypeDirectionAndProperties<String, ? extends ImmutableProperties<String>> relationship) {
        return matches((HasTypeAndDirection) relationship) && getProperties().isMoreGeneralThan(relationship.getProperties());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isStrictlyMoreGeneralThan(HasTypeDirectionAndProperties<String, ? extends ImmutableProperties<String>> relationship) {
        return matches((HasTypeAndDirection) relationship) && getProperties().isStrictlyMoreGeneralThan(relationship.getProperties());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isMoreSpecificThan(HasTypeDirectionAndProperties<String, ? extends ImmutableProperties<String>> relationship) {
        return matches((HasTypeAndDirection) relationship) && getProperties().isMoreSpecificThan(relationship.getProperties());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isStrictlyMoreSpecificThan(HasTypeDirectionAndProperties<String, ? extends ImmutableProperties<String>> relationship) {
        return matches((HasTypeAndDirection) relationship) && getProperties().isStrictlyMoreSpecificThan(relationship.getProperties());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<CompactibleRelationship> generateOneMoreGeneral() {
        return withDifferentProperties(getProperties().generateOneMoreGeneral());
    }

    /**
     * {@inheritDoc}
     */
    @Override
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
     * {@inheritDoc}
     */
    @Override
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
     * {@inheritDoc}
     */
    @Override
    public boolean isMutuallyExclusive(HasTypeDirectionAndProperties<String, ? extends ImmutableProperties<String>> other) {
        return !matches((HasTypeAndDirection) other) || getProperties().isMutuallyExclusive(other.getProperties());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected CompactibleRelationship newRelationship(RelationshipType type, Direction direction, Map<String, ?> properties) {
        return new CompactibleRelationshipImpl(type, direction, properties);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected CompactibleProperties newProperties(Map<String, ?> properties) {
        return new CompactiblePropertiesImpl(properties);
    }
}
