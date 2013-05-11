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

package com.graphaware.neo4j.relcount.representation;

import com.graphaware.neo4j.representation.property.MakesCopyWithProperty;
import com.graphaware.neo4j.representation.property.MakesCopyWithoutProperty;
import com.graphaware.neo4j.representation.relationship.CopyMakingRelationship;
import com.graphaware.neo4j.representation.relationship.Relationship;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.RelationshipType;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static com.graphaware.neo4j.relcount.representation.LiteralComparableProperties.LITERAL;

/**
 * A {@link com.graphaware.neo4j.representation.relationship.Relationship} {@link PartiallyComparableByGenerality}.
 * <p/>
 * {@link com.graphaware.neo4j.representation.relationship.Relationship} X is more general than Y iff they are both of the same type and direction
 * and the properties of X are more general than properties of Y.
 * {@link com.graphaware.neo4j.representation.relationship.Relationship} X is more general than Y iff they are both of the same type and direction
 * and the properties of X are more specific than properties of Y.
 */
public class ComparableRelationship extends CopyMakingRelationship<ComparableProperties, ComparableRelationship> implements
        Relationship<ComparableProperties>,
        MakesCopyWithProperty<ComparableRelationship>,
        MakesCopyWithoutProperty<ComparableRelationship>,
        TotallyComparableByGenerality<Relationship, ComparableRelationship>,
        GeneratesMoreGeneral<ComparableRelationship> {

    /**
     * Construct a relationship representation.
     *
     * @param relationship Neo4j relationship to represent.
     * @param pointOfView  node which is looking at this relationship and thus determines its direction.
     */
    public ComparableRelationship(org.neo4j.graphdb.Relationship relationship, Node pointOfView) {
        super(relationship, pointOfView);
    }

    /**
     * Construct a relationship representation. Please note that using this constructor, the actual properties on the
     * relationship are ignored! The provided properties are used instead.
     *
     * @param relationship Neo4j relationship to represent.
     * @param pointOfView  node which is looking at this relationship and thus determines its direction.
     * @param properties   to use as if they were in the relationship.
     */
    public ComparableRelationship(org.neo4j.graphdb.Relationship relationship, Node pointOfView, ComparableProperties properties) {
        super(relationship, pointOfView, properties);
    }

    /**
     * Construct a relationship representation with no properties.
     *
     * @param type      type.
     * @param direction direction.
     */
    public ComparableRelationship(RelationshipType type, Direction direction) {
        super(type, direction);
    }

    /**
     * Construct a relationship representation.
     *
     * @param type       type.
     * @param direction  direction.
     * @param properties props.
     */
    public ComparableRelationship(RelationshipType type, Direction direction, ComparableProperties properties) {
        super(type, direction, properties);
    }

    /**
     * Construct a relationship representation.
     *
     * @param type       type.
     * @param direction  direction.
     * @param properties props.
     */
    public ComparableRelationship(RelationshipType type, Direction direction, Map<String, String> properties) {
        super(type, direction, properties);
    }

    /**
     * Construct a relationship representation from a string.
     *
     * @param string string to construct relationship from. Must be of the form type#direction#key1#value1#key2#value2...
     *               (assuming the default {@link com.graphaware.neo4j.utils.Constants#SEPARATOR}.
     */
    public ComparableRelationship(String string) {
        super(string);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ComparableRelationship newRelationship(RelationshipType type, Direction direction, ComparableProperties properties) {
        return new ComparableRelationship(type, direction, properties);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ComparableProperties newProperties(String string) {
        if (string.startsWith(LITERAL)) {
            return new LiteralComparableProperties(string);
        }
        return new ComparableProperties(string);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ComparableProperties newProperties(PropertyContainer propertyContainer) {
        return new ComparableProperties(propertyContainer);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ComparableProperties newProperties(Map<String, String> properties) {
        return new ComparableProperties(properties);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(ComparableRelationship that) {
        if (equals(that)) {
            return 0;
        } else if (isMoreGeneralThan(that)) {
            return 1;
        } else if (isMoreSpecificThan(that)) {
            return -1;
        }

        return toString().compareTo(that.toString());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isMoreGeneralThan(Relationship relationship) {
        return typeAndDirectionSameAs(relationship)
                && getProperties().isMoreGeneralThan(relationship.getProperties());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isStrictlyMoreGeneralThan(Relationship relationship) {
        return typeAndDirectionSameAs(relationship)
                && getProperties().isStrictlyMoreGeneralThan(relationship.getProperties());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isMoreSpecificThan(Relationship relationship) {
        return typeAndDirectionSameAs(relationship)
                && getProperties().isMoreSpecificThan(relationship.getProperties());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isStrictlyMoreSpecificThan(Relationship relationship) {
        return typeAndDirectionSameAs(relationship)
                && getProperties().isStrictlyMoreSpecificThan(relationship.getProperties());
    }

    /**
     * Check whether the type and direction of this relationships are the same as the ones of the given relationship.
     *
     * @param relationship to check.
     * @return true iff both type and direction are the same.
     */
    public boolean typeAndDirectionSameAs(Relationship relationship) {
        return getType().name().equals(relationship.getType().name())
                && getDirection().equals(relationship.getDirection());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<ComparableRelationship> generateOneMoreGeneral() {
        return withDifferentProperties(getProperties().generateOneMoreGeneral());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<ComparableRelationship> generateAllMoreGeneral() {
        return withDifferentProperties(getProperties().generateAllMoreGeneral());
    }

    private Set<ComparableRelationship> withDifferentProperties(Set<ComparableProperties> propertySets) {
        Set<ComparableRelationship> result = new TreeSet<ComparableRelationship>();

        for (ComparableProperties propertySet : propertySets) {
            result.add(new ComparableRelationship(getType(), getDirection(), propertySet));
        }

        return result;
    }
}
