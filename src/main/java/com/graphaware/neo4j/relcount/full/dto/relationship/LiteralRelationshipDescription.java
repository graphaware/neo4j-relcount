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
import com.graphaware.neo4j.relcount.full.dto.property.LiteralPropertiesDescription;
import com.graphaware.neo4j.relcount.full.dto.property.PropertiesDescription;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import java.util.*;

/**
 * A {@link RelationshipDescription} in which a missing property is treated as a concrete value (undefined) as opposed
 * to "any". This is for situations where a relationship is explicitly created without some property that other relationships
 * of the same type might have. In such case, this relationship should not be treated as more general than the others.
 */
public class LiteralRelationshipDescription extends BaseRelationshipDescription implements RelationshipDescription {

    /**
     * Construct a description. If the start node of this relationship is the same as the end node,
     * the direction will be resolved as {@link org.neo4j.graphdb.Direction#BOTH}.
     *
     * @param relationship Neo4j relationship to describe.
     * @param pointOfView  node which is looking at this relationship and thus determines its direction.
     */
    public LiteralRelationshipDescription(Relationship relationship, Node pointOfView) {
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
    public LiteralRelationshipDescription(Relationship relationship, Node pointOfView, Map<String, ?> properties) {
        super(relationship, pointOfView, properties);
    }

    /**
     * Construct a description.
     *
     * @param type       type.
     * @param direction  direction.
     * @param properties props.
     */
    public LiteralRelationshipDescription(RelationshipType type, Direction direction, Map<String, ?> properties) {
        super(type, direction, properties);
    }

    /**
     * Construct a description from a string.
     *
     * @param string string to construct description from. Must be of the form type#direction#_LITERAL_#anything#key1#value1#key2#value2
     *               (assuming the default {@link com.graphaware.neo4j.common.Constants#SEPARATOR} and {@link LiteralPropertiesDescription#LITERAL}),
     *               in which case the _LITERAL_ property will be removed upon construction. Can also be of the form
     *               type#direction#key1#value1#key2#value2, in which case it will be constructed with those properties.
     */
    public LiteralRelationshipDescription(String string) {
        super(string);
    }

    /**
     * Construct a description from another one.
     *
     * @param relationship relationships representation.
     */
    public LiteralRelationshipDescription(HasTypeDirectionAndProperties<String, ?> relationship) {
        super(relationship);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<RelationshipDescription> generateOneMoreGeneral() {
        return Collections.<RelationshipDescription>singleton(new GeneralRelationshipDescription(this));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<RelationshipDescription> generateAllMoreGeneral() {
        Set<RelationshipDescription> result = new TreeSet<>();
        result.add(this);
        result.addAll(new GeneralRelationshipDescription(this).generateAllMoreGeneral());
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PropertiesDescription newProperties(Map<String, ?> properties) {
        Map<String, ?> withoutLiteral = new HashMap<>(properties);
        withoutLiteral.remove(LiteralPropertiesDescription.LITERAL);
        return new LiteralPropertiesDescription(withoutLiteral);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected RelationshipDescription newRelationship(RelationshipType type, Direction direction, Map<String, ?> properties) {
        return new LiteralRelationshipDescription(type, direction, properties);
    }
}
