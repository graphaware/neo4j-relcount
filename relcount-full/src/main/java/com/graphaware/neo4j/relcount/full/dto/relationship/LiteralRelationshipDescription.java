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
import com.graphaware.neo4j.dto.string.relationship.BaseCopyMakingSerializableDirectedRelationship;
import com.graphaware.neo4j.relcount.full.dto.property.LiteralPropertiesDescription;
import com.graphaware.neo4j.relcount.full.dto.property.PropertiesDescription;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.RelationshipType;

import java.util.Map;

/**
 * A {@link RelationshipDescription} in which a missing property is treated as a concrete value (undefined) as opposed
 * to "any".
 */
public class LiteralRelationshipDescription extends BaseCopyMakingSerializableDirectedRelationship<PropertiesDescription, RelationshipDescription> implements RelationshipDescription {

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
     * @param string    string to construct description from. Must be of the form prefix + type#direction#key1#value1#key2#value2
     *                  (assuming # separator).
     * @param prefix    of the string that should be removed before conversion.
     * @param separator of information, ideally a single character, must not be null or empty.
     */
    public LiteralRelationshipDescription(String string, String prefix, String separator) {
        super(string, prefix, separator);
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
    protected RelationshipDescription newRelationship(RelationshipType type, Direction direction, Map<String, ?> properties) {
        return new LiteralRelationshipDescription(type, direction, properties);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PropertiesDescription newProperties(Map<String, ?> properties) {
        return new LiteralPropertiesDescription(properties);
    }
}
