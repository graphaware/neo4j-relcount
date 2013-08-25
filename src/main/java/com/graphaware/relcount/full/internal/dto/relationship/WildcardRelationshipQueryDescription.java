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

package com.graphaware.relcount.full.internal.dto.relationship;

import com.graphaware.propertycontainer.dto.common.relationship.HasTypeDirectionAndProperties;
import com.graphaware.propertycontainer.dto.string.relationship.BaseSerializableDirectedRelationship;
import com.graphaware.relcount.full.internal.dto.property.PropertiesQueryDescription;
import com.graphaware.relcount.full.internal.dto.property.WildcardPropertiesQueryDescription;

import java.util.Map;

/**
 * A {@link RelationshipQueryDescription} in which a missing property is treated as a wildcard ("any") as opposed
 * to "undefined".
 */
public class WildcardRelationshipQueryDescription extends BaseSerializableDirectedRelationship<PropertiesQueryDescription> implements RelationshipQueryDescription {

    /**
     * Construct a description from a string.
     *
     * @param string    string to construct description from. Must be of the form prefix + type#direction#key1#value1#key2#value2
     *                  (assuming # separator).
     * @param prefix    of the string that should be removed before conversion.
     * @param separator of information, ideally a single character, must not be null or empty.
     */
    public WildcardRelationshipQueryDescription(String string, String prefix, String separator) {
        super(string, prefix, separator);
    }

    /**
     * Construct a description from another one.
     *
     * @param relationship relationships representation.
     */
    public WildcardRelationshipQueryDescription(HasTypeDirectionAndProperties<String, ?> relationship) {
        super(relationship);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PropertiesQueryDescription newProperties(Map<String, ?> properties) {
        return new WildcardPropertiesQueryDescription(properties);
    }
}