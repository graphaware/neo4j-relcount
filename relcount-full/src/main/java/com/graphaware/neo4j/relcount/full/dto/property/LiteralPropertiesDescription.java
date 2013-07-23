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

package com.graphaware.neo4j.relcount.full.dto.property;

import com.graphaware.neo4j.dto.string.property.SerializablePropertiesImpl;

import java.util.Map;

/**
 *  {@link PropertiesDescription} where a missing property means "undefined".
 */
public class LiteralPropertiesDescription extends SerializablePropertiesImpl implements PropertiesDescription {

    /**
     * Construct a representation of properties from a {@link java.util.Map}.
     *
     * @param properties to take (copy).
     */
    public LiteralPropertiesDescription(Map<String, ?> properties) {
        super(properties);
    }

    /**
     * Construct a representation of properties from a {@link String}.
     *
     * @param string    to construct properties from. Must be of the form key1#value1#key2#value2... (assuming # separator).
     * @param separator of keys and values, ideally a single character, must not be null or empty.
     */
    LiteralPropertiesDescription(String string, String separator) {
        super(string, separator);
    }
}
