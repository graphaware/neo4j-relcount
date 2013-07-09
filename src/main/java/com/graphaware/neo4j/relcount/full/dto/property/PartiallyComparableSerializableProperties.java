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

import com.graphaware.neo4j.dto.common.property.ImmutableProperties;
import com.graphaware.neo4j.dto.string.property.BaseCopyMakingSerializableProperties;
import com.graphaware.neo4j.dto.string.property.CopyMakingSerializableProperties;
import org.neo4j.graphdb.PropertyContainer;

import java.util.Map;

/**
 * Abstract base-class for {@link PartiallyComparableProperties}, {@link CopyMakingSerializableProperties} implementations.
 */
public abstract class PartiallyComparableSerializableProperties<T extends CopyMakingSerializableProperties<T>> extends BaseCopyMakingSerializableProperties<T> {

    /**
     * Construct a representation of properties from a {@link org.neo4j.graphdb.PropertyContainer}.
     *
     * @param propertyContainer to take (copy) properties from.
     */
    protected PartiallyComparableSerializableProperties(PropertyContainer propertyContainer) {
        super(propertyContainer);
    }

    /**
     * Construct a representation of properties from a {@link java.util.Map}.
     *
     * @param properties to take (copy).
     */
    protected PartiallyComparableSerializableProperties(Map<String, String> properties) {
        super(properties);
    }

    /**
     * Construct a representation of properties from a {@link String}.
     *
     * @param string to construct properties from. Must be of the form key1#value1#key2#value2... (assuming the default
     *               {@link com.graphaware.neo4j.common.Constants#SEPARATOR}.
     */
    protected PartiallyComparableSerializableProperties(String string) {
        super(string);
    }

    /**
     * Is this instance more general than (or at least as general as) the given instance?
     *
     * @param properties to compare.
     * @return true iff this instance is more general than or as general as the provided instance.
     */
    public boolean isMoreGeneralThan(ImmutableProperties<String> properties) {
        for (String thisKey : keySet()) {
            if (!properties.containsKey(thisKey)) {
                return false;
            }
            if (!get(thisKey).equals(properties.get(thisKey))) {
                return false;
            }
        }

        for (String thatKey : properties.keySet()) {
            if (containsKey(thatKey) && !get(thatKey).equals(properties.get(thatKey))) {
                return false;
            }
        }

        return true;
    }

    /**
     * Is this instance strictly more general than the given instance?
     *
     * @param properties to compare.
     * @return true iff this instance is strictly more general than the provided instance.
     */
    public boolean isStrictlyMoreGeneralThan(ImmutableProperties<String> properties) {
        return isMoreGeneralThan(properties) && !isMoreSpecificThan(properties);
    }

    /**
     * Is this instance more specific than (or at least as specific as) the given instance?
     *
     * @param properties to compare.
     * @return true iff this instance is more specific than or as specific as the provided instance.
     */
    public boolean isMoreSpecificThan(ImmutableProperties<String> properties) {
        for (String thatKey : properties.keySet()) {
            if (!containsKey(thatKey)) {
                return false;
            }
            if (!get(thatKey).equals(properties.get(thatKey))) {
                return false;
            }
        }

        for (String thisKey : keySet()) {
            if (properties.containsKey(thisKey) && !properties.get(thisKey).equals(get(thisKey))) {
                return false;
            }
        }

        return true;
    }

    /**
     * Is this instance strictly more specific than the given instance?
     *
     * @param properties to compare.
     * @return true iff this instance is strictly more specific than the provided instance.
     */
    public boolean isStrictlyMoreSpecificThan(ImmutableProperties<String> properties) {
        return isMoreSpecificThan(properties) && !isMoreGeneralThan(properties);
    }
}
