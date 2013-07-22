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

import com.graphaware.neo4j.dto.string.property.BaseCopyMakingSerializableProperties;
import org.neo4j.graphdb.PropertyContainer;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static com.graphaware.neo4j.framework.config.FrameworkConfiguration.DEFAULT_SEPARATOR;

/**
 * Abstract base-class for {@link PropertiesDescription} implementations.
 */
public abstract class BasePropertiesDescription extends BaseCopyMakingSerializableProperties<PropertiesDescription> {

    /**
     * Construct a description from a {@link org.neo4j.graphdb.PropertyContainer}.
     *
     * @param propertyContainer to take (copy) properties from.
     */
    protected BasePropertiesDescription(PropertyContainer propertyContainer) {
        super(propertyContainer);
    }

    /**
     * Construct a description from a {@link java.util.Map}.
     *
     * @param properties to take (copy).
     */
    protected BasePropertiesDescription(Map<String, ?> properties) {
        super(properties);
    }

    /**
     * Construct a description from a {@link String}.
     *
     * @param string    to construct properties from. Must be of the form key1#value1#key2#value2... (assuming # separator).
     * @param separator of keys and values, ideally a single character, must not be null or empty.
     */
    protected BasePropertiesDescription(String string, String separator) {
        super(string, separator);
    }

    /**
     * Is this instance more general than (or at least as general as) the given instance?
     *
     * @param properties to compare.
     * @return true iff this instance is more general than or as general as the provided instance.
     */
    public boolean isMoreGeneralThan(PropertiesDescription properties) {
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
    public boolean isStrictlyMoreGeneralThan(PropertiesDescription properties) {
        return isMoreGeneralThan(properties) && !isMoreSpecificThan(properties);
    }

    /**
     * Is this instance more specific than (or at least as specific as) the given instance?
     *
     * @param properties to compare.
     * @return true iff this instance is more specific than or as specific as the provided instance.
     */
    public boolean isMoreSpecificThan(PropertiesDescription properties) {
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
    public boolean isStrictlyMoreSpecificThan(PropertiesDescription properties) {
        return isMoreSpecificThan(properties) && !isMoreGeneralThan(properties);
    }

    /**
     * @see {@link Comparable#compareTo(Object)}.
     */
    public int compareTo(PropertiesDescription that) {
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
     * Generate items one step more general than (or as general as) this instance.
     *
     * @return set of one-level more/equally general instances, ordered by decreasing generality.
     */
    public Set<PropertiesDescription> generateOneMoreGeneral() {
        Set<PropertiesDescription> result = new TreeSet<>();
        result.add(self());
        for (String key : keySet()) {
            result.add(without(key));
        }
        return result;
    }

    /**
     * Generate all items more general than (or as general as) this instance.
     *
     * @return set of all more/equally general instances, ordered by decreasing generality.
     */
    public Set<PropertiesDescription> generateAllMoreGeneral() {
        return generateAllMoreGeneral(self());
    }

    protected Set<PropertiesDescription> generateAllMoreGeneral(PropertiesDescription propertiesRepresentation) {
        //base case
        if (propertiesRepresentation.isEmpty()) {
            return Collections.singleton(propertiesRepresentation);
        }

        //recursion
        Set<PropertiesDescription> result = new TreeSet<>();
        Map.Entry<String, String> next = propertiesRepresentation.entrySet().iterator().next();
        for (PropertiesDescription properties : generateAllMoreGeneral(propertiesRepresentation.without(next.getKey()))) {
            result.add(properties);
            result.add(properties.with(next.getKey(), next.getValue()));
        }

        return result;
    }

    /**
     * @return this.
     */
    protected abstract PropertiesDescription self();
}