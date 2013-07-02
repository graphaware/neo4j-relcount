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

package com.graphaware.neo4j.relcount.dto;

import com.graphaware.neo4j.dto.property.immutable.AbstractCopyMakingSerializableProperties;
import com.graphaware.neo4j.dto.property.immutable.Properties;
import com.graphaware.neo4j.dto.property.immutable.WithPropertyCopyMakingProperties;
import com.graphaware.neo4j.dto.property.immutable.WithoutPropertyCopyMakingProperties;
import org.neo4j.graphdb.PropertyContainer;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * {@link Properties} {@link PartiallyComparableByGenerality}.
 * <p/>
 * {@link Properties} X is more general than Y iff Y contains at least X's properties and they have identical values.
 * {@link Properties} X is more specific than Y iff X contains at least Y's properties and they have identical values.
 * <p/>
 * This class is also capable of generating all more general representations than itself.
 */
public class ComparableProperties extends AbstractCopyMakingSerializableProperties<ComparableProperties> implements
        Properties,
        WithPropertyCopyMakingProperties<ComparableProperties>,
        WithoutPropertyCopyMakingProperties<ComparableProperties>,
        TotallyComparableByGenerality<Properties, ComparableProperties>,
        GeneratesMoreGeneral<ComparableProperties> {

    /**
     * Construct a representation of properties from a {@link org.neo4j.graphdb.PropertyContainer}.
     *
     * @param propertyContainer to take (copy) properties from.
     */
    public ComparableProperties(PropertyContainer propertyContainer) {
        super(propertyContainer);
    }

    /**
     * Construct a representation of properties from a {@link java.util.Map}.
     *
     * @param properties to take (copy).
     */
    public ComparableProperties(Map<String, String> properties) {
        super(properties);
    }

    /**
     * Construct a representation of properties from a {@link String}.
     *
     * @param string to construct properties from. Must be of the form key1#value1#key2#value2 (assuming the default
     *               {@link com.graphaware.neo4j.common.Constants#SEPARATOR}.
     */
    public ComparableProperties(String string) {
        super(string);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ComparableProperties newInstance(Map<String, String> props) {
        return new ComparableProperties(props);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(ComparableProperties that) {
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
    public boolean isMoreGeneralThan(Properties properties) {
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
     * {@inheritDoc}
     */
    @Override
    public boolean isStrictlyMoreGeneralThan(Properties properties) {
        return isMoreGeneralThan(properties) && !isMoreSpecificThan(properties);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isMoreSpecificThan(Properties properties) {
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
     * {@inheritDoc}
     */
    @Override
    public boolean isStrictlyMoreSpecificThan(Properties properties) {
        return isMoreSpecificThan(properties) && !isMoreGeneralThan(properties);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<ComparableProperties> generateOneMoreGeneral() {
        Set<ComparableProperties> result = new TreeSet<ComparableProperties>();
        result.add(this);
        for (String key : keySet()) {
            result.add(without(key));
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<ComparableProperties> generateAllMoreGeneral() {
        return generateAllMoreGeneral(this);
    }

    protected Set<ComparableProperties> generateAllMoreGeneral(ComparableProperties propertiesRepresentation) {
        //base case
        if (propertiesRepresentation.isEmpty()) {
            return Collections.singleton(propertiesRepresentation);
        }

        //recursion
        Set<ComparableProperties> result = new TreeSet<ComparableProperties>();
        Map.Entry<String, String> next = propertiesRepresentation.entrySet().iterator().next();
        for (ComparableProperties properties : generateAllMoreGeneral(propertiesRepresentation.without(next.getKey()))) {
            result.add(properties);
            result.add(properties.with(next.getKey(), next.getValue()));
        }

        return result;
    }
}
