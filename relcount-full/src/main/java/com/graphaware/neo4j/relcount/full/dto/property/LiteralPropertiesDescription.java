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

import org.neo4j.graphdb.PropertyContainer;

import java.util.*;

/**
 * An extension of {@link GeneralPropertiesDescription} in which a missing property is treated as a concrete value ({@link #UNDEF}) as opposed
 * to "any". This is for situations where a relationship is explicitly created without some property that other relationships
 * of the same type might have. In such case, this relationship should not be treated as more general than the others.
 */
public class LiteralPropertiesDescription extends GeneralPropertiesDescription {

    /**
     * This string will be used as an extra "marker" property to indicate that they are meant literally.
     */
    public static final String LITERAL = "_LITERAL_";

    /**
     * This is the marker's property value.
     */
    public static final String TRUE = "true";

    /**
     * Value of undefined keys. It is not printed anywhere, but it is used internally for comparisons.
     */
    private static final String UNDEF = "_UNDEF_";

    /**
     * Construct a description from a {@link org.neo4j.graphdb.PropertyContainer}.
     *
     * @param propertyContainer to take (copy) properties from.
     */
    public LiteralPropertiesDescription(PropertyContainer propertyContainer) {
        super(propertyContainer);
    }

    /**
     * Construct a description from a {@link java.util.Map}.
     *
     * @param properties to take (copy).
     */
    public LiteralPropertiesDescription(Map<String, ?> properties) {
        super(properties);
    }

    /**
     * Construct a description from a {@link String}.
     *
     * @param string    to construct properties from. Must be of the form key1#value1#key2#value2 (assuming # separator).
     * @param separator of keys and values, ideally a single character, must not be null or empty.
     */
    LiteralPropertiesDescription(String string, String separator) {
        super(string, separator);
    }

    /**
     * {@inheritDoc}
     *
     * @return always true.
     */
    @Override
    public boolean containsKey(String key) {
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @return the actual value or {@link #UNDEF} if the underlying properties do not contain the key.
     */
    @Override
    public String get(String key) {
        String value = super.get(key);
        if (value != null) {
            return value;
        }

        return UNDEF;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> keySet() {
        Set<String> result = new HashSet<>(super.keySet());
        result.add(LITERAL);
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Map.Entry<String, String>> entrySet() {
        Set<Map.Entry<String, String>> result = new HashSet<>(super.entrySet());
        result.add(new AbstractMap.SimpleEntry<>(LITERAL, TRUE));
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<String> values() {
        Collection<String> result = new HashSet<>(super.values());
        result.add(TRUE);
        return result;
    }

    /**
     * Get an immutable {@link java.util.Map} of represented properties.
     *
     * @return read-only {@link java.util.Map} of properties.
     */
    @Override
    public Map<String, String> getProperties() {
        Map<String, String> result = new TreeMap<>(super.getProperties());
        result.put(LITERAL, TRUE);
        return result;
    }

    /**
     * {@inheritDoc}
     *
     * @return a single more general instance, which is {@link GeneralPropertiesDescription} with exactly the same properties.
     */
    @Override
    public Set<PropertiesDescription> generateOneMoreGeneral() {
        return Collections.<PropertiesDescription>singleton(new GeneralPropertiesDescription(super.getProperties()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<PropertiesDescription> generateAllMoreGeneral() {
        Set<PropertiesDescription> result = new TreeSet<>();
        result.add(self());
        result.addAll(super.generateAllMoreGeneral(generateOneMoreGeneral().iterator().next()));
        return result;
    }
}
