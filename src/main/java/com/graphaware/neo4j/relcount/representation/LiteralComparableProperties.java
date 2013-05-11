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

import org.neo4j.graphdb.PropertyContainer;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * An extension of {@link ComparableProperties} in which a missing property is treated as a concrete value ({@link #UNDEF}) as opposed
 * to "any". This is for situations where a relationship is explicitly created without some property that other relationships
 * of the same type might have. In such case, this relationship should not be treated as more general than the others.
 */
public class LiteralComparableProperties extends ComparableProperties {

    /**
     * This string will be inserted before the String representation of these properties to indicate that they are meant literally.
     */
    public static final String LITERAL = "_LITERAL_";

    /**
     * Value of undefined keys. It is not printed anywhere, but it is used internally for comparisons.
     */
    private static final String UNDEF = "_UNDEF_";

    /**
     * Construct a representation of properties from a {@link org.neo4j.graphdb.PropertyContainer}.
     *
     * @param propertyContainer to take (copy) properties from.
     */
    public LiteralComparableProperties(PropertyContainer propertyContainer) {
        super(propertyContainer);
    }

    /**
     * Construct a representation of properties from a {@link java.util.Map}.
     *
     * @param properties to take (copy).
     */
    public LiteralComparableProperties(Map<String, String> properties) {
        super(properties);
    }

    /**
     * Construct a representation of properties from a {@link String}.
     *
     * @param string to construct properties from. Must be of the form _LITERAL_key1#value1#key2#value2 (assuming the default
     *               {@link com.graphaware.neo4j.utils.Constants#SEPARATOR} and {@link #LITERAL}).
     */
    public LiteralComparableProperties(String string) {
        super(string.substring(LITERAL.length()));
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
     *
     * @return a single more general instance, which is {@link ComparableProperties} with exactly the same properties.
     */
    @Override
    public Set<ComparableProperties> generateOneMoreGeneral() {
        return Collections.singleton(new ComparableProperties(getProperties()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<ComparableProperties> generateAllMoreGeneral() {
        Set<ComparableProperties> result = new TreeSet<ComparableProperties>();
        result.add(this);
        result.addAll(super.generateAllMoreGeneral(generateOneMoreGeneral().iterator().next()));
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return LITERAL + super.toString();
    }
}
