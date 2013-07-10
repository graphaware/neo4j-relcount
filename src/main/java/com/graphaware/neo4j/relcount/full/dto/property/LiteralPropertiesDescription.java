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

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static com.graphaware.neo4j.common.Constants.SEPARATOR;

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
     * @param string to construct properties from. Must be of the form _LITERAL_#anything#key1#value1#key2#value2
     *               (assuming the default {@link com.graphaware.neo4j.common.Constants#SEPARATOR} and {@link #LITERAL}),
     *               in which case the _LITERAL_ property will be removed upon construction. Can also be of the form
     *               key1#value1#key2#value2, in which case it will be constructed with those properties.
     */
    public LiteralPropertiesDescription(String string) {
        super(string);
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
     * @return a single more general instance, which is {@link GeneralPropertiesDescription} with exactly the same properties.
     */
    @Override
    public Set<PropertiesDescription> generateOneMoreGeneral() {
        return Collections.<PropertiesDescription>singleton(new GeneralPropertiesDescription(getProperties()));
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

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return LITERAL + SEPARATOR + "true" + SEPARATOR + super.toString();
    }
}
