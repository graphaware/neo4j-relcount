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
 *
 */
public class UndefinedIsValueComparableProperties extends ComparableProperties {

    public static final String LITERAL = "_LITERAL_";
    private static final String UNDEF = "UNDEF";

    public UndefinedIsValueComparableProperties(PropertyContainer propertyContainer) {
        super(propertyContainer);
    }

    public UndefinedIsValueComparableProperties(Map<String, String> properties) {
        super(properties);
    }

    public UndefinedIsValueComparableProperties(String string) {
        super(string.substring(LITERAL.length()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsKey(String key) {
        return true;
    }

    /**
     * {@inheritDoc}
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
