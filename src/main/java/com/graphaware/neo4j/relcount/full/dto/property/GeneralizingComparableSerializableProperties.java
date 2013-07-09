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

import com.graphaware.neo4j.dto.string.property.CopyMakingSerializableProperties;
import org.neo4j.graphdb.PropertyContainer;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 *
 */
public abstract class GeneralizingComparableSerializableProperties<T extends GeneralizingProperties<T> & CopyMakingSerializableProperties<T>> extends TotallyComparableSerializableProperties<T> {

    protected GeneralizingComparableSerializableProperties(PropertyContainer propertyContainer) {
        super(propertyContainer);
    }

    protected GeneralizingComparableSerializableProperties(Map<String, String> properties) {
        super(properties);
    }

    protected GeneralizingComparableSerializableProperties(String string) {
        super(string);
    }

    /**
     * Generate items one step more general than (or as general as) this instance.
     *
     * @return set of one-level more/equally general instances, ordered by decreasing generality.
     */
    public Set<T> generateOneMoreGeneral() {
        Set<T> result = new TreeSet<>();
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
    public Set<T> generateAllMoreGeneral() {
        return generateAllMoreGeneral(self());
    }

    protected Set<T> generateAllMoreGeneral(T propertiesRepresentation) {
        //base case
        if (propertiesRepresentation.isEmpty()) {
            return Collections.singleton(propertiesRepresentation);
        }

        //recursion
        Set<T> result = new TreeSet<>();
        Map.Entry<String, String> next = propertiesRepresentation.entrySet().iterator().next();
        for (T properties : generateAllMoreGeneral(propertiesRepresentation.without(next.getKey()))) {
            result.add(properties);
            result.add(properties.with(next.getKey(), next.getValue()));
        }

        return result;
    }

    /**
     * @return this.
     */
    protected abstract T self();
}
