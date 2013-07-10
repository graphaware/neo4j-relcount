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

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 *
 */
public abstract class BaseCandidateProperties<T extends GeneralizingProperties<T> & CopyMakingSerializableProperties<T>> extends BaseCopyMakingSerializableProperties<T> {

    protected BaseCandidateProperties(PropertyContainer propertyContainer) {
        super(propertyContainer);
    }

    protected BaseCandidateProperties(Map<String, String> properties) {
        super(properties);
    }

    protected BaseCandidateProperties(String string) {
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

    /**
     * {@inheritDoc}
     */
    public int compareTo(TotallyComparableProperties that) {
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
