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

import java.util.*;

import static com.graphaware.neo4j.framework.config.FrameworkConfiguration.DEFAULT_SEPARATOR;

/**
 * Abstract base-class for {@link CompactibleProperties} implementations.
 */
public class CompactiblePropertiesImpl extends BaseCopyMakingSerializableProperties<CompactibleProperties> implements CompactibleProperties {

    public static final String ANY_VALUE = "_ANY_";

    /**
     * Construct a description from a {@link java.util.Map}.
     *
     * @param properties to take (copy).
     */
    public CompactiblePropertiesImpl(Map<String, ?> properties) {
        super(properties);
    }

    /**
     * Construct a description from a {@link String}.
     *
     * @param string    to construct properties from. Must be of the form key1#value1#key2#value2... (assuming # separator).
     * @param separator of keys and values, ideally a single character, must not be null or empty.
     */
    public CompactiblePropertiesImpl(String string, String separator) {
        super(string, separator);
    }

    /**
     * Is this instance more general than (or at least as general as) the given instance?
     *
     * @param properties to compare.
     * @return true iff this instance is more general than or as general as the provided instance.
     */
    public boolean isMoreGeneralThan(ImmutableProperties<String> properties) {
        for (String thisKey : keySet()) {
            String value = get(thisKey);

            if (ANY_VALUE.equals(value)) {
                continue;
            }

            if (!properties.containsKey(thisKey)) {
                return false;
            }

            if (!value.equals(properties.get(thisKey))) {
                return false;
            }
        }

        for (String thatKey : properties.keySet()) {
            if (!containsKey(thatKey)) {
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
            String value = properties.get(thatKey);

            if (ANY_VALUE.equals(value)) {
                continue;
            }

            if (!containsKey(thatKey)) {
                return false;
            }

            if (!value.equals(get(thatKey))) {
                return false;
            }
        }

        for (String thisKey : keySet()) {
            if (!properties.containsKey(thisKey)) {
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
     * @see {@link Comparable#compareTo(Object)}.
     */
    public int compareTo(CompactibleProperties that) {
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
    public Set<CompactibleProperties> generateOneMoreGeneral() {
        Set<CompactibleProperties> result = new TreeSet<>();
        result.add(this);
        for (String key : keySet()) {
            result.add(with(key, ANY_VALUE));
        }
        return result;
    }

    /**
     * Generate all items more general than (or as general as) this instance.
     *
     * @return set of all more/equally general instances, ordered by decreasing generality.
     */
    public Set<CompactibleProperties> generateAllMoreGeneral() {
        return generateAllMoreGeneral(this);
    }

    protected Set<CompactibleProperties> generateAllMoreGeneral(CompactibleProperties properties) {
        Set<String> nonWildcardKeys = nonWildcardKeys(properties);

        //base case
        if (nonWildcardKeys.isEmpty()) {
            return Collections.singleton(properties);
        }

        //recursion
        Set<CompactibleProperties> result = new TreeSet<>();
        for (String key : nonWildcardKeys) {
            for (CompactibleProperties moreGeneral : generateAllMoreGeneral(properties.with(key, ANY_VALUE))) {
                result.add(moreGeneral);
                result.add(moreGeneral.with(key, get(key)));
            }
        }

        return result;
    }

    private Set<String> nonWildcardKeys(CompactibleProperties compactibleProperties) {
        Set<String> result = new HashSet<>();
        for (String key : compactibleProperties.keySet()) {
            if (!ANY_VALUE.equals(compactibleProperties.get(key))) {
                result.add(key);
            }
        }

        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isMutuallyExclusive(ImmutableProperties<String> other) {
        for (String key : keySet()) {
            if (ANY_VALUE.equals(get(key))) {
                continue;
            }

            if (!other.containsKey(key)) {
                return true;
            }

            if (!ANY_VALUE.equals(other.get(key)) && !get(key).equals(other.get(key))) {
                return true;
            }
        }

        for (String key : other.keySet()) {
            if (ANY_VALUE.equals(other.get(key))) {
                continue;
            }

            if (!containsKey(key)) {
                return true;
            }
        }

        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected CompactibleProperties newInstance(Map<String, String> props) {
        return new CompactiblePropertiesImpl(props);
    }
}
