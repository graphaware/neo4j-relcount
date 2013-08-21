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

package com.graphaware.relcount.full.internal.dto.property;

import com.graphaware.propertycontainer.dto.common.property.ImmutableProperties;
import com.graphaware.propertycontainer.dto.string.property.BaseCopyMakingSerializableProperties;

import java.util.*;

import static com.graphaware.framework.config.FrameworkConfiguration.GA_PREFIX;

/**
 * Base-class for {@link CacheablePropertiesDescription} implementations.
 */
public class CacheablePropertiesDescriptionImpl extends BaseCopyMakingSerializableProperties<CacheablePropertiesDescription> implements CacheablePropertiesDescription {

    public static final String ANY_VALUE = GA_PREFIX + "*";

    private String string;

    /**
     * Construct a description from a {@link java.util.Map}.
     *
     * @param properties to take (copy).
     */
    public CacheablePropertiesDescriptionImpl(Map<String, ?> properties) {
        super(properties);
    }

    /**
     * Construct a description from a {@link String}.
     *
     * @param string    to construct properties from. Must be of the form key1#value1#key2#value2... (assuming # separator).
     * @param separator of keys and values, ideally a single character, must not be null or empty.
     */
    public CacheablePropertiesDescriptionImpl(String string, String separator) {
        super(string, separator);
        this.string = string;
    }

    /**
     * {@inheritDoc}
     */
    @Override
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
     * {@inheritDoc}
     */
    @Override
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
     * {@inheritDoc}
     */
    @Override
    public int compareTo(CacheablePropertiesDescription that) {
        if (equals(that)) {
            return 0;
        } else if (isMoreGeneralThan(that)) {
            return 1;
        } else if (isMoreSpecificThan(that)) {
            return -1;
        }

        int result = size() - that.size();
        if (result != 0) {
            return result;

        }
        return toString().compareTo(that.toString());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<CacheablePropertiesDescription> generateAllMoreGeneral(Collection<String> unknownKeys) {
        Set<CacheablePropertiesDescription> result = generateAllMoreGeneral(this, unknownKeys);
        result.remove(this);
        return result;
    }

    protected Set<CacheablePropertiesDescription> generateAllMoreGeneral(CacheablePropertiesDescription properties, Collection<String> unknownKeys) {
        Set<String> nonWildcardKeys = nonWildcardKeys(properties, unknownKeys);

        Set<CacheablePropertiesDescription> result = new TreeSet<>();
        result.add(properties);

        //base case
        if (nonWildcardKeys.isEmpty()) {
            return result;
        }

        //recursion
        for (String key : nonWildcardKeys) {
            result.addAll(generateAllMoreGeneral(properties.with(key, ANY_VALUE), unknownKeys));
        }

        return result;
    }

    private Set<String> nonWildcardKeys(CacheablePropertiesDescription cacheablePropertiesDescription, Collection<String> unknownKeys) {
        Set<String> result = new HashSet<>();

        for (String key : cacheablePropertiesDescription.keySet()) {
            if (!ANY_VALUE.equals(cacheablePropertiesDescription.get(key))) {
                result.add(key);
            }
        }

        for (String key : unknownKeys) {
            if (!cacheablePropertiesDescription.containsKey(key)) {
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
    protected CacheablePropertiesDescription newInstance(Map<String, String> props) {
        return new CacheablePropertiesDescriptionImpl(props);
    }

    @Override
    public String toString(String separator) {
        if (string == null) {
            string = super.toString(separator);
        }
        return string;
    }
}
