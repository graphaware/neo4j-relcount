/*
 * Copyright (c) 2013-2016 GraphAware
 *
 * This file is part of the GraphAware Framework.
 *
 * GraphAware Framework is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details. You should have received a copy of
 * the GNU General Public License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

package com.graphaware.module.relcount.compact;

/**
 * Encapsulates relationship type, property key and its change frequency.
 */
class PropertyChangeFrequency implements Comparable<PropertyChangeFrequency> {
    private final String type;
    private final String property;
    private final double frequency;

    PropertyChangeFrequency(String type, String property, double frequency) {
        this.type = type;
        this.property = property;
        this.frequency = frequency;
    }

    public String getType() {
        return type;
    }

    public String getProperty() {
        return property;
    }

    @Override
    public int compareTo(PropertyChangeFrequency o) {
        int result = new Double(o.frequency).compareTo(frequency);

        if (result != 0) {
            return result;
        }

        result = type.compareTo(o.type);

        if (result != 0) {
            return result;
        }

        return property.compareTo(o.property);
    }
}
