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
import com.graphaware.neo4j.relcount.full.dto.common.TotallyComparable;

/**
 * Interface for types that can be compared with one another using a general to specific ordering, with a fallback to
 * total ordering using an alternative {@link Comparable} in case nothing can be said about the two compared objects'
 * relative generality.
 *
 */
public interface TotallyComparableProperties extends PartiallyComparableProperties, TotallyComparable<ImmutableProperties<String>, TotallyComparableProperties> {
}
