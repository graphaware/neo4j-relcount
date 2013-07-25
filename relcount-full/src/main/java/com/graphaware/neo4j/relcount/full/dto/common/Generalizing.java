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

package com.graphaware.neo4j.relcount.full.dto.common;

import java.util.Collection;
import java.util.Set;

/**
 * Interface for types that are able to generate more general versions of themselves.
 *
 * @param <G> type of generated more general objects.
 */
public interface Generalizing<G extends Generalizing<G, H>, H> {

    /**
     * Generate all items strictly more general than this instance.
     *
     * @param helpers a collection of objects that can help with generating all more general instances for implementations
     *                that aren't aware of all the generalizing possibilities. For example, if a set of properties,
     *                in which a missing property means "undefined", should generalize itself, it can only do so by
     *                generalizing known properties to "any". This argument would be used to provide additional unknown
     *                properties that are relevant to the rest of the system.
     * @return set of all more/equally general instances, ordered by decreasing generality.
     */
    Set<G> generateAllMoreGeneral(Collection<H> helpers);
}
