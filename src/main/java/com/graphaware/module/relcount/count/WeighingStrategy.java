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

package com.graphaware.module.relcount.count;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * A strategy for determining the weight of a {@link Relationship} when counting relationships. This could, for
 * example, be a property on the {@link Relationship}, or it could be computed based on the {@link Node}s the
 * relationship connects.
 */
public interface WeighingStrategy {

    /**
     * Get a relationship's weight.
     *
     * @param relationship to find weight for.
     * @param pointOfView  node whose point of view we are currently looking at the relationship. This gives the opportunity
     *                     to determine relationship weight based on the other node's characteristics, for instance.
     * @return the relationship weight. Should be positive.
     */
    int getRelationshipWeight(Relationship relationship, Node pointOfView);
}
