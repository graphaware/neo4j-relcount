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

package com.graphaware.neo4j.relcount.common.api;

import org.neo4j.graphdb.Node;

/**
 * A node in/out-degree counter. Degree is the number of incoming/outgoing relationships that a node has. Implementations
 * should provide a way to describe, which relationships should be counted (i.e. direction, type, properties).
 */
public interface RelationshipCounter {

    /**
     * Count relationships described by this counter on the given node.
     *
     * @param node on which to count relationships.
     * @return number of relationships.
     */
    int count(Node node);
}
