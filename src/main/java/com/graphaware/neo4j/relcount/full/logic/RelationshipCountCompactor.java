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

package com.graphaware.neo4j.relcount.full.logic;

import org.neo4j.graphdb.Node;

/**
 * Component responsible for compacting the relationship counts cached as properties on nodes.
 * <p/>
 * For example, a node might have the following relationship counts:
 * - FRIEND_OF#OUTGOING#timestamp#5.4.2013 - 1x
 * - FRIEND_OF#OUTGOING#timestamp#6.4.2013 - 3x
 * - FRIEND_OF#OUTGOING#timestamp#7.4.2013 - 5x
 * <p/>
 * The compactor might decide to compact this into:
 * - FRIEND_OF#OUTGOING - 9x
 */
public interface RelationshipCountCompactor {

    /**
     * Compact relationship counts if needed.
     * <p/>
     * Note: assumes a transaction is running.
     *
     * @param node to compact cached relationship counts on.
     */
    void compactRelationshipCounts(Node node);
}
