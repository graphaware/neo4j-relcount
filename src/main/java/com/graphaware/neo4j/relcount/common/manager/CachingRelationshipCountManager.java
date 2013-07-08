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

package com.graphaware.neo4j.relcount.common.manager;

import com.graphaware.neo4j.dto.common.relationship.HasTypeAndDirection;
import org.neo4j.graphdb.Node;

import java.util.Map;

/**
 * {@link RelationshipCountManager} that caches relationship counts as properties on {@link Node}s. The key of such
 * property is a {@link String} representation of some kind of relationship. The value is the number of relationships
 * of that kind the node has.
 *
 * @param <T> type of relationship representation that can be used to query relationship counts on nodes.
 * @param <C> type of the (typically string-convertible) object representation of the cached relationship.
 */
public interface CachingRelationshipCountManager<T extends HasTypeAndDirection, C extends T> extends RelationshipCountManager<T> {

    /**
     * Get all relationship counts cached on a node.
     *
     * @param node from which to get cached relationship counts.
     * @return cached relationship counts (key = relationship representation, value = count).
     */
    Map<C, Integer> getRelationshipCounts(Node node);

    /**
     * Increment the cached relationship count on the given node by 1.
     *
     * @param relationship representation of a relationship.
     * @param node         on which to increment the cached relationship count.
     * @return true iff the cached value did not exist and had to be created.
     */
    boolean incrementCount(C relationship, Node node);

    /**
     * Increment the cached relationship count on the given node by delta.
     *
     * @param relationship representation of a relationship.
     * @param node         on which to increment the cached relationship count.
     * @param delta        increment.
     * @return true iff the cached value did not exist and had to be created.
     */
    boolean incrementCount(C relationship, Node node, int delta);

    /**
     * Decrement the cached relationship count on the given node by 1.
     * Delete the cached relationship count if the count is 0 after the decrement.
     *
     * @param relationship representation of a relationship.
     * @param node         on which to decrement the cached relationship count.
     * @return true iff the cached value existed and was >= 1.
     */
    boolean decrementCount(C relationship, Node node);

    /**
     * Decrement the cached relationship count on the given node by delta.
     * Delete the cached relationship count if the count is 0 after the decrement.
     *
     * @param relationship representation of a relationship.
     * @param node         on which to decrement the cached relationship count.
     * @param delta        increment.
     * @return true iff the cached value existed and was >= delta.
     */
    boolean decrementCount(C relationship, Node node, int delta);

    /**
     * Stop tracking relationship count for a node.
     *
     * @param relationship representation of the relationship to stop tracking.
     * @param node         on which to stop tracking.
     */
    void deleteCount(C relationship, Node node);
}
