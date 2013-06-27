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

package com.graphaware.neo4j.relcount.logic;

import com.graphaware.neo4j.dto.relationship.immutable.DirectedRelationship;
import com.graphaware.neo4j.relcount.representation.ComparableRelationship;
import org.neo4j.graphdb.Node;

import java.util.Map;

/**
 * Component responsible for reading, writing, counting etc. of relationship counts cached as node properties.
 * <p/>
 * All mutating operations assume that a transaction is running.
 */
public interface RelationshipCountManager {

    /**
     * Get a relationship count cached on a node. The count is the aggregated sum of all the cached counts that match the
     * given relationship representation, i.e. all more specific and equal relationships.
     *
     * @param relationship representation of the relationship for which to get count.
     * @param node         from which to get cached relationship counts.
     * @return cached count, 0 if there is none.
     */
    int getRelationshipCount(DirectedRelationship relationship, Node node);

    /**
     * Get all relationship counts cached on a node. No aggregation is performed, this is the raw data as stored
     * (as opposed to {@link #getRelationshipCount(com.graphaware.neo4j.representation.relationship.Relationship, org.neo4j.graphdb.Node)}).
     *
     * @param node from which to get cached relationship counts.
     * @return cached relationship counts (key = relationship representation, value = count). The map is sorted
     *         so that it is iterated in a relationship specific to general order.
     */
    Map<ComparableRelationship, Integer> getRelationshipCounts(Node node);

    /**
     * Increment the cached relationship count on the given node by 1.
     *
     * @param relationship representation of a relationship.
     * @param node         on which to increment the cached relationship count.
     * @return true iff the cached value did not exist and had to be created.
     */
    boolean incrementCount(ComparableRelationship relationship, Node node);

    /**
     * Increment the cached relationship count on the given node by delta.
     *
     * @param relationship representation of a relationship.
     * @param node         on which to increment the cached relationship count.
     * @param delta        increment.
     * @return true iff the cached value did not exist and had to be created.
     */
    boolean incrementCount(ComparableRelationship relationship, Node node, int delta);

    /**
     * Decrement the cached relationship count on the given node by 1.
     * Delete the cached relationship count if the count is 0 after the decrement.
     *
     * @param relationship representation of a relationship.
     * @param node         on which to decrement the cached relationship count.
     * @return true iff the cached value existed and was >= 1.
     */
    boolean decrementCount(ComparableRelationship relationship, Node node);

    /**
     * Decrement the cached relationship count on the given node by delta.
     * Delete the cached relationship count if the count is 0 after the decrement.
     *
     * @param relationship representation of a relationship.
     * @param node         on which to decrement the cached relationship count.
     * @param delta        increment.
     * @return true iff the cached value existed and was >= delta.
     */
    boolean decrementCount(ComparableRelationship relationship, Node node, int delta);

    /**
     * Stop tracking relationship count for a node.
     *
     * @param relationship representation of the relationship to stop tracking.
     * @param node         on which to stop tracking.
     */
    void deleteCount(ComparableRelationship relationship, Node node);
}
