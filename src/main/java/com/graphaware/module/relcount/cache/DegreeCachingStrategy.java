/*
 * Copyright (c) 2013-2015 GraphAware
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

package com.graphaware.module.relcount.cache;

import com.graphaware.common.description.relationship.DetachedRelationshipDescription;
import org.neo4j.graphdb.Node;

import java.util.Map;
import java.util.Set;

/**
 * A strategy deciding where and how to cache node degrees.
 */
public interface DegreeCachingStrategy {

    /**
     * Write node degrees to the database (or another persistent store).
     *
     * @param node           for which the degrees are being cached.
     * @param prefix         for metadata written.
     * @param cachedDegrees  the "full picture" - all cached degrees of the node.
     * @param updatedDegrees updated degrees only.
     * @param removedDegrees removed degrees only.
     */
    void writeDegrees(Node node,
                      String prefix,
                      Map<DetachedRelationshipDescription, Integer> cachedDegrees,
                      Set<DetachedRelationshipDescription> updatedDegrees,
                      Set<DetachedRelationshipDescription> removedDegrees);

    /**
     * Read the cached degrees for a node.
     *
     * @param node   to read cached degrees for.
     * @param prefix for metadata read.
     * @return cached degrees.
     */
    Map<DetachedRelationshipDescription, Integer> readDegrees(Node node, String prefix);
}
