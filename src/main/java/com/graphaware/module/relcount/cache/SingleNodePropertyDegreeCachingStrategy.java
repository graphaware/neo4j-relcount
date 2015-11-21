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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.graphaware.common.serialize.Serializer.fromByteArray;
import static com.graphaware.common.serialize.Serializer.toByteArray;

/**
 * {@link DegreeCachingStrategy} that caches degrees as a single node property on the node that the degrees are for.
 * The key of the property is the prefix (runtime identifier + module prefix) and the value is the entire map of
 * degrees serialized to a byte array.
 */
public class SingleNodePropertyDegreeCachingStrategy implements DegreeCachingStrategy {

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeDegrees(Node node, String prefix, Map<DetachedRelationshipDescription, Integer> cachedDegrees, Set<DetachedRelationshipDescription> updatedDegrees, Set<DetachedRelationshipDescription> removedDegrees) {
        node.setProperty(prefix, toByteArray(cachedDegrees));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<DetachedRelationshipDescription, Integer> readDegrees(Node node, String prefix) {
        if (!node.hasProperty(prefix)) {
            return new HashMap<>();
        }

        //noinspection unchecked
        return fromByteArray((byte[]) node.getProperty(prefix));
    }
}
