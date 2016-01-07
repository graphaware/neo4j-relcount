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

package com.graphaware.module.relcount.cache;

import com.graphaware.common.description.relationship.DetachedRelationshipDescription;
import com.graphaware.common.serialize.Serializer;
import org.neo4j.graphdb.Node;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * {@link DegreeCachingStrategy} that caches degrees as node properties on the node that the degree is for. For each
 * degree with respect to a {@link DetachedRelationshipDescription}, one property is created. The key of the property
 * is the {@link DetachedRelationshipDescription} serialized to string and the value is the actual degree.
 */
public class NodePropertiesDegreeCachingStrategy implements DegreeCachingStrategy {

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeDegrees(Node node, String prefix, Map<DetachedRelationshipDescription, Integer> cachedDegrees, Set<DetachedRelationshipDescription> updatedDegrees, Set<DetachedRelationshipDescription> removedDegrees) {
        for (DetachedRelationshipDescription updated : updatedDegrees) {
            node.setProperty(Serializer.toString(updated, prefix), cachedDegrees.get(updated));
        }

        for (DetachedRelationshipDescription removed : removedDegrees) {
            node.removeProperty(Serializer.toString(removed, prefix));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<DetachedRelationshipDescription, Integer> readDegrees(Node node, String prefix) {
        Map<DetachedRelationshipDescription, Integer> cachedCounts = new HashMap<>();

        for (String key : node.getPropertyKeys()) {
            if (key.startsWith(prefix)) {
                DetachedRelationshipDescription description = Serializer.fromString(key, prefix);
                cachedCounts.put(description, (Integer) node.getProperty(key));
            }
        }

        return cachedCounts;
    }
}
