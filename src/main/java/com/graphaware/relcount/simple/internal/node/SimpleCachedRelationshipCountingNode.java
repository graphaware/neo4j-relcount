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

package com.graphaware.relcount.simple.internal.node;

import com.graphaware.propertycontainer.dto.common.relationship.HasTypeAndDirection;
import com.graphaware.propertycontainer.dto.string.relationship.SerializableTypeAndDirection;
import com.graphaware.propertycontainer.dto.string.relationship.SerializableTypeAndDirectionImpl;
import com.graphaware.relcount.common.internal.node.CachedRelationshipCountingNode;
import com.graphaware.relcount.common.internal.node.RelationshipCountingNode;
import org.neo4j.graphdb.Node;

/**
 * A simple {@link RelationshipCountingNode}. It must be used in conjunction with {@link com.graphaware.relcount.simple.module.SimpleRelationshipCountModule}
 * registered with {@link com.graphaware.framework.GraphAwareFramework}.
 * <p/>
 * It is simple in the sense that it only cares about {@link org.neo4j.graphdb.RelationshipType}s
 * and {@link org.neo4j.graphdb.Direction}; it completely ignores {@link org.neo4j.graphdb.Relationship} properties.
 */
public class SimpleCachedRelationshipCountingNode extends CachedRelationshipCountingNode<SerializableTypeAndDirection, HasTypeAndDirection> implements RelationshipCountingNode<HasTypeAndDirection> {

    /**
     * Construct a new counting node.
     *
     * @param node      backing Neo4j node.
     * @param prefix    of the cached relationship string representation.
     * @param separator of information in the cached relationship string representation.
     */
    public SimpleCachedRelationshipCountingNode(Node node, String prefix, String separator) {
        super(node, prefix, separator);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean candidateMatchesDescription(SerializableTypeAndDirection candidate, HasTypeAndDirection description) {
        return candidate.matches(description);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected SerializableTypeAndDirection newCachedRelationship(String string) {
        return new SerializableTypeAndDirectionImpl(string, prefix, separator);
    }
}
