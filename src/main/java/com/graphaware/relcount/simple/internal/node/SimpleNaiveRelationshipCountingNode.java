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
import com.graphaware.propertycontainer.dto.common.relationship.TypeAndDirection;
import com.graphaware.relcount.common.internal.node.NaiveRelationshipCountingNode;
import com.graphaware.relcount.common.internal.node.RelationshipCountingNode;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * {@link com.graphaware.relcount.common.internal.node.RelationshipCountingNode} that counts relationships by traversing
 * them (assumes no caching). It can thus be used on any graph without any {@link com.graphaware.framework.GraphAwareModule}s
 * registered and even without the {@link com.graphaware.framework.GraphAwareFramework} running at all.
 * <p/>
 * It is simple in the sense that it only cares about {@link org.neo4j.graphdb.RelationshipType}s and {@link org.neo4j.graphdb.Direction}s;
 * it completely ignores {@link Relationship} properties.
 */
public class SimpleNaiveRelationshipCountingNode extends NaiveRelationshipCountingNode<HasTypeAndDirection, HasTypeAndDirection> implements RelationshipCountingNode<HasTypeAndDirection> {

    /**
     * Construct a new relationship counting node.
     *
     * @param node wrapped Neo4j node on which to count relationships.
     */
    public SimpleNaiveRelationshipCountingNode(Node node) {
        super(node);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean candidateMatchesDescription(HasTypeAndDirection candidate, HasTypeAndDirection description) {
        return candidate.matches(description);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected HasTypeAndDirection newCandidate(Relationship relationship) {
        return new TypeAndDirection(relationship, node);
    }
}
