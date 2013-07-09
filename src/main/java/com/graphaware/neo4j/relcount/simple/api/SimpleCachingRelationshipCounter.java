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

package com.graphaware.neo4j.relcount.simple.api;

import com.graphaware.neo4j.dto.common.relationship.TypeAndDirection;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

/**
 * Base class for {@link SimpleRelationshipCounter} implementations, allowing subclasses to choose which
 * {@link com.graphaware.neo4j.relcount.common.manager.RelationshipCountManager} to use.
 */
public class SimpleCachingRelationshipCounter extends TypeAndDirection implements SimpleRelationshipCounter {

    /**
     * Construct a new relationship counter.
     *
     * @param type      type of the relationships to count.
     * @param direction direction of the relationships to count.
     */
    public SimpleCachingRelationshipCounter(RelationshipType type, Direction direction) {
        super(type, direction);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int count(Node node) {
        return 0;//getRelationshipCountManager().getRelationshipCount(this, node);
    }
}
