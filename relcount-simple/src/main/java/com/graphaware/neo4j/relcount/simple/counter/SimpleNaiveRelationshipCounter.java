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

package com.graphaware.neo4j.relcount.simple.counter;

import com.graphaware.neo4j.dto.common.relationship.TypeAndDirection;
import com.graphaware.neo4j.relcount.simple.dto.TypeAndDirectionDescriptionImpl;
import com.graphaware.neo4j.relcount.simple.logic.SimpleNaiveRelationshipCountReader;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

/**
 * A naive {@link SimpleRelationshipCounter} that counts matching relationships by inspecting all {@link org.neo4j.graphdb.Node}'s {@link org.neo4j.graphdb.Relationship}s.
 * <p/>
 * <b>Simple</b> relationship counter means that it inspects relationship types and directions, but <b>not</b> properties.
 * <p/>
 * Matching relationships are all relationships that are of the same {@link RelationshipType} and {@link Direction} as the relationship description provided to this counter.
 * <p/>
 * Because relationships are counted on the fly (no caching performed), this can be used on any graph without any
 * {@link com.graphaware.neo4j.framework.GraphAwareModule}s registered and even without the
 * {@link com.graphaware.neo4j.framework.GraphAwareFramework} running at all.
 * <p/>
 * This counter always returns a count, never throws {@link com.graphaware.neo4j.relcount.common.counter.UnableToCountException}.
 */
public class SimpleNaiveRelationshipCounter extends TypeAndDirection implements SimpleRelationshipCounter {

    /**
     * Construct a new relationship counter.
     *
     * @param type      type of the relationships to count.
     * @param direction direction of the relationships to count.
     */
    public SimpleNaiveRelationshipCounter(RelationshipType type, Direction direction) {
        super(type, direction);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int count(Node node) {
        return new SimpleNaiveRelationshipCountReader().getRelationshipCount(new TypeAndDirectionDescriptionImpl(this), node);
    }
}
