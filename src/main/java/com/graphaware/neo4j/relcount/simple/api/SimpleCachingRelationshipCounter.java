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
import com.graphaware.neo4j.relcount.simple.dto.TypeAndDirectionDescriptionImpl;
import com.graphaware.neo4j.relcount.simple.manager.SimpleCachingRelationshipCountManager;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

/**
 * A {@link SimpleRelationshipCounter} that counts matching relationships by by looking them up in cached {@link org.neo4j.graphdb.Node}'s properties.
 * <p/>
 * <b>Simple</b> relationship counter means that it inspects relationship types and directions, but <b>not</b> properties.
 * <p/>
 * Matching relationships are all relationships that are exactly the same as the relationship description provided to this counter.
 * <p/>
 * WARNING: This counter will only work if {@link com.graphaware.neo4j.relcount.simple.handler.SimpleRelationshipCountTransactionEventHandler}
 * is used! If you just started using this functionality and you have an existing graph, call //todo!!! (re-caclculate counts)
 * <p/>
 * This counter always returns a count, never throws {@link com.graphaware.neo4j.relcount.common.api.UnableToCountException}.
 * If a relationship count isn't cached and you think it should be, please check that you are using {@link com.graphaware.neo4j.relcount.simple.handler.SimpleRelationshipCountTransactionEventHandler}
 * on the {@link org.neo4j.graphdb.GraphDatabaseService} and that you haven't excluded the relationships from caching by
 * means of a custom {@link com.graphaware.neo4j.tx.event.strategy.RelationshipInclusionStrategy}.
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
        return new SimpleCachingRelationshipCountManager().getRelationshipCount(new TypeAndDirectionDescriptionImpl(this), node);
    }
}
