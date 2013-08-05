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

package com.graphaware.relcount.simple.counter;

import com.graphaware.framework.config.DefaultFrameworkConfiguration;
import com.graphaware.framework.config.FrameworkConfiguration;
import com.graphaware.propertycontainer.dto.common.relationship.TypeAndDirection;
import com.graphaware.relcount.simple.internal.node.SimpleCachedRelationshipCountingNode;
import com.graphaware.relcount.simple.module.SimpleRelationshipCountModule;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

/**
 * A {@link SimpleRelationshipCounter} that counts matching relationships by looking them up cached in {@link org.neo4j.graphdb.Node}'s properties.
 * <p/>
 * <b>Simple</b> relationship counter means that it inspects relationship types and directions, but <b>not</b> properties.
 * <p/>
 * Matching relationships are all relationships that are of the same {@link RelationshipType} and {@link Direction} as the relationship description provided to this counter.
 * <p/>
 * This counter must be used in conjunction with {@link com.graphaware.relcount.simple.module.SimpleRelationshipCountModule}
 * registered with {@link com.graphaware.neo4j.framework.GraphAwareFramework}.
 * <p/>
 * This counter always returns a count, never throws {@link com.graphaware.relcount.common.counter.UnableToCountException}.
 * <p/>
 * If a relationship count is 0 and you think it should not be, please check that you are using {@link com.graphaware.relcount.simple.module.SimpleRelationshipCountModule}
 * registered with the {@link com.graphaware.neo4j.framework.GraphAwareFramework} and that you haven't excluded the
 * relationships from caching by means of a custom {@link com.graphaware.neo4j.tx.event.strategy.RelationshipInclusionStrategy}.
 */
public class SimpleCachedRelationshipCounter extends TypeAndDirection implements SimpleRelationshipCounter {

    private final FrameworkConfiguration config;

    /**
     * Construct a new relationship counter. Use this constructor if {@link com.graphaware.neo4j.framework.GraphAwareFramework} is used
     * with default configuration (typical use case).
     *
     * @param type      type of the relationships to count.
     * @param direction direction of the relationships to count.
     */
    public SimpleCachedRelationshipCounter(RelationshipType type, Direction direction) {
        this(type, direction, DefaultFrameworkConfiguration.getInstance());
    }

    /**
     * Construct a new relationship counter. Use this constructor if {@link com.graphaware.neo4j.framework.GraphAwareFramework} is used
     * with custom configuration (will rarely be the case).
     *
     * @param type      type of the relationships to count.
     * @param direction direction of the relationships to count.
     * @param config    used with the {@link com.graphaware.neo4j.framework.GraphAwareFramework}.
     */
    public SimpleCachedRelationshipCounter(RelationshipType type, Direction direction, FrameworkConfiguration config) {
        super(type, direction);
        this.config = config;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int count(Node node) {
        return new SimpleCachedRelationshipCountingNode(node, config.createPrefix(SimpleRelationshipCountModule.SIMPLE_RELCOUNT_ID), config.separator()).getRelationshipCount(new TypeAndDirection(this));
    }
}
