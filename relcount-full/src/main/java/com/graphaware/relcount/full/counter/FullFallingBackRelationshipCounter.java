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

package com.graphaware.relcount.full.counter;

import com.graphaware.framework.config.DefaultFrameworkConfiguration;
import com.graphaware.framework.config.FrameworkConfiguration;
import com.graphaware.propertycontainer.dto.string.property.CopyMakingSerializableProperties;
import com.graphaware.relcount.common.counter.UnableToCountException;
import com.graphaware.relcount.full.internal.dto.relationship.LiteralRelationshipDescription;
import com.graphaware.relcount.full.internal.dto.relationship.WildcardRelationshipDescription;
import com.graphaware.relcount.full.module.FullRelationshipCountModule;
import com.graphaware.relcount.full.strategy.RelationshipCountStrategies;
import com.graphaware.relcount.full.strategy.RelationshipCountStrategiesImpl;
import org.apache.log4j.Logger;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

/**
 * {@link FullRelationshipCounter} that counts matching relationships by first trying to look them up in cached
 * {@link org.neo4j.graphdb.Node}'s properties, falling back to naive approach of iterating through all {@link Node}'s
 * {@link org.neo4j.graphdb.Relationship}s.
 * <p/>
 * This is a <b>full</b> relationship counter, meaning that it inspects relationship types, directions, and properties.
 * <p/>
 * It should be used in conjunction with {@link com.graphaware.relcount.full.module.FullRelationshipCountModule}
 * registered with {@link com.graphaware.neo4j.framework.GraphAwareFramework}. The easiest and recommended way to create
 * and instance of this counter is through the corresponding {@link com.graphaware.relcount.full.module.FullRelationshipCountModule#fallingBackCounter(org.neo4j.graphdb.RelationshipType, org.neo4j.graphdb.Direction)}.
 * <p/>
 * This counter always returns a count, never throws {@link com.graphaware.relcount.common.counter.UnableToCountException}.
 * <p/>
 * About fallback: Fallback to naive approach only happens if it is detected that compaction has taken place
 * (see {@link com.graphaware.relcount.full.internal.cache.FullRelationshipCountCache}) and the needed granularity has
 * been compacted out. There is a performance penalty to this fallback.
 * To avoid it, make sure the compaction threshold is set correctly. No fallback happens when a {@link com.graphaware.neo4j.tx.event.strategy.RelationshipInclusionStrategy} has been used that explicitly excludes
 * the relationships being counted (0 is returned). If you prefer an exception to fallback, use {@link FullCachedRelationshipCounter}.
 */
public class FullFallingBackRelationshipCounter extends BaseFullRelationshipCounter implements FullRelationshipCounter {

    private static final Logger LOG = Logger.getLogger(FullFallingBackRelationshipCounter.class);

    private final String id;
    private final RelationshipCountStrategies strategies;
    private final FrameworkConfiguration config;

    /**
     * Construct a new relationship counter with default settings. Use this constructor when
     * {@link com.graphaware.neo4j.framework.GraphAwareFramework} is used with default configuration, only a single
     * instance of {@link com.graphaware.relcount.full.module.FullRelationshipCountModule} is registered, and
     * no custom {@link RelationshipCountStrategies} are in use. If unsure, it is always easy and correct to instantiate
     * this counter through {@link com.graphaware.relcount.full.module.FullRelationshipCountModule#fallingBackCounter(org.neo4j.graphdb.RelationshipType, org.neo4j.graphdb.Direction)}.
     *
     * @param type      type of the relationships to count.
     * @param direction direction of the relationships to count.
     */
    public FullFallingBackRelationshipCounter(RelationshipType type, Direction direction) {
        this(type, direction, FullRelationshipCountModule.FULL_RELCOUNT_DEFAULT_ID, RelationshipCountStrategiesImpl.defaultStrategies(), DefaultFrameworkConfiguration.getInstance());
    }

    /**
     * Construct a new relationship counter with granular settings. It is always easier and recommended to use
     * {@link com.graphaware.relcount.full.module.FullRelationshipCountModule#fallingBackCounter(org.neo4j.graphdb.RelationshipType, org.neo4j.graphdb.Direction)}
     * instead.
     *
     * @param type       type of the relationships to count.
     * @param direction  direction of the relationships to count.
     * @param id         of the {@link com.graphaware.relcount.full.module.FullRelationshipCountModule} used to cache relationship counts.
     * @param strategies for counting relationships, provided to the {@link com.graphaware.relcount.full.module.FullRelationshipCountModule}.
     * @param config     used with the {@link com.graphaware.neo4j.framework.GraphAwareFramework}.
     */
    public FullFallingBackRelationshipCounter(RelationshipType type, Direction direction, String id, RelationshipCountStrategies strategies, FrameworkConfiguration config) {
        super(type, direction);
        this.id = id;
        this.strategies = strategies;
        this.config = config;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int count(Node node) {
        try {
            return new FullCachedRelationshipCounter(getType(), getDirection(), getProperties(), id, config).count(node);
        } catch (UnableToCountException e) {
            LOG.warn("Unable to count relationships with description: " + new WildcardRelationshipDescription(this).toString() +
                    " for node " + node.toString() + ". Falling back to naive approach");
            return new FullNaiveRelationshipCounter(getType(), getDirection(), getProperties(), strategies).count(node);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int countLiterally(Node node) {
        try {
            return new FullCachedRelationshipCounter(getType(), getDirection(), getProperties(), id, config).countLiterally(node);
        } catch (UnableToCountException e) {
            LOG.warn("Unable to count relationships with description: " + new LiteralRelationshipDescription(this).toString() +
                    " for node " + node.toString() + ". Falling back to naive approach");
            return new FullNaiveRelationshipCounter(getType(), getDirection(), getProperties(), strategies).countLiterally(node);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FullRelationshipCounter with(String key, Object value) {
        return new FullFallingBackRelationshipCounter(getType(), getDirection(), getProperties().with(key, value), id, strategies, config);
    }

    /**
     * Construct a counter.
     *
     * @param type       type.
     * @param direction  direction.
     * @param properties props.
     * @param strategies strategies, of which only {@link com.graphaware.relcount.full.strategy.RelationshipPropertiesExtractionStrategy}
     */
    protected FullFallingBackRelationshipCounter(RelationshipType type, Direction direction, CopyMakingSerializableProperties properties, String id, RelationshipCountStrategies strategies, FrameworkConfiguration config) {
        super(type, direction, properties);
        this.id = id;
        this.strategies = strategies;
        this.config = config;
    }
}
