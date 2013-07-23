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

package com.graphaware.neo4j.relcount.full.api;

import com.graphaware.neo4j.dto.string.property.CopyMakingSerializableProperties;
import com.graphaware.neo4j.framework.config.DefaultFrameworkConfiguration;
import com.graphaware.neo4j.framework.config.FrameworkConfiguration;
import com.graphaware.neo4j.relcount.full.dto.relationship.LiteralRelationshipDescription;
import com.graphaware.neo4j.relcount.full.dto.relationship.WildcardRelationshipDescription;
import com.graphaware.neo4j.relcount.full.logic.FullCachedRelationshipCountReader;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

import static com.graphaware.neo4j.relcount.full.Constants.FULL_RELCOUNT_DEFAULT_ID;

/**
 * {@link FullRelationshipCounter} that counts matching relationships by looking them up cached in {@link org.neo4j.graphdb.Node}'s properties.
 * <p/>
 * <b>Full</b> relationship counter means that it inspects relationship types, directions, and properties.
 * <p/>
 * It must be used in conjunction with {@link com.graphaware.neo4j.relcount.full.module.FullRelationshipCountModule}
 * registered with {@link com.graphaware.neo4j.framework.GraphAwareFramework}.
 * <p/>
 * This counter throws {@link com.graphaware.neo4j.relcount.common.api.UnableToCountException} if it detects it can not
 * reliably answer the question. This means compaction has taken place and this counter can't serve a request for
 * relationship count this specific. If you still want to count the relationship, either use {@link FullNaiveRelationshipCounter}
 * or consider increasing the compaction threshold.
 *
 * @see com.graphaware.neo4j.relcount.full.logic.RelationshipCountCompactor
 */
public class FullCachedRelationshipCounter extends BaseFullRelationshipCounter implements FullRelationshipCounter {

    private final String id;
    private final FrameworkConfiguration config;

    /**
     * Construct a new relationship counter. Use this constructor when {@link com.graphaware.neo4j.framework.GraphAwareFramework}
     * is used with default configuration and only a single instance of {@link com.graphaware.neo4j.relcount.full.module.FullRelationshipCountModule}
     * is registered. This will be the case for most use cases.
     *
     * @param type      type of the relationships to count.
     * @param direction direction of the relationships to count.
     */
    public FullCachedRelationshipCounter(RelationshipType type, Direction direction) {
        this(type, direction, FULL_RELCOUNT_DEFAULT_ID);
    }

    /**
     * Construct a new relationship counter. Use this constructor when the
     * {@link com.graphaware.neo4j.framework.GraphAwareFramework} is used with custom configuration. This should rarely
     * be the case. Alternatively, use {@link com.graphaware.neo4j.relcount.full.module.FullRelationshipCountModule#cachedCounter(org.neo4j.graphdb.RelationshipType, org.neo4j.graphdb.Direction)}.
     *
     * @param type      type of the relationships to count.
     * @param direction direction of the relationships to count.
     * @param config    used with the {@link com.graphaware.neo4j.framework.GraphAwareFramework}.
     */
    public FullCachedRelationshipCounter(RelationshipType type, Direction direction, FrameworkConfiguration config) {
        this(type, direction, FULL_RELCOUNT_DEFAULT_ID, config);
    }

    /**
     * Construct a new relationship counter. Use this constructor when multiple instances of {@link com.graphaware.neo4j.relcount.full.module.FullRelationshipCountModule}
     * have been registered with the {@link com.graphaware.neo4j.framework.GraphAwareFramework}.
     * Alternatively, use {@link com.graphaware.neo4j.relcount.full.module.FullRelationshipCountModule#cachedCounter(org.neo4j.graphdb.RelationshipType, org.neo4j.graphdb.Direction)}.
     *
     * @param type      type of the relationships to count.
     * @param direction direction of the relationships to count.
     * @param id        of the {@link com.graphaware.neo4j.relcount.full.module.FullRelationshipCountModule} used to cache relationship counts.
     */
    public FullCachedRelationshipCounter(RelationshipType type, Direction direction, String id) {
        this(type, direction, id, DefaultFrameworkConfiguration.getInstance());
    }

    /**
     * Construct a new relationship counter. Use this constructor when multiple instances of {@link com.graphaware.neo4j.relcount.full.module.FullRelationshipCountModule}
     * have been registered with the {@link com.graphaware.neo4j.framework.GraphAwareFramework} and when the
     * {@link com.graphaware.neo4j.framework.GraphAwareFramework} is used with custom configuration. This should rarely
     * be the case. Alternatively, use {@link com.graphaware.neo4j.relcount.full.module.FullRelationshipCountModule#cachedCounter(org.neo4j.graphdb.RelationshipType, org.neo4j.graphdb.Direction)}.
     *
     * @param type      type of the relationships to count.
     * @param direction direction of the relationships to count.
     * @param id        of the {@link com.graphaware.neo4j.relcount.full.module.FullRelationshipCountModule} used to cache relationship counts.
     * @param config    used with the {@link com.graphaware.neo4j.framework.GraphAwareFramework}.
     */
    public FullCachedRelationshipCounter(RelationshipType type, Direction direction, String id, FrameworkConfiguration config) {
        super(type, direction);
        this.id = id;
        this.config = config;
    }

    /**
     * Construct a counter.
     *
     * @param type       type.
     * @param direction  direction.
     * @param properties props.
     */
    protected FullCachedRelationshipCounter(RelationshipType type, Direction direction, CopyMakingSerializableProperties properties, String id, FrameworkConfiguration config) {
        super(type, direction, properties);
        this.id = id;
        this.config = config;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int count(Node node) {
        FullCachedRelationshipCountReader countReader = new FullCachedRelationshipCountReader(id, config);

        if (Direction.BOTH.equals(getDirection())) {
            int outgoingResult = countReader.getRelationshipCount(new WildcardRelationshipDescription(this.getType(), Direction.OUTGOING, this.getProperties().getProperties()), node);
            int incomingResult = countReader.getRelationshipCount(new WildcardRelationshipDescription(this.getType(), Direction.INCOMING, this.getProperties().getProperties()), node);
            return incomingResult + outgoingResult;
        }

        return countReader.getRelationshipCount(new WildcardRelationshipDescription(this), node);
    }

    @Override
    public int countLiterally(Node node) {
        FullCachedRelationshipCountReader countReader = new FullCachedRelationshipCountReader(id, config);

        if (Direction.BOTH.equals(getDirection())) {
            int outgoingResult = countReader.getRelationshipCount(new LiteralRelationshipDescription(this.getType(), Direction.OUTGOING, this.getProperties().getProperties()), node);
            int incomingResult = countReader.getRelationshipCount(new LiteralRelationshipDescription(this.getType(), Direction.INCOMING, this.getProperties().getProperties()), node);
            return incomingResult + outgoingResult;
        }

        return countReader.getRelationshipCount(new LiteralRelationshipDescription(this), node);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FullRelationshipCounter with(String key, Object value) {
        return new FullCachedRelationshipCounter(getType(), getDirection(), getProperties().with(key, value), id, config);
    }
}
