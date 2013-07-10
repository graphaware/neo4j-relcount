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

import com.graphaware.neo4j.dto.common.relationship.HasTypeDirectionAndProperties;
import com.graphaware.neo4j.dto.string.property.CopyMakingSerializableProperties;
import com.graphaware.neo4j.relcount.full.manager.FullCachingRelationshipCountManager;
import com.graphaware.neo4j.relcount.simple.manager.SimpleCachingRelationshipCountManager;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

/**
 * {@link FullRelationshipCounter} that counts matching relationships by looking them up in cached {@link org.neo4j.graphdb.Node}'s properties.
 * <p/>
 * <b>Full</b> relationship counter means that it inspects relationship types, directions, and properties.
 * If no properties are provided to this counter, no relationship properties will be inspected. This effectively means
 * this becomes a {@link com.graphaware.neo4j.relcount.simple.api.SimpleCachingRelationshipCounter}.
 * <p/>
 * Matching relationships are all relationships at least as specific as the relationship description provided to this counter.
 * For example, if this counter is configured to count all OUTGOING relationships of type "FRIEND" with property "strength"
 * equal to 2, all relationships with that specification <b>including those with other properties</b> (such as "timestamp" = 123456)
 * will be counted.
 * <p/>
 * WARNING: This counter will only work if {@link com.graphaware.neo4j.relcount.full.handler.FullRelationshipCountTransactionEventHandler}
 * is used! If you just started using this functionality and you have an existing graph, call //todo!!! (re-caclculate counts)
 * <p/>
 * This counter throws {@link com.graphaware.neo4j.relcount.common.api.UnableToCountException} in case the relationship
 * being counted is more specific than any cached count, but there is a more general cached count and at the same time.
 * This means compaction has taken place and this counter can't serve a request for relationship count this specific.
 */
public class FullCachedMoreGeneralRelationshipCounter extends BaseFullRelationshipCounter implements FullRelationshipCounter {

    /**
     * Construct a new relationship counter.
     *
     * @param type      type of the relationships to count.
     * @param direction direction of the relationships to count.
     */
    public FullCachedMoreGeneralRelationshipCounter(RelationshipType type, Direction direction) {
        super(type, direction);
    }

    /**
     * Construct a relationship representation from another one.
     *
     * @param relationship relationships representation.
     */
    protected FullCachedMoreGeneralRelationshipCounter(HasTypeDirectionAndProperties<String, ?> relationship) {
        super(relationship);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int count(Node node) {
        if (getProperties().isEmpty()) {
            return new SimpleCachingRelationshipCountManager().getRelationshipCount(this, node);
        }

        return new FullCachingRelationshipCountManager().getRelationshipCount(this, node);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FullRelationshipCounter with(String key, Object value) {
        return new FullCachedMoreGeneralRelationshipCounter(getType(), getDirection(), getProperties().with(key, value));
    }

    /**
     * Construct a counter.
     *
     * @param type       type.
     * @param direction  direction.
     * @param properties props.
     */
    protected FullCachedMoreGeneralRelationshipCounter(RelationshipType type, Direction direction, CopyMakingSerializableProperties properties) {
        super(type, direction, properties);
    }
}
