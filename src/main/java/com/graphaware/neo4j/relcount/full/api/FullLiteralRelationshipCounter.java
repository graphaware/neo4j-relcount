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
import com.graphaware.neo4j.relcount.common.api.UnableToCountException;
import com.graphaware.neo4j.relcount.full.dto.relationship.LiteralRelationshipDescription;
import org.apache.log4j.Logger;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

/**
 * {@link FullRelationshipCounter} that counts matching relationships by first trying to look them up in cached
 * {@link org.neo4j.graphdb.Node}'s properties, falling back to naive approach of iterating through all {@link Node}'s {@link org.neo4j.graphdb.Relationship}s.
 * <p/>
 * This is a <b>full</b> relationship counter, meaning that it inspects relationship types, directions, and properties.
 * <p/>
 * Matching relationships are all relationships that are exactly the same as the relationship description provided to this counter.
 * For example, if this counter is configured to count all OUTGOING relationships of type "FRIEND" with property "strength"
 * equal to 2, only relationships with that specification <b>excluding those with other properties</b> (such as "timestamp" = 123456)
 * will be counted.
 * <p/>
 * WARNING: This counter will only work if {@link com.graphaware.neo4j.relcount.full.handler.FullRelationshipCountTransactionEventHandler}
 * is used! If you just started using this functionality and you have an existing graph, call //todo!!! (re-caclculate counts)
 * <p/>
 * This counter always returns a count, never throws {@link com.graphaware.neo4j.relcount.common.api.UnableToCountException}.
 * <p/>
 * About fallback: Fallback to naive approach only happens if it is detected that compaction has taken place (see {@link com.graphaware.neo4j.relcount.full.compactor.RelationshipCountCompactor})
 * and the relationship being counted is more specific than corresponding generalized cached counts. There is a performance
 * penalty to this fallback. To avoid it, make sure the compaction threshold is set correctly. No fallback happens when
 * a {@link com.graphaware.neo4j.tx.event.strategy.RelationshipInclusionStrategy} has been used that explicitly excludes
 * the relationships being counted (0 is returned). If you prefer exception to fallback, use {@link FullCachedLiteralRelationshipCounter}.
 */
public class FullLiteralRelationshipCounter extends BaseFullRelationshipCounter implements FullRelationshipCounter {

    private static final Logger LOG = Logger.getLogger(FullLiteralRelationshipCounter.class);

    /**
     * Construct a new relationship counter.
     *
     * @param type      type of the relationships to count.
     * @param direction direction of the relationships to count.
     */
    public FullLiteralRelationshipCounter(RelationshipType type, Direction direction) {
        super(type, direction);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int count(Node node) {
        try {
            return new FullCachedLiteralRelationshipCounter(this).count(node);
        } catch (UnableToCountException e) {
            LOG.warn("Unable to count relationships with description: " + new LiteralRelationshipDescription(this).toString() +
                    " for node " + node.toString() + ". Falling back to naive approach");
            return new FullNaiveLiteralRelationshipCounter(this).count(node);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FullRelationshipCounter with(String key, Object value) {
        return new FullLiteralRelationshipCounter(getType(), getDirection(), getProperties().with(key, value));
    }

    /**
     * Construct a counter.
     *
     * @param type       type.
     * @param direction  direction.
     * @param properties props.
     */
    protected FullLiteralRelationshipCounter(RelationshipType type, Direction direction, CopyMakingSerializableProperties properties) {
        super(type, direction, properties);
    }
}
