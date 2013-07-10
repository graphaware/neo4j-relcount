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
import com.graphaware.neo4j.relcount.full.dto.relationship.GeneralRelationshipDescription;
import com.graphaware.neo4j.tx.event.strategy.RelationshipPropertiesExtractionStrategy;
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
 * Matching relationships are all relationships at least as specific as the relationship description provided to this counter.
 * For example, if this counter is configured to count all OUTGOING relationships of type "FRIEND" with property "strength"
 * equal to 2, all relationships with that specification <b>including those with other properties</b> (such as "timestamp" = 123456)
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
 * the relationships being counted (0 is returned). If you prefer exception to fallback, use {@link FullCachedMoreGeneralRelationshipCounter}.
 */
public class FullMoreGeneralRelationshipCounter extends BasePossiblyNaiveRelationshipCounter implements FullRelationshipCounter {

    private static final Logger LOG = Logger.getLogger(FullMoreGeneralRelationshipCounter.class);

    /**
     * Construct a new relationship counter.
     * <p/>
     * Properties are extracted using {@link com.graphaware.neo4j.tx.event.strategy.ExtractAllRelationshipProperties}.
     *
     * @param type      type of the relationships to count.
     * @param direction direction of the relationships to count.
     */
    public FullMoreGeneralRelationshipCounter(RelationshipType type, Direction direction) {
        super(type, direction);
    }

    /**
     * Construct a new relationship counter.
     *
     * @param type               type of the relationships to count.
     * @param direction          direction of the relationships to count.
     * @param extractionStrategy for extracting properties from relationships.
     */
    public FullMoreGeneralRelationshipCounter(RelationshipType type, Direction direction, RelationshipPropertiesExtractionStrategy extractionStrategy) {
        super(type, direction, extractionStrategy);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int count(Node node) {
        try {
            return new FullCachedMoreGeneralRelationshipCounter(this).count(node);
        } catch (UnableToCountException e) {
            LOG.warn("Unable to count relationships with description: " + new GeneralRelationshipDescription(this).toString() +
                    " for node " + node.toString() + ". Falling back to naive approach");
            return new FullNaiveMoreGeneralRelationshipCounter(this, extractionStrategy).count(node);
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public FullRelationshipCounter with(String key, Object value) {
        return new FullMoreGeneralRelationshipCounter(getType(), getDirection(), getProperties().with(key, value), extractionStrategy);
    }

    /**
     * Construct a counter.
     *
     * @param type               type.
     * @param direction          direction.
     * @param properties         props.
     * @param extractionStrategy for extracting properties from relationships.
     */
    protected FullMoreGeneralRelationshipCounter(RelationshipType type, Direction direction, CopyMakingSerializableProperties properties, RelationshipPropertiesExtractionStrategy extractionStrategy) {
        super(type, direction, properties, extractionStrategy);
    }
}
