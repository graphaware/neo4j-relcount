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
import com.graphaware.neo4j.relcount.full.dto.relationship.LiteralRelationshipDescription;
import com.graphaware.neo4j.relcount.full.manager.FullNaiveRelationshipCountManager;
import com.graphaware.neo4j.relcount.simple.dto.TypeAndDirectionDescriptionImpl;
import com.graphaware.neo4j.relcount.simple.manager.SimpleNaiveRelationshipCountManager;
import com.graphaware.neo4j.tx.event.strategy.RelationshipPropertiesExtractionStrategy;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

/**
 * A naive {@link FullRelationshipCounter} that counts matching relationships by inspecting all {@link org.neo4j.graphdb.Node}'s {@link org.neo4j.graphdb.Relationship}s.
 * <p/>
 * <b>Full</b> relationship counter means that it inspects relationship types, directions, and properties.
 * If no properties are provided to this counter, no relationship properties will be inspected. This effectively means
 * this becomes a {@link com.graphaware.neo4j.relcount.simple.api.SimpleNaiveRelationshipCounter}.
 * <p/>
 * Matching relationships are all relationships that are exactly the same as the relationship description provided to this counter.
 * For example, if this counter is configured to count all OUTGOING relationships of type "FRIEND" with property "strength"
 * equal to 2, only relationships with that specification <b>excluding those with other properties</b> (such as "timestamp" = 123456)
 * will be counted.
 * <p/>
 * Because relationships are counted on the fly (no caching performed), this can be used without any {@link org.neo4j.graphdb.event.TransactionEventHandler}s
 * and on already existing graphs.
 * <p/>
 * This counter always returns a count, never throws {@link com.graphaware.neo4j.relcount.common.api.UnableToCountException}.
 */
public class FullNaiveLiteralRelationshipCounter extends BasePossiblyNaiveRelationshipCounter implements FullRelationshipCounter {

    /**
     * Construct a new relationship counter.
     * <p/>
     * Properties are extracted using {@link com.graphaware.neo4j.tx.event.strategy.ExtractAllRelationshipProperties}.
     *
     * @param type      type of the relationships to count.
     * @param direction direction of the relationships to count.
     */
    public FullNaiveLiteralRelationshipCounter(RelationshipType type, Direction direction) {
        super(type, direction);
    }

    /**
     * Construct a relationship representation from another one.
     * <p/>
     * Properties are extracted using {@link com.graphaware.neo4j.tx.event.strategy.ExtractAllRelationshipProperties}.
     *
     * @param relationship relationships representation.
     */
    protected FullNaiveLiteralRelationshipCounter(HasTypeDirectionAndProperties<String, ?> relationship) {
        super(relationship);
    }

    /**
     * Construct a new relationship counter.
     *
     * @param type               type of the relationships to count.
     * @param direction          direction of the relationships to count.
     * @param extractionStrategy for extracting properties from relationships.
     */
    public FullNaiveLiteralRelationshipCounter(RelationshipType type, Direction direction, RelationshipPropertiesExtractionStrategy extractionStrategy) {
        super(type, direction, extractionStrategy);
    }

    /**
     * Construct a counter.
     *
     * @param type               type.
     * @param direction          direction.
     * @param properties         props.
     * @param extractionStrategy for extracting properties from relationships.
     */
    protected FullNaiveLiteralRelationshipCounter(RelationshipType type, Direction direction, CopyMakingSerializableProperties properties, RelationshipPropertiesExtractionStrategy extractionStrategy) {
        super(type, direction, properties, extractionStrategy);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int count(Node node) {
        if (getProperties().isEmpty()) {
            return new SimpleNaiveRelationshipCountManager().getRelationshipCount(new TypeAndDirectionDescriptionImpl(this), node);
        }

        return new FullNaiveRelationshipCountManager(extractionStrategy).getRelationshipCount(new LiteralRelationshipDescription(this), node);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FullRelationshipCounter with(String key, Object value) {
        return new FullNaiveLiteralRelationshipCounter(getType(), getDirection(), getProperties().with(key, value), extractionStrategy);
    }

    /**
     * Construct a relationship representation from another one.
     *
     * @param relationship       relationships representation.
     * @param extractionStrategy for extracting properties from relationships.
     */
    protected FullNaiveLiteralRelationshipCounter(HasTypeDirectionAndProperties<String, ?> relationship, RelationshipPropertiesExtractionStrategy extractionStrategy) {
        super(relationship, extractionStrategy);
    }
}
