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
import com.graphaware.neo4j.relcount.full.dto.relationship.GeneralRelationshipDescription;
import com.graphaware.neo4j.relcount.full.dto.relationship.LiteralRelationshipDescription;
import com.graphaware.neo4j.relcount.full.logic.FullNaiveRelationshipCountReader;
import com.graphaware.neo4j.relcount.full.strategy.ExtractAllRelationshipProperties;
import com.graphaware.neo4j.relcount.full.strategy.ExtractNoRelationshipProperties;
import com.graphaware.neo4j.relcount.full.strategy.RelationshipCountStrategies;
import com.graphaware.neo4j.relcount.full.strategy.RelationshipCountStrategiesImpl;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

/**
 * A naive {@link FullRelationshipCounter} that counts matching relationships by inspecting all {@link org.neo4j.graphdb.Node}'s {@link org.neo4j.graphdb.Relationship}s.
 * <p/>
 * <b>Full</b> relationship counter means that it inspects relationship types, directions, and properties.
 * <p/>
 * If no properties are provided to this counter and the {@link #count(org.neo4j.graphdb.Node)} method is invoked,
 * no relationship properties will be inspected.
 * <p/>
 * Because relationships are counted on the fly (no caching performed), this can be used without the
 * {@link com.graphaware.neo4j.framework.GraphAwareFramework} and/or any {@link com.graphaware.neo4j.framework.GraphAwareModule}s.
 * <p/>
 * This counter always returns a count, never throws {@link com.graphaware.neo4j.relcount.common.api.UnableToCountException}.
 */
public class FullNaiveRelationshipCounter extends BaseFullRelationshipCounter implements FullRelationshipCounter {

    private final RelationshipCountStrategies relationshipCountStrategies;

    /**
     * Construct a new relationship counter with default strategies.
     *
     * @param type      type of the relationships to count.
     * @param direction direction of the relationships to count.
     */
    public FullNaiveRelationshipCounter(RelationshipType type, Direction direction) {
        this(type, direction, RelationshipCountStrategiesImpl.defaultStrategies());
    }

    /**
     * Construct a new relationship counter. Use when custom {@link RelationshipCountStrategies} have been used for the
     * {@link com.graphaware.neo4j.relcount.full.module.FullRelationshipCountModule}. Alternatively, it might be easier
     * use {@link com.graphaware.neo4j.relcount.full.module.FullRelationshipCountModule#naiveCounter(org.neo4j.graphdb.RelationshipType, org.neo4j.graphdb.Direction)}.
     *
     * @param type                        type of the relationships to count.
     * @param direction                   direction of the relationships to count.
     * @param relationshipCountStrategies strategies, of which only {@link com.graphaware.neo4j.relcount.full.strategy.RelationshipPropertiesExtractionStrategy} is used.
     */
    public FullNaiveRelationshipCounter(RelationshipType type, Direction direction, RelationshipCountStrategies relationshipCountStrategies) {
        super(type, direction);
        this.relationshipCountStrategies = relationshipCountStrategies;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int count(Node node) {
        if (getProperties().isEmpty() && relationshipCountStrategies.getRelationshipPropertiesExtractionStrategy().equals(ExtractAllRelationshipProperties.getInstance())) {
            return new FullNaiveRelationshipCountReader(ExtractNoRelationshipProperties.getInstance(), relationshipCountStrategies.getRelationshipWeighingStrategy()).getRelationshipCount(new GeneralRelationshipDescription(this), node);
        }

        return new FullNaiveRelationshipCountReader(relationshipCountStrategies.getRelationshipPropertiesExtractionStrategy(), relationshipCountStrategies.getRelationshipWeighingStrategy()).getRelationshipCount(new GeneralRelationshipDescription(this), node);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int countLiterally(Node node) {
        return new FullNaiveRelationshipCountReader(relationshipCountStrategies.getRelationshipPropertiesExtractionStrategy(), relationshipCountStrategies.getRelationshipWeighingStrategy()).getRelationshipCount(new LiteralRelationshipDescription(this), node);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FullRelationshipCounter with(String key, Object value) {
        return new FullNaiveRelationshipCounter(getType(), getDirection(), getProperties().with(key, value), relationshipCountStrategies);
    }

    /**
     * Construct a counter.
     *
     * @param type       type.
     * @param direction  direction.
     * @param properties props.
     */
    protected FullNaiveRelationshipCounter(RelationshipType type, Direction direction, CopyMakingSerializableProperties properties, RelationshipCountStrategies relationshipCountStrategies) {
        super(type, direction, properties);
        this.relationshipCountStrategies = relationshipCountStrategies;
    }
}