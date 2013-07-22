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

package com.graphaware.neo4j.relcount.full.logic;

import com.graphaware.neo4j.relcount.common.logic.BaseRelationshipCountCache;
import com.graphaware.neo4j.relcount.common.logic.RelationshipCountCache;
import com.graphaware.neo4j.relcount.full.dto.relationship.GeneralRelationshipDescription;
import com.graphaware.neo4j.relcount.full.dto.relationship.LiteralRelationshipDescription;
import com.graphaware.neo4j.relcount.full.dto.relationship.RelationshipDescription;
import com.graphaware.neo4j.relcount.full.strategy.RelationshipCountStrategies;
import org.apache.log4j.Logger;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.Map;

import static com.graphaware.neo4j.relcount.full.dto.property.LiteralPropertiesDescription.LITERAL;
import static com.graphaware.neo4j.utils.DirectionUtils.resolveDirection;

/**
 * A full-blown implementation of {@link com.graphaware.neo4j.relcount.common.logic.RelationshipCountCache}.  It is "full" in
 * the sense that it cares about {@link org.neo4j.graphdb.RelationshipType}s, {@link org.neo4j.graphdb.Direction}s, and properties.
 */
public class FullRelationshipCountCache extends BaseRelationshipCountCache<RelationshipDescription> implements RelationshipCountCache {

    private static final Logger LOG = Logger.getLogger(FullRelationshipCountCache.class);

    private final RelationshipCountStrategies relationshipCountStrategies;
    private final RelationshipCountCompactor relationshipCountCompactor;

    /**
     * Construct a new cache.
     *
     * @param id                          of the module this cache belongs to.
     * @param relationshipCountStrategies strategies for counting relationships. This class is specifically interested in the compaction threshold.
     */
    public FullRelationshipCountCache(String id, RelationshipCountStrategies relationshipCountStrategies) {
        super(id);
        this.relationshipCountStrategies = relationshipCountStrategies;
        this.relationshipCountCompactor = new ThresholdBasedRelationshipCountCompactor(relationshipCountStrategies.getCompactionThreshold(), this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean cachedMatch(RelationshipDescription cached, RelationshipDescription relationship) {
        return cached.isMoreGeneralThan(relationship);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected RelationshipDescription newCachedRelationship(String string, String prefix, String separator) {
        RelationshipDescription result = new GeneralRelationshipDescription(string, prefix, separator);

        if (result.getProperties().containsKey(LITERAL)) {
            return new LiteralRelationshipDescription(result);
        }

        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleCreatedRelationship(Relationship relationship, Node pointOfView, Direction defaultDirection) {
        Map<String, String> extractedProperties = relationshipCountStrategies.getRelationshipPropertiesExtractionStrategy().extractProperties(relationship, pointOfView);
        int relationshipWeight = relationshipCountStrategies.getRelationshipWeighingStrategy().getRelationshipWeight(relationship, pointOfView);

        LiteralRelationshipDescription createdRelationship = new LiteralRelationshipDescription(relationship.getType(), resolveDirection(relationship, pointOfView, defaultDirection), extractedProperties);

        if (incrementCount(createdRelationship, pointOfView, relationshipWeight)) {
            relationshipCountCompactor.compactRelationshipCounts(pointOfView); //todo async
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleDeletedRelationship(Relationship relationship, Node pointOfView, Direction defaultDirection) {
        Map<String, String> extractedProperties = relationshipCountStrategies.getRelationshipPropertiesExtractionStrategy().extractProperties(relationship, pointOfView);
        int relationshipWeight = relationshipCountStrategies.getRelationshipWeighingStrategy().getRelationshipWeight(relationship, pointOfView);

        LiteralRelationshipDescription deletedRelationship = new LiteralRelationshipDescription(relationship.getType(), resolveDirection(relationship, pointOfView, defaultDirection), extractedProperties);

        if (!decrementCount(deletedRelationship, pointOfView, relationshipWeight)) {
            LOG.warn(deletedRelationship.toString() + " was out of sync on node " + pointOfView.getId());
        }
    }
}
