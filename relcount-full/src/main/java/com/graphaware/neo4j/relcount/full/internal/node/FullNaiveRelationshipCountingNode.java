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

package com.graphaware.neo4j.relcount.full.internal.node;

import com.graphaware.neo4j.relcount.common.internal.node.NaiveRelationshipCountingNode;
import com.graphaware.neo4j.relcount.common.internal.node.RelationshipCountingNode;
import com.graphaware.neo4j.relcount.full.internal.dto.relationship.CompactibleRelationship;
import com.graphaware.neo4j.relcount.full.internal.dto.relationship.CompactibleRelationshipImpl;
import com.graphaware.neo4j.relcount.full.internal.dto.relationship.RelationshipDescription;
import com.graphaware.neo4j.relcount.full.strategy.RelationshipPropertiesExtractionStrategy;
import com.graphaware.neo4j.relcount.full.strategy.RelationshipWeighingStrategy;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.Map;

/**
 * {@link com.graphaware.neo4j.relcount.common.internal.node.RelationshipCountingNode} that counts relationships by traversing
 * them (performs no caching). It is "full" in the sense that it cares about {@link org.neo4j.graphdb.RelationshipType}s,
 * {@link org.neo4j.graphdb.Direction}s, and properties.
 */
public class FullNaiveRelationshipCountingNode extends NaiveRelationshipCountingNode<CompactibleRelationship, RelationshipDescription> implements RelationshipCountingNode<RelationshipDescription> {

    private final RelationshipPropertiesExtractionStrategy extractionStrategy;
    private final RelationshipWeighingStrategy weighingStrategy;

    /**
     * Construct a new relationship counting node.
     *
     * @param node               wrapped Neo4j node on which to count relationships.
     * @param extractionStrategy strategy for extracting properties from relationships.
     * @param weighingStrategy   strategy for weighing each relationship.
     */
    public FullNaiveRelationshipCountingNode(Node node, RelationshipPropertiesExtractionStrategy extractionStrategy, RelationshipWeighingStrategy weighingStrategy) {
        super(node);
        this.extractionStrategy = extractionStrategy;
        this.weighingStrategy = weighingStrategy;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean candidateMatchesDescription(CompactibleRelationship candidate, RelationshipDescription description) {
        return candidate.isMoreSpecificThan(description);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected CompactibleRelationship newCandidate(Relationship relationship) {
        Map<String, String> extractedProperties = extractionStrategy.extractProperties(relationship, node);
        return new CompactibleRelationshipImpl(relationship, node, extractedProperties);   //direction can resolve to both, but that's ok for non-cached relationships
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected int relationshipWeight(Relationship relationship) {
        return weighingStrategy.getRelationshipWeight(relationship, node);
    }
}
