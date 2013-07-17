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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static com.graphaware.neo4j.relcount.full.dto.property.LiteralPropertiesDescription.LITERAL;
import static com.graphaware.neo4j.utils.DirectionUtils.resolveDirection;

/**
 * A full-blown implementation of {@link com.graphaware.neo4j.relcount.common.logic.RelationshipCountCache}.  It is "full" in
 * the sense that it cares about {@link org.neo4j.graphdb.RelationshipType}s, {@link org.neo4j.graphdb.Direction}s, and properties.
 */
public class FullRelationshipCountCache extends BaseRelationshipCountCache<RelationshipDescription> implements RelationshipCountCache {

    private static final Logger LOG = Logger.getLogger(FullRelationshipCountCache.class);

    private final RelationshipCountStrategies relationshipCountStrategies;

    public FullRelationshipCountCache(String id, RelationshipCountStrategies relationshipCountStrategies) {
        super(id);
        this.relationshipCountStrategies = relationshipCountStrategies;
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
    protected RelationshipDescription newCachedRelationship(String string, String prefix) {
        RelationshipDescription result = new GeneralRelationshipDescription(string, prefix);

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
            compactRelationshipCounts(pointOfView); //todo async
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

    /**
     * Compact relationship counts if needed.
     * <p/>
     * For example, a node might have the following relationship counts:
     * - FRIEND_OF#OUTGOING#timestamp#5.4.2013 - 1x
     * - FRIEND_OF#OUTGOING#timestamp#6.4.2013 - 3x
     * - FRIEND_OF#OUTGOING#timestamp#7.4.2013 - 5x
     * <p/>
     * The cache might decide to compact this into:
     * - FRIEND_OF#OUTGOING - 9x
     *
     * @param node to compact cached relationship counts on.
     */
    public void compactRelationshipCounts(Node node) {
        if (!performCompaction(node)) {
            LOG.warn("Compactor could not reach the desired threshold (" + relationshipCountStrategies.getCompactionThreshold() + ") " +
                    "on node " + node.getId() + ". This is potentially due to the fact that there are more than " + relationshipCountStrategies.getCompactionThreshold() +
                    " distinct relationship type - direction pairs being cached for the node. If that's what's desired," +
                    " increase the threshold. If not, implement a RelationshipInclusionStrategy and that does not include" +
                    " the unwanted relationships.");
        }
    }

    private boolean performCompaction(Node node) {
        Map<RelationshipDescription, Integer> cachedCounts = getRelationshipCounts(node);

        //Not above the threshold => no need for compaction
        if (cachedCounts.size() < relationshipCountStrategies.getCompactionThreshold()) {
            return true;
        }

        //Generate all possible generalizations
        Set<RelationshipDescription> generalizations = new TreeSet<>();
        for (RelationshipDescription cached : cachedCounts.keySet()) {
            generalizations.addAll(cached.generateAllMoreGeneral());
        }

        //Find the most specific generalization that has a chance to result in some compaction
        for (RelationshipDescription generalization : generalizations) {
            Set<RelationshipDescription> candidates = new HashSet<>();
            for (RelationshipDescription potentialCandidate : cachedCounts.keySet()) {
                if (generalization.isMoreGeneralThan(potentialCandidate)) {
                    candidates.add(potentialCandidate);
                }
            }

            //See if the generalization will result in compaction
            if (candidates.size() > 1) {

                //It will, do it!
                int candidateCachedCount = 0;
                for (RelationshipDescription candidate : candidates) {
                    candidateCachedCount += cachedCounts.get(candidate);
                    deleteCount(candidate, node);
                }

                incrementCount(generalization, node, candidateCachedCount);

                //After the compaction, see if more is needed using a recursive call
                performCompaction(node);

                //Break the for loop, other generalizations (if needed) will be found using recursion above.
                break;
            }
        }

        //If we reached this point and haven't compacted enough, we can't!
        return getRelationshipCounts(node).size() < relationshipCountStrategies.getCompactionThreshold();
    }
}
