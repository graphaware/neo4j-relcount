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

package com.graphaware.neo4j.relcount.logic;

import com.graphaware.neo4j.relcount.representation.ComparableRelationship;
import org.neo4j.graphdb.Node;

import java.util.HashMap;
import java.util.Map;

/**
 * Default production implementation of {@link RelationshipCountCompactor} which compacts relationship counts based
 * on a threshold.
 * <p/>
 * More specifically, if there is a single more general relationship representation (see {@link com.graphaware.neo4j.relcount.representation.PartiallyComparableByGenerality} for more info)
 * that would cover more than {@link #DEFAULT_COMPACTION_THRESHOLD} cached relationship counts, these relationship counts are compacted into the
 * general form.
 */
public class ThresholdBasedRelationshipCountCompactor implements RelationshipCountCompactor {

    private static final int DEFAULT_COMPACTION_THRESHOLD = 10;

    private final int compactionThreshold;
    private final RelationshipCountManager countManager;

    /**
     * Construct a new compactor with default compaction threshold.
     *
     * @param countManager manager.
     */
    public ThresholdBasedRelationshipCountCompactor(RelationshipCountManager countManager) {
        this(DEFAULT_COMPACTION_THRESHOLD, countManager);
    }

    /**
     * Construct a new compactor.
     *
     * @param compactionThreshold compaction threshold, see class javadoc.
     * @param countManager        manager.
     */
    public ThresholdBasedRelationshipCountCompactor(int compactionThreshold, RelationshipCountManager countManager) {
        this.compactionThreshold = compactionThreshold;
        this.countManager = countManager;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void compactRelationshipCounts(ComparableRelationship trigger, Node node) {

        Map<ComparableRelationship, Integer> cachedCounts = countManager.getRelationshipCounts(node);

        //if this is not the most concrete relationship, perform no compaction
        for (ComparableRelationship cachedCount : cachedCounts.keySet()) {
            if (cachedCount.isStrictlyMoreSpecificThan(trigger)) {
                return;
            }
        }

        for (ComparableRelationship generalization : trigger.generateOneMoreGeneral()) {
            Map<ComparableRelationship, Integer> compactionCandidates = new HashMap<ComparableRelationship, Integer>();
            for (Map.Entry<ComparableRelationship, Integer> compactionCandidate : cachedCounts.entrySet()) {
                if (generalization.isStrictlyMoreGeneralThan(compactionCandidate.getKey())) {
                    compactionCandidates.put(compactionCandidate.getKey(), compactionCandidate.getValue());
                }
            }

            if (compactionCandidates.size() > compactionThreshold) {
                for (Map.Entry<ComparableRelationship, Integer> soonToBeGone : compactionCandidates.entrySet()) {
                    countManager.incrementCount(generalization, node, soonToBeGone.getValue());
                    countManager.deleteCount(soonToBeGone.getKey(), node);
                }

                compactRelationshipCounts(generalization, node);
            }
        }
    }
}
