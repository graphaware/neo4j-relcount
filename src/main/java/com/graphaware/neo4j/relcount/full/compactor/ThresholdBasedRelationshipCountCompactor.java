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

package com.graphaware.neo4j.relcount.full.compactor;

import com.graphaware.neo4j.relcount.full.dto.relationship.RelationshipDescription;
import com.graphaware.neo4j.relcount.full.manager.FullCachingRelationshipCountManager;
import org.apache.log4j.Logger;
import org.neo4j.graphdb.Node;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Default production implementation of {@link RelationshipCountCompactor} which compacts relationship counts based
 * on a threshold.
 * <p/>
 * More specifically, if there are more than {@link #compactionThreshold} distinct cached relationship counts,
 * least general generalizations (or equivalently most specific generalizations) are created until the number of cached
 * relationship counts is below the threshold again. If unable to reach such state, a meaningful message is logged.
 */
public class ThresholdBasedRelationshipCountCompactor implements RelationshipCountCompactor {
    private static final Logger LOG = Logger.getLogger(ThresholdBasedRelationshipCountCompactor.class);

    private static final int DEFAULT_COMPACTION_THRESHOLD = 20;

    private final int compactionThreshold;
    private final FullCachingRelationshipCountManager countManager;

    /**
     * Construct a new compactor with default compaction threshold.
     *
     * @param countManager manager.
     */
    public ThresholdBasedRelationshipCountCompactor(FullCachingRelationshipCountManager countManager) {
        this(DEFAULT_COMPACTION_THRESHOLD, countManager);
    }

    /**
     * Construct a new compactor.
     *
     * @param compactionThreshold compaction threshold, see class javadoc.
     * @param countManager        manager.
     */
    public ThresholdBasedRelationshipCountCompactor(int compactionThreshold, FullCachingRelationshipCountManager countManager) {
        this.compactionThreshold = compactionThreshold;
        this.countManager = countManager;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void compactRelationshipCounts(Node node) {
        if (!performCompaction(node)) {
            LOG.warn("Compactor could not reach the desired threshold (" + compactionThreshold + ") " +
                    "on node " + node.getId() + ". This is potentially due to the fact that there are more than " + compactionThreshold +
                    " distinct relationship type - direction pairs being cached for the node. If that's what's desired," +
                    " increase the threshold. If not, implement a RelationshipInclusionStrategy and that does not include" +
                    " the unwanted relationships.");
        }
    }

    private boolean performCompaction(Node node) {
        Map<RelationshipDescription, Integer> cachedCounts = countManager.getRelationshipCounts(node);

        //Not above the threshold => no need for compaction
        if (cachedCounts.size() < compactionThreshold) {
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
                    countManager.deleteCount(candidate, node);
                }

                countManager.incrementCount(generalization, node, candidateCachedCount);

                //After the compaction, see if more is needed using a recursive call
                performCompaction(node);

                //Break the for loop, other generalizations (if needed) will be found using recursion above.
                break;
            }
        }

        //If we reached this point and haven't compacted enough, we can't!
        return countManager.getRelationshipCounts(node).size() < compactionThreshold;
    }
}
