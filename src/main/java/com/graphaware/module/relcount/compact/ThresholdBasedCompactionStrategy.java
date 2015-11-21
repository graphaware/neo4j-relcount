/*
 * Copyright (c) 2013-2015 GraphAware
 *
 * This file is part of the GraphAware Framework.
 *
 * GraphAware Framework is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details. You should have received a copy of
 * the GNU General Public License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

package com.graphaware.module.relcount.compact;

import com.graphaware.common.description.relationship.DetachedRelationshipDescription;
import com.graphaware.module.relcount.cache.DegreeCachingNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * {@link CompactionStrategy} which compacts degrees based the number of cached degrees and a threshold.
 * <p/>
 * More specifically, if there are more than {@link #compactionThreshold} distinct cached degrees (i.e. degrees with
 * respect to distinct relationship descriptions generalizations are created using a {@link GeneralizationStrategy}
 * until the number of cached degrees is below the threshold again. If unable to reach such state, a meaningful message
 * is logged.
 */
public class ThresholdBasedCompactionStrategy implements CompactionStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(ThresholdBasedCompactionStrategy.class);

    private final int compactionThreshold;
    private final GeneralizationStrategy generalizationStrategy;

    /**
     * Construct a new compaction strategy with default {@link GeneralizationStrategy}, which is
     * {@link GeneralizeFrequentlyChanging}.
     *
     * @param compactionThreshold compaction threshold.
     */
    public ThresholdBasedCompactionStrategy(int compactionThreshold) {
        this(compactionThreshold, new GeneralizeFrequentlyChanging());
    }

    /**
     * Construct a new compaction strategy.
     *
     * @param compactionThreshold    compaction threshold.
     * @param generalizationStrategy generalization strategy.
     */
    public ThresholdBasedCompactionStrategy(int compactionThreshold, GeneralizationStrategy generalizationStrategy) {
        this.compactionThreshold = compactionThreshold;
        this.generalizationStrategy = generalizationStrategy;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void compactRelationshipCounts(DegreeCachingNode node) {
        if (!performCompaction(node)) {
            LOG.warn("The desired threshold (" + compactionThreshold + ") could not be achieved using the current compaction strategy " +
                    "on node " + node.getId() + ". This is potentially due to the fact that there are more than " + compactionThreshold +
                    " distinct relationship type - direction pairs being cached for the node. If that's what's desired," +
                    " increase the threshold. If not, implement a RelationshipInclusionPolicy that does not include" +
                    " the unwanted relationships.");
        }
    }

    private boolean performCompaction(DegreeCachingNode node) {
        Map<DetachedRelationshipDescription, Integer> cachedDegrees = node.getCachedDegrees();

        //Not above the threshold => no need for compaction
        if (cachedDegrees.size() <= compactionThreshold) {
            return true;
        }

        //Not suitable generalization => bad luck
        DetachedRelationshipDescription generalization = generalizationStrategy.produceGeneralization(cachedDegrees);
        if (generalization == null) {
            return false;
        }

        //Find all the candidates to be eliminated by the generalization
        Set<DetachedRelationshipDescription> candidates = new HashSet<>();
        for (DetachedRelationshipDescription potentialCandidate : cachedDegrees.keySet()) {
            if (generalization.isMoreGeneralThan(potentialCandidate)) {
                candidates.add(potentialCandidate);
            }
        }

        int candidateCachedCount = 0;
        for (DetachedRelationshipDescription candidate : candidates) {
            int count = cachedDegrees.get(candidate);
            candidateCachedCount += count;
            node.decrementDegree(candidate, count);
        }

        node.incrementDegree(generalization, candidateCachedCount, true);

        //enough? => return, otherwise try again
        return cachedDegrees.size() <= compactionThreshold || performCompaction(node);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ThresholdBasedCompactionStrategy that = (ThresholdBasedCompactionStrategy) o;

        if (compactionThreshold != that.compactionThreshold) return false;
        if (!generalizationStrategy.equals(that.generalizationStrategy)) return false;

        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        int result = compactionThreshold;
        result = 31 * result + generalizationStrategy.hashCode();
        return result;
    }
}
