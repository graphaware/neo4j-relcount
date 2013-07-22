package com.graphaware.neo4j.relcount.full.logic;

import com.graphaware.neo4j.relcount.full.dto.relationship.RelationshipDescription;
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

    private final int compactionThreshold;
    private final FullRelationshipCountCache countCache;

    /**
     * Construct a new compactor.
     *
     * @param compactionThreshold compaction threshold, see class javadoc.
     * @param countCache          relationship count cache.
     */
    public ThresholdBasedRelationshipCountCompactor(int compactionThreshold, FullRelationshipCountCache countCache) {
        this.compactionThreshold = compactionThreshold;
        this.countCache = countCache;
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
        Map<RelationshipDescription, Integer> cachedCounts = countCache.getRelationshipCounts(node);

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
                    countCache.deleteCount(candidate, node);
                }

                countCache.incrementCount(generalization, node, candidateCachedCount);

                //After the compaction, see if more is needed using a recursive call
                performCompaction(node);

                //Break the for loop, other generalizations (if needed) will be found using recursion above.
                break;
            }
        }

        //If we reached this point and haven't compacted enough, we can't!
        return countCache.getRelationshipCounts(node).size() < compactionThreshold;
    }
}
