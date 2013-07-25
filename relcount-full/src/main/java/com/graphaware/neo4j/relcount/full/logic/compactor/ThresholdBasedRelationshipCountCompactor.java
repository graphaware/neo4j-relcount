package com.graphaware.neo4j.relcount.full.logic.compactor;

import com.graphaware.neo4j.relcount.full.dto.relationship.CompactibleRelationship;
import com.graphaware.neo4j.relcount.full.logic.FullRelationshipCountCache;
import org.apache.log4j.Logger;
import org.neo4j.graphdb.Node;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Default production implementation of {@link RelationshipCountCompactor} which compacts relationship counts based
 * on a threshold.
 * <p/>
 * More specifically, if there are more than {@link #compactionThreshold} distinct cached relationship counts,
 * generalizations are created according to a {@link GeneralizationStrategy} until the number of cached
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
        Map<CompactibleRelationship, Integer> cachedCounts = countCache.getRelationshipCounts(node);

        //Not above the threshold => no need for compaction
        if (cachedCounts.size() < compactionThreshold) {
            return true;
        }

        for (CompactibleRelationship generalization : new AverageCardinalityGeneralizationStrategy().produceGeneralizations(cachedCounts)) {

            //Find all the candidates to be eliminated by the generalization
            Set<CompactibleRelationship> candidates = new HashSet<>();
            for (CompactibleRelationship potentialCandidate : cachedCounts.keySet()) {
                if (generalization.isMoreGeneralThan(potentialCandidate)) {
                    candidates.add(potentialCandidate);
                }
            }

            //See if the generalization will result in compaction
            if (candidates.size() > 1) {

                //It will, do it!
                int candidateCachedCount = 0;
                for (CompactibleRelationship candidate : candidates) {
                    candidateCachedCount += cachedCounts.get(candidate);
                    countCache.deleteCount(candidate, node);
                }

                countCache.incrementCount(generalization, node, candidateCachedCount);

                //After the compaction, see if more is needed using a recursive call
                performCompaction(node);

                break;
            }
        }

        //If we reached this point and haven't compacted enough, we can't!
        return countCache.getRelationshipCounts(node).size() < compactionThreshold;
    }
}
