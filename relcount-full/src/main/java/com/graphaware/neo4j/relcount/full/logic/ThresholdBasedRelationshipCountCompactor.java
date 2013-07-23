package com.graphaware.neo4j.relcount.full.logic;

import com.graphaware.neo4j.relcount.full.dto.relationship.CompactibleRelationship;
import org.apache.log4j.Logger;
import org.neo4j.graphdb.Node;

import java.util.*;

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
        Map<CompactibleRelationship, Integer> cachedCounts = countCache.getRelationshipCounts(node);

        //Not above the threshold => no need for compaction
        if (cachedCounts.size() < compactionThreshold) {
            return true;
        }

        //Generate all possible generalizations
        Set<CompactibleRelationship> generalizations = new TreeSet<>();
        for (CompactibleRelationship cached : cachedCounts.keySet()) {
            generalizations.addAll(cached.generateAllMoreGeneral());
        }

        //Find the most specific generalization that has a chance to result in the best compaction
        Set<CompactibleRelationship> bestCandidates = Collections.emptySet();
        CompactibleRelationship bestGeneralization = null;

        for (CompactibleRelationship generalization : generalizations) {
            if (bestGeneralization != null && generalization.isMoreGeneralThan(bestGeneralization)) {
                break; //already reached a generalization that is not the
            }

            Set<CompactibleRelationship> candidates = new HashSet<>();
            for (CompactibleRelationship potentialCandidate : cachedCounts.keySet()) {
                if (generalization.isMoreGeneralThan(potentialCandidate)) {
                    candidates.add(potentialCandidate);
                }
            }

            if (candidates.size() > 1 && candidates.size() > bestCandidates.size()) {
                bestCandidates = candidates;
                bestGeneralization = generalization;
            }
        }

        //See if the generalization will result in compaction
        if (bestCandidates.size() > 1) {

            //It will, do it!
            int candidateCachedCount = 0;
            for (CompactibleRelationship candidate : bestCandidates) {
                candidateCachedCount += cachedCounts.get(candidate);
                countCache.deleteCount(candidate, node);
            }

            countCache.incrementCount(bestGeneralization, node, candidateCachedCount);

            //After the compaction, see if more is needed using a recursive call
            performCompaction(node);
        }

        //If we reached this point and haven't compacted enough, we can't!
        return countCache.getRelationshipCounts(node).size() < compactionThreshold;
    }
}
