package com.graphaware.neo4j.relcount.full.internal.cache;

import com.graphaware.neo4j.relcount.full.internal.dto.relationship.CompactibleRelationship;

import java.util.List;
import java.util.Map;

/**
 * A strategy for producing generalizations of cached counts.
 */
public interface GeneralizationStrategy {

    /**
     * Produce all possible generalizations of the cached counts, sorted from best to worst. Implementations should
     * determine what "best" means.
     *
     * @param cachedCounts cached counts that need to be compacted.
     * @return best-to-worst sorted generalizations.
     */
    List<CompactibleRelationship> produceGeneralizations(Map<CompactibleRelationship, Integer> cachedCounts);
}
