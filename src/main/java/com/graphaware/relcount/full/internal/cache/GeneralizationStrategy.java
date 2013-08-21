package com.graphaware.relcount.full.internal.cache;

import com.graphaware.relcount.full.internal.dto.relationship.CacheableRelationshipDescription;

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
    List<CacheableRelationshipDescription> produceGeneralizations(Map<CacheableRelationshipDescription, Integer> cachedCounts);
}
