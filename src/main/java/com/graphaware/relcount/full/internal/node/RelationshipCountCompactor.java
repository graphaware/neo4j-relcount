package com.graphaware.relcount.full.internal.node;

import com.graphaware.relcount.common.internal.node.RelationshipCountCachingNode;
import com.graphaware.relcount.full.internal.dto.relationship.CacheableRelationshipDescription;

/**
 * Component responsible for compacting the relationship counts cached as properties on nodes.
 * <p/>
 * For example, a node might have the following relationship counts:
 * - FRIEND_OF#OUTGOING#timestamp#5.4.2013 - 1x
 * - FRIEND_OF#OUTGOING#timestamp#6.4.2013 - 3x
 * - FRIEND_OF#OUTGOING#timestamp#7.4.2013 - 5x
 * <p/>
 * The compactor might decide to compact this into:
 * - FRIEND_OF#OUTGOING#timestamp#_GA_* - 9x
 */
public interface RelationshipCountCompactor {

    /**
     * Compact relationship counts if needed.
     * <p/>
     * Note: assumes a transaction is running.
     *
     * @param node to compact cached relationship counts on.
     */
    void compactRelationshipCounts(RelationshipCountCachingNode<CacheableRelationshipDescription> node);
}
