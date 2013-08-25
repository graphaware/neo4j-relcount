package com.graphaware.relcount.full.internal.node;

import com.graphaware.relcount.common.internal.node.BaseRelationshipCountCachingNode;
import com.graphaware.relcount.common.internal.node.RelationshipCountCachingNode;
import com.graphaware.relcount.full.internal.dto.relationship.CacheableRelationshipDescription;
import com.graphaware.relcount.full.internal.dto.relationship.CacheableRelationshipDescriptionImpl;
import org.neo4j.graphdb.Node;

/**
 * {@link RelationshipCountCachingNode} that caches relationship counts with their "full" details, i.e. {@link org.neo4j.graphdb.RelationshipType},
 * {@link org.neo4j.graphdb.Direction}, and properties.
 */
public class FullRelationshipCountCachingNode extends BaseRelationshipCountCachingNode<CacheableRelationshipDescription> implements RelationshipCountCachingNode<CacheableRelationshipDescription> {

    private final RelationshipCountCompactor relationshipCountCompactor;

    /**
     * Construct a new caching node.
     *
     * @param node                       backing Neo4j node.
     * @param prefix                     of the cached relationship string representation.
     * @param separator                  of information in the cached relationship string representation.
     * @param relationshipCountCompactor relationship count compactor.
     */
    public FullRelationshipCountCachingNode(Node node, String prefix, String separator, RelationshipCountCompactor relationshipCountCompactor) {
        super(node, prefix, separator);
        this.relationshipCountCompactor = relationshipCountCompactor;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected CacheableRelationshipDescription newCachedRelationship(String string) {
        return new CacheableRelationshipDescriptionImpl(string, prefix, separator);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean cachedMatch(CacheableRelationshipDescription cached, CacheableRelationshipDescription relationship) {
        return cached.isMoreGeneralThan(relationship);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void propertyCreated() {
        relationshipCountCompactor.compactRelationshipCounts(this);
    }
}
