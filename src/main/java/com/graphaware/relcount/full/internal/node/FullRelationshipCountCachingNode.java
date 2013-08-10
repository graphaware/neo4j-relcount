package com.graphaware.relcount.full.internal.node;

import com.graphaware.relcount.common.internal.node.BaseRelationshipCountCachingNode;
import com.graphaware.relcount.common.internal.node.RelationshipCountCachingNode;
import com.graphaware.relcount.full.internal.cache.RelationshipCountCompactor;
import com.graphaware.relcount.full.internal.dto.relationship.CompactibleRelationship;
import com.graphaware.relcount.full.internal.dto.relationship.CompactibleRelationshipImpl;
import org.neo4j.graphdb.Node;

/**
 * {@link RelationshipCountCachingNode} that caches relationship counts with their "full" details, i.e. {@link org.neo4j.graphdb.RelationshipType},
 * {@link org.neo4j.graphdb.Direction}, and properties.
 */
public class FullRelationshipCountCachingNode extends BaseRelationshipCountCachingNode<CompactibleRelationship> implements RelationshipCountCachingNode<CompactibleRelationship> {

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
    protected CompactibleRelationship newCachedRelationship(String string) {
        return new CompactibleRelationshipImpl(string, prefix, separator);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean cachedMatch(CompactibleRelationship cached, CompactibleRelationship relationship) {
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
