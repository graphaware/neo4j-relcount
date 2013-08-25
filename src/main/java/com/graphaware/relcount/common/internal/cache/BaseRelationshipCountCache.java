package com.graphaware.relcount.common.internal.cache;

import com.graphaware.framework.config.BaseFrameworkConfigured;
import com.graphaware.propertycontainer.dto.string.relationship.SerializableTypeAndDirection;
import com.graphaware.propertycontainer.wrapper.NodeWrapper;
import com.graphaware.relcount.common.internal.node.RelationshipCountCachingNode;
import org.apache.log4j.Logger;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.HashMap;
import java.util.Map;

/**
 * Base-class for {@link RelationshipCountCache} implementations that cache relationship counts as properties on {@link Node}s.
 *
 * @param <CACHED> type of relationship representation that can be used as a cached relationship description.
 *                 Must be {@link Comparable}; the resulting order is essential for determining, which representation
 *                 corresponds to an about-to-be-cached relationship count.
 */
public abstract class BaseRelationshipCountCache<CACHED extends SerializableTypeAndDirection> extends BaseFrameworkConfigured {
    private static final Logger LOG = Logger.getLogger(BaseRelationshipCountCache.class);

    protected final String id;

    /**
     * Construct a new cache.
     *
     * @param id of the module this cache is for.
     */
    protected BaseRelationshipCountCache(String id) {
        this.id = id;
    }

    /**
     * @see {@link RelationshipCountCache#startCaching()}
     */
    public void startCaching() {
        if (getNodeCache().get() != null) {
            throw new IllegalStateException("Previous caching hasn't been ended!");
        }

        getNodeCache().set(new HashMap<Long, RelationshipCountCachingNode<CACHED>>());
    }

    /**
     * @see {@link RelationshipCountCache#endCaching()}
     */
    public void endCaching() {
        ThreadLocal<Map<Long, RelationshipCountCachingNode<CACHED>>> nodeCache = getNodeCache();

        if (nodeCache.get() == null) {
            throw new IllegalStateException("No caching has been started!");
        }

        for (RelationshipCountCachingNode<?> node : nodeCache.get().values()) {
            node.flush();
        }

        nodeCache.set(null);
    }

    /**
     * Get node cache from subclass.
     *
     * @return node cache.
     */
    protected abstract ThreadLocal<Map<Long, RelationshipCountCachingNode<CACHED>>> getNodeCache();

    /**
     * @see {@link RelationshipCountCache#handleCreatedRelationship(org.neo4j.graphdb.Relationship, org.neo4j.graphdb.Node, org.neo4j.graphdb.Direction)}
     */
    public final void handleCreatedRelationship(Relationship relationship, Node pointOfView, Direction defaultDirection) {
        throwExceptionIfDirectionIsNullOrBoth(defaultDirection);

        CACHED createdRelationship = newCachedRelationship(relationship, pointOfView, defaultDirection);
        int relationshipWeight = relationshipWeight(relationship, pointOfView);

        RelationshipCountCachingNode<CACHED> cachingNode = cachingNode(unwrap(pointOfView));
        cachingNode.incrementCount(createdRelationship, relationshipWeight);
    }


    /**
     * @see {@link RelationshipCountCache#handleDeletedRelationship(org.neo4j.graphdb.Relationship, org.neo4j.graphdb.Node, org.neo4j.graphdb.Direction)}
     */
    public final void handleDeletedRelationship(Relationship relationship, Node pointOfView, Direction defaultDirection) {
        throwExceptionIfDirectionIsNullOrBoth(defaultDirection);

        CACHED deletedRelationship = newCachedRelationship(relationship, pointOfView, defaultDirection);
        int relationshipWeight = relationshipWeight(relationship, pointOfView);

        RelationshipCountCachingNode<CACHED> cachingNode = cachingNode(unwrap(pointOfView));
        cachingNode.decrementCount(deletedRelationship, relationshipWeight);
    }

    /**
     * Create a cached relationship representation from a Neo4j relationship.
     *
     * @param relationship     to create a cached representation from.
     * @param pointOfView      node whose point of view the relationship is being looked at.
     * @param defaultDirection in case the relationship direction would be resolved to {@link org.neo4j.graphdb.Direction#BOTH}, what
     *                         should it actually be resolved to? This is guaranteed to be {@link org.neo4j.graphdb.Direction#OUTGOING} or {@link org.neo4j.graphdb.Direction#INCOMING},
     *                         never {@link org.neo4j.graphdb.Direction#BOTH}.
     * @return representation of the cached relationship.
     */
    protected abstract CACHED newCachedRelationship(Relationship relationship, Node pointOfView, Direction defaultDirection);

    /**
     * Get the weight of a relationship, i.e. how much it counts for?
     *
     * @param relationship to get weight for.
     * @param pointOfView  node whose point of view the relationship is being looked at.
     * @return relationship weight, 1 by default, can be overridden by subclasses. Should be positive.
     */
    protected int relationshipWeight(Relationship relationship, Node pointOfView) {
        return 1;
    }

    /**
     * Get an instance of caching node from cache, or create a new one and put it in cache.
     *
     * @param node for which a caching node should be obtained.
     * @return caching node.
     */
    private RelationshipCountCachingNode<CACHED> cachingNode(Node node) {
        Map<Long, RelationshipCountCachingNode<CACHED>> nodeCache = getNodeCache().get();

        if (nodeCache == null) {
            throw new IllegalStateException("No caching has been started!");
        }

        if (!nodeCache.containsKey(node.getId())) {
            nodeCache.put(node.getId(), newCachingNode(node));
        }

        return nodeCache.get(node.getId());
    }

    /**
     * Create a caching node from an unfiltered Neo4j node.
     *
     * @param node to wrap / represent.
     * @return caching node.
     */
    protected abstract RelationshipCountCachingNode<CACHED> newCachingNode(Node node);

    /**
     * Unwrap a potentially decorated Neo4j node.
     *
     * @param node to unwrap.
     * @return node with no decorators around it.
     */
    private Node unwrap(Node node) {
        if (node instanceof NodeWrapper) {
            return ((NodeWrapper) node).getWrapped();
        }

        LOG.warn("Unwrapping a non-wrapper node...");
        return node.getGraphDatabase().getNodeById(node.getId());
    }

    /**
     * Check that the given direction is not null or {@link Direction#BOTH} and throw an exception if it is.
     *
     * @param direction to check.
     * @throws IllegalArgumentException in case direction is null or {@link Direction#BOTH}.
     */
    private void throwExceptionIfDirectionIsNullOrBoth(Direction direction) {
        if (direction == null || direction.equals(Direction.BOTH)) {
            throw new IllegalArgumentException("Default direction must not be null or BOTH. This is a bug.");
        }
    }
}
