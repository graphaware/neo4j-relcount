package com.graphaware.neo4j.relcount.common.internal.cache;

import com.graphaware.neo4j.dto.common.relationship.SerializableTypeAndDirection;
import org.neo4j.graphdb.Node;

import java.util.HashMap;
import java.util.Map;

/**
 * Base-class for {@link BatchFriendlyRelationshipCountCache} implementations. It keeps track of nodes' properties
 * stored in {@link ThreadLocal} and only flushes them to the database when the batch ends. This improves performance
 * significantly when many relationships are being created for a single node in the same transaction.
 */
public abstract class BaseBatchFriendlyRelationshipCountCache<CACHED extends SerializableTypeAndDirection> extends BaseRelationshipCountCache<CACHED> {

    private static final ThreadLocal<Map<Long, BatchFriendlyNode>> nodeCache = new ThreadLocal<>();

    /**
     * Construct a new cache.
     *
     * @param id of the module this cache is for.
     */
    protected BaseBatchFriendlyRelationshipCountCache(String id) {
        super(id);
    }

    /**
     * @see {@link BatchFriendlyRelationshipCountCache#startBatchMode()}
     */
    public void startBatchMode() {
        if (nodeCache.get() != null) {
            throw new IllegalStateException("Previous batch hasn't been ended!");
        }

        nodeCache.set(new HashMap<Long, BatchFriendlyNode>());
    }

    /**
     * @see {@link BatchFriendlyRelationshipCountCache#endBatchMode()}
     */
    public void endBatchMode() {
        if (nodeCache.get() == null) {
            throw new IllegalStateException("No batch has been started!");
        }

        for (BatchFriendlyNode batchFriendlyNode : nodeCache.get().values()) {
            batchFriendlyNode.flush();
        }

        nodeCache.set(null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Node unwrap(Node node) {
        Node rawNode = super.unwrap(node);

        //no batch started
        Map<Long, BatchFriendlyNode> fakeNodes = nodeCache.get();
        if (fakeNodes == null) {
            return rawNode;
        }

        //batch in progress
        if (!fakeNodes.containsKey(rawNode.getId())) {
            fakeNodes.put(rawNode.getId(), new BatchFriendlyNode(rawNode));
        }

        return fakeNodes.get(rawNode.getId());
    }
}
