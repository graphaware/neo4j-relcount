package com.graphaware.relcount.common.internal.node;

import com.graphaware.framework.NeedsInitializationException;
import com.graphaware.propertycontainer.dto.string.relationship.SerializableTypeAndDirection;
import org.apache.log4j.Logger;
import org.neo4j.graphdb.Node;

import java.util.*;

/**
 * Base-class for {@link BaseRelationshipCountCachingNode} implementations. For efficiency, it keeps track of all the
 * cached counts changes and only writes them to the underlying {@link Node} once when {@link #flush()} is called.
 */
public abstract class BaseRelationshipCountCachingNode<CACHED extends SerializableTypeAndDirection> {

    private static final Logger LOG = Logger.getLogger(BaseRelationshipCountCachingNode.class);

    protected final Node node;
    protected final String prefix;
    protected final String separator;

    private final Map<CACHED, Integer> cachedCounts;
    private final Set<CACHED> updatedCounts;
    private final Set<CACHED> removedCounts;

    /**
     * Construct a new caching node.
     *
     * @param node      backing Neo4j node.
     * @param prefix    of the cached relationship string representation.
     * @param separator of information in the cached relationship string representation.
     */
    protected BaseRelationshipCountCachingNode(Node node, String prefix, String separator) {
        this.node = node;
        this.prefix = prefix;
        this.separator = separator;

        cachedCounts = new HashMap<>();

        for (String key : node.getPropertyKeys()) {
            if (key.startsWith(prefix)) {
                cachedCounts.put(newCachedRelationship(key), (Integer) node.getProperty(key));
            }
        }

        updatedCounts = new HashSet<>();
        removedCounts = new HashSet<>();
    }

    /**
     * Create a cached relationship representation from a String representation of the cached relationship, coming from
     * a node's property key.
     *
     * @param string string representation of the cached relationship.
     * @return object representation of the cached relationship.
     */
    protected abstract CACHED newCachedRelationship(String string);

    /**
     * @see {@link RelationshipCountCachingNode#getId()}
     */
    public long getId() {
        return node.getId();
    }

    /**
     * @see {@link RelationshipCountCachingNode#getCachedCounts()}
     */
    public Map<CACHED, Integer> getCachedCounts() {
        return Collections.unmodifiableMap(cachedCounts);
    }

    /**
     * @see {@link RelationshipCountCachingNode#incrementCount(com.graphaware.propertycontainer.dto.common.relationship.HasTypeAndDirection, int)}
     */
    public final void incrementCount(CACHED relationship, int delta) {
        for (CACHED cachedRelationship : cachedCounts.keySet()) {
            if (cachedMatch(cachedRelationship, relationship)) {
                int newValue = cachedCounts.get(cachedRelationship) + delta;
                put(cachedRelationship, newValue);
                return;
            }
        }

        put(relationship, delta);
        propertyCreated();
    }

    /**
     * Allow subclasses to react to the fact that a new internal property has been created on a node.
     */
    protected void propertyCreated() {
        //allow subclasses to react
    }

    /**
     * @see {@link RelationshipCountCachingNode#decrementCount(com.graphaware.propertycontainer.dto.common.relationship.HasTypeAndDirection, int)}
     */
    public final void decrementCount(CACHED relationship, int delta) {
        for (CACHED cachedRelationship : cachedCounts.keySet()) {
            if (cachedMatch(cachedRelationship, relationship)) {
                int newValue = cachedCounts.get(cachedRelationship) - delta;
                put(cachedRelationship, newValue);

                if (newValue <= 0) {
                    delete(cachedRelationship);
                }

                if (newValue < 0) {
                    LOG.warn(cachedRelationship.toString() + " was out of sync on node " + node.getId());
                    throw new NeedsInitializationException(cachedRelationship.toString() + " was out of sync on node " + node.getId());
                }

                return;
            }
        }

        LOG.warn(relationship.toString() + " was not present on node " + node.getId());
        throw new NeedsInitializationException(relationship.toString() + " was not present on node " + node.getId());
    }

    /**
     * Should the cached relationship be affected (incremented, decremented) by the given relationship representation?
     *
     * @param cached       cached relationship that could correspond to the given relationship being updated.
     * @param relationship representation being updated.
     * @return true iff the cached relationship is to be treated as the about-to-be-updated relationship's representation.
     */
    protected abstract boolean cachedMatch(CACHED cached, CACHED relationship);

    /**
     * Update the value (count) of a cached relationship.
     *
     * @param cachedRelationship to update.
     * @param value              new value.
     */
    private void put(CACHED cachedRelationship, int value) {
        cachedCounts.put(cachedRelationship, value);
        updatedCounts.add(cachedRelationship);
        removedCounts.remove(cachedRelationship);
    }

    /**
     * Delete a cached relationship.
     *
     * @param cachedRelationship to update.
     */
    private void delete(CACHED cachedRelationship) {
        cachedCounts.remove(cachedRelationship);
        updatedCounts.remove(cachedRelationship);
        removedCounts.add(cachedRelationship);
    }

    /**
     * @see {@link com.graphaware.relcount.common.internal.node.RelationshipCountCachingNode#flush()}.
     */
    public void flush() {
        for (CACHED updated : updatedCounts) {
            node.setProperty(updated.toString(prefix, separator), cachedCounts.get(updated));
        }

        for (CACHED removed : removedCounts) {
            node.removeProperty(removed.toString(prefix, separator));
        }
    }
}
