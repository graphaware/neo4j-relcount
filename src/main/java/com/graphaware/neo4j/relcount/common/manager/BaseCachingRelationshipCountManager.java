package com.graphaware.neo4j.relcount.common.manager;

import com.graphaware.neo4j.dto.common.relationship.HasDirectionAndType;
import org.neo4j.graphdb.Node;

import java.util.Map;
import java.util.TreeMap;

import static com.graphaware.neo4j.common.Constants.GA_REL_PREFIX;

/**
 * Base class for {@link CachingRelationshipCountManager} implementations.
 *
 * @param <T> type of relationship representation that can be used to query relationship counts on nodes.
 * @param <C> type of the (typically string-convertible) object representation of the cached relationship.
 */
public abstract class BaseCachingRelationshipCountManager<T extends HasDirectionAndType, C extends T> {

    /**
     * Get a relationship count cached on a node. The count is the sum of all the cached counts that {@link #shouldBeUsedForLookup(com.graphaware.neo4j.dto.common.relationship.HasDirectionAndType, com.graphaware.neo4j.dto.common.relationship.HasDirectionAndType)},
     * unless {@link #continueAfterFirstLookupMatch()} returns <code>false</code>, in which case it is just the first
     * matching value found.
     *
     * @param relationship representation of the relationship for which to get count.
     * @param node         from which to get cached relationship counts.
     * @return cached count, 0 if there is none.
     */
    public int getRelationshipCount(T relationship, Node node) {
        int result = 0;
        for (Map.Entry<C, Integer> cachedRelationshipCount : getRelationshipCounts(node).entrySet()) {
            if (shouldBeUsedForLookup(cachedRelationshipCount.getKey(), relationship)) {
                result += cachedRelationshipCount.getValue();
                if (!continueAfterFirstLookupMatch()) {
                    return result;
                }
            }
        }
        return result;
    }

    /**
     * Should the cached relationship be used as a representation of the given relationship being looked up?
     *
     * @param cached       candidate relationship that could correspond to the given relationship being looked up.
     * @param relationship being looked up.
     * @return true iff the cached relationship is to be treated as the looked up relationship's representation.
     */
    protected abstract boolean shouldBeUsedForLookup(C cached, T relationship);

    /**
     * When looking up a cached count for a relationship, should one continue looking after the first match has been found?
     *
     * @return true for lookup continuation, false to return the first cached count.
     */
    protected abstract boolean continueAfterFirstLookupMatch();

    /**
     * Get all relationship counts cached on a node. No aggregation is performed, this is the raw data as stored
     * (as opposed to {@link #getRelationshipCount(com.graphaware.neo4j.dto.common.relationship.HasDirectionAndType, org.neo4j.graphdb.Node)}).
     *
     * @param node from which to get cached relationship counts.
     * @return cached relationship counts (key = relationship representation, value = count). The map is sorted
     *         so that it is iterated in a relationship specific to general order.
     */
    public Map<C, Integer> getRelationshipCounts(Node node) {
        Map<C, Integer> result = new TreeMap<>();
        for (String key : node.getPropertyKeys()) {
            if (key.startsWith(GA_REL_PREFIX)) {
                result.put(newCachedRelationship(key), (Integer) node.getProperty(key));
            }
        }
        return result;
    }

    /**
     * Create a cached relationship representation from a String representation of the cached relationship.
     *
     * @param key string representation of the cached relationship.
     * @return object representation of the cached relationship.
     */
    protected abstract C newCachedRelationship(String key);

    /**
     * Increment the cached relationship count on the given node by 1.
     *
     * @param relationship representation of a relationship.
     * @param node         on which to increment the cached relationship count.
     * @return true iff the cached value did not exist and had to be created.
     */
    public boolean incrementCount(C relationship, Node node) {
        return incrementCount(relationship, node, 1);
    }

    /**
     * Increment the cached relationship count on the given node by delta.
     *
     * @param relationship representation of a relationship.
     * @param node         on which to increment the cached relationship count.
     * @param delta        increment.
     * @return true iff the cached value did not exist and had to be created.
     */
    public boolean incrementCount(C relationship, Node node, int delta) {
        for (C cachedRelationship : getRelationshipCounts(node).keySet()) {
            if (shouldBeUsedForCaching(cachedRelationship, relationship)) {
                node.setProperty(cachedRelationship.toString(), (Integer) node.getProperty(cachedRelationship.toString()) + delta);
                return false;
            }
        }

        node.setProperty(relationship.toString(), delta);
        return true;

    }

    /**
     * Decrement the cached relationship count on the given node by 1.
     * Delete the cached relationship count if the count is 0 after the decrement.
     *
     * @param relationship representation of a relationship.
     * @param node         on which to decrement the cached relationship count.
     * @return true iff the cached value existed and was >= 1.
     */
    public boolean decrementCount(C relationship, Node node) {
        return decrementCount(relationship, node, 1);
    }

    /**
     * Decrement the cached relationship count on the given node by delta.
     * Delete the cached relationship count if the count is 0 after the decrement.
     *
     * @param relationship representation of a relationship.
     * @param node         on which to decrement the cached relationship count.
     * @param delta        increment.
     * @return true iff the cached value existed and was >= delta.
     */
    public boolean decrementCount(C relationship, Node node, int delta) {
        for (C cachedRelationship : getRelationshipCounts(node).keySet()) {
            if (shouldBeUsedForCaching(cachedRelationship, relationship)) {
                int newValue = (Integer) node.getProperty(cachedRelationship.toString()) - delta;
                node.setProperty(cachedRelationship.toString(), newValue);

                if (newValue <= 0) {
                    deleteCount(cachedRelationship, node);
                }

                return newValue >= 0;
            }
        }

        return false;
    }

    /**
     * Should the cached relationship be used as a representation of the given relationship being updated (incremented, decremented)?
     *
     * @param cached       candidate relationship that could correspond to the given relationship being updated.
     * @param relationship being updated.
     * @return true iff the cached relationship is to be treated as the about-to-be-stored relationship's representation.
     */
    protected abstract boolean shouldBeUsedForCaching(C cached, T relationship);

    /**
     * Stop tracking relationship count for a node.
     *
     * @param relationship representation of the relationship to stop tracking.
     * @param node         on which to stop tracking.
     */
    public void deleteCount(C relationship, Node node) {
        node.removeProperty(relationship.toString());
    }
}
