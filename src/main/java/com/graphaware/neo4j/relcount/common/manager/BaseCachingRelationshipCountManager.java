package com.graphaware.neo4j.relcount.common.manager;

import com.graphaware.neo4j.dto.common.relationship.HasTypeAndDirection;
import org.neo4j.graphdb.Node;

import java.util.Map;
import java.util.TreeMap;

import static com.graphaware.neo4j.common.Constants.GA_REL_PREFIX;

/**
 * Abstract base-class for {@link CachingRelationshipCountManager} implementations.
 *
 * @param <DESCRIPTION> type of relationship representation that can be used as a relationship description for querying
 *                      and caching. Must be {@link Comparable}; the resulting order is the order in which candidates
 *                      are evaluated.
 */
public abstract class BaseCachingRelationshipCountManager<DESCRIPTION extends HasTypeAndDirection & Comparable<DESCRIPTION>> extends BaseRelationshipCountManager<DESCRIPTION> {

    /**
     * {@inheritDoc}
     * <p/>
     * Gets all relationship counts cached as the node's properties. Ignores the description, always returns all cached
     * counts.
     */
    @Override
    public Map<DESCRIPTION, Integer> getRelationshipCounts(DESCRIPTION description, Node node) {
        return getRelationshipCounts(node);
    }

    /**
     * Get all relationship counts cached on a node. No aggregation is performed, this is the raw data as stored
     * (as opposed to {@link #getRelationshipCount(com.graphaware.neo4j.dto.common.relationship.HasTypeAndDirection, org.neo4j.graphdb.Node)}).
     *
     * @param node from which to get cached relationship counts.
     * @return cached relationship counts (key = relationship representation, value = count). The map is sorted
     *         so that it can be iterated in order (e.g. alphabetic or specific to general).
     */
    public Map<DESCRIPTION, Integer> getRelationshipCounts(Node node) {
        Map<DESCRIPTION, Integer> result = new TreeMap<>();
        for (String key : node.getPropertyKeys()) {
            if (key.startsWith(GA_REL_PREFIX)) {
                result.put(newCachedRelationship(key), (Integer) node.getProperty(key));
            }
        }
        return result;
    }

    /**
     * Create a cached relationship representation from a String representation of the cached relationship, coming from
     * a node's property key.
     *
     * @param string string representation of the cached relationship.
     * @return object representation of the cached relationship.
     */
    protected abstract DESCRIPTION newCachedRelationship(String string);

    /**
     * Increment the cached relationship count on the given node by 1.
     *
     * @param relationship representation of a relationship.
     * @param node         on which to increment the cached relationship count.
     * @return true iff the cached value did not exist and had to be created.
     */
    public boolean incrementCount(DESCRIPTION relationship, Node node) {
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
    public boolean incrementCount(DESCRIPTION relationship, Node node, int delta) {
        for (DESCRIPTION cachedRelationship : getRelationshipCounts(node).keySet()) {
            if (cachedMatch(cachedRelationship, relationship)) {
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
    public boolean decrementCount(DESCRIPTION relationship, Node node) {
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
    public boolean decrementCount(DESCRIPTION relationship, Node node, int delta) {
        for (DESCRIPTION cachedRelationship : getRelationshipCounts(node).keySet()) {
            if (cachedMatch(cachedRelationship, relationship)) {
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
     * Should the cached relationship be affected (incremented, decremented) by the given relationship representation?
     *
     * @param cached       cached relationship that could correspond to the given relationship being updated.
     * @param relationship representation being updated.
     * @return true iff the cached relationship is to be treated as the about-to-be-updated relationship's representation.
     */
    protected abstract boolean cachedMatch(DESCRIPTION cached, DESCRIPTION relationship);

    /**
     * Stop tracking relationship count for a node.
     *
     * @param relationship representation of the relationship to stop tracking.
     * @param node         on which to stop tracking.
     */
    public void deleteCount(DESCRIPTION relationship, Node node) {
        node.removeProperty(relationship.toString());
    }
}