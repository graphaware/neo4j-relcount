package com.graphaware.neo4j.relcount.common.logic;

import com.graphaware.neo4j.common.Constants;
import com.graphaware.neo4j.dto.common.relationship.HasTypeAndDirection;
import org.neo4j.graphdb.Node;

import java.util.Map;
import java.util.TreeMap;

/**
 * Abstract base-class for {@link RelationshipCountReader} implementations that read relationship counts cached as
 * {@link Node}'s properties, written by a subclass of {@link BaseRelationshipCountCache}.
 *
 * @param <DESCRIPTION> type of relationship representation that can be used as a relationship description for querying.
 *                      Must be {@link Comparable}; the resulting order is the order in which candidates are evaluated.
 */
public abstract class CachedRelationshipCountReader<DESCRIPTION extends HasTypeAndDirection & Comparable<DESCRIPTION>> extends BaseRelationshipCountReader<DESCRIPTION> {

    private final String id;

    protected CachedRelationshipCountReader(String id) {
        this.id = id;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * Gets all relationship counts cached as the node's properties. Ignores the description, always returns all cached
     * counts. No aggregation is performed, this is the raw data as stored
     * (as opposed to {@link #getRelationshipCount(com.graphaware.neo4j.dto.common.relationship.HasTypeAndDirection, org.neo4j.graphdb.Node)}).
     * The returned map is sorted so that it can be iterated in order (e.g. alphabetic or specific to general).
     */
    @Override
    public Map<DESCRIPTION, Integer> getCandidates(DESCRIPTION description, Node node) {
        Map<DESCRIPTION, Integer> result = new TreeMap<>();
        for (String key : node.getPropertyKeys()) {
            if (key.startsWith(prefix())) {
                result.put(newCachedRelationship(key, prefix()), (Integer) node.getProperty(key));
            }
        }
        return result;
    }

    /**
     * Create a cached relationship representation from a String representation of the cached relationship, coming from
     * a node's property key.
     *
     * @param string string representation of the cached relationship.
     * @param prefix to be removed from the string representation before conversion.
     * @return object representation of the cached relationship.
     */
    protected abstract DESCRIPTION newCachedRelationship(String string, String prefix);

    /**
     * Build a prefix that all properties on nodes written and read by this cache will get.
     *
     * @return prefix.
     */
    private String prefix() {
        return Constants.GA_PREFIX + id + "_";
    }
}
