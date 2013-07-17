package com.graphaware.neo4j.relcount.common.logic;

import com.graphaware.neo4j.common.Constants;
import com.graphaware.neo4j.dto.common.relationship.SerializableTypeAndDirection;
import com.graphaware.neo4j.tx.batch.IterableInputBatchExecutor;
import com.graphaware.neo4j.tx.batch.UnitOfWork;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.Map;
import java.util.TreeMap;

/**
 * Abstract base-class for {@link RelationshipCountCache} implementations that cache relationship counts as properties
 * on {@link Node}s.
 *
 * @param <DESCRIPTION> type of relationship representation that can be used as a relationship description for caching.
 *                      Must be {@link Comparable}; the resulting order is essential for determining, which property
 *                      corresponds to an about-to-be-cached relationship count.
 */
public abstract class BaseRelationshipCountCache<DESCRIPTION extends SerializableTypeAndDirection & Comparable<DESCRIPTION>> {

    private final String id;

    /**
     * Construct a new cache.
     *
     * @param id of the module this cache is for.
     */
    protected BaseRelationshipCountCache(String id) {
        this.id = id;
    }

    /**
     * Get all relationship counts cached on a node. No aggregation is performed, this is the raw data as stored.
     *
     * @param node from which to get cached relationship counts.
     * @return cached relationship counts (key = relationship representation, value = count). The map is sorted
     *         so that it can be iterated in order (e.g. alphabetic or specific to general).
     */
    public Map<DESCRIPTION, Integer> getRelationshipCounts(Node node) {
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
     * Handle (i.e. cache) a created relationship.
     *
     * @param relationship     the has been created.
     * @param pointOfView      node whose point of view the created relationships is being handled, i.e. the one on which
     *                         the relationship count should be cached.
     * @param defaultDirection in case the relationship direction would be resolved to {@link org.neo4j.graphdb.Direction#BOTH}, what
     *                         should it actually be resolved to? This must be {@link org.neo4j.graphdb.Direction#OUTGOING} or {@link org.neo4j.graphdb.Direction#INCOMING},
     *                         never cache {@link org.neo4j.graphdb.Direction#BOTH}! (because its meaning would be unclear - is it just the
     *                         cyclical relationships, or all? Also, there would be trouble during compaction and eventually,
     *                         incoming and outgoing relationships could be compacted to BOTH, so it would be impossible
     *                         to find only incoming or outgoing.
     */
    protected abstract void handleCreatedRelationship(Relationship relationship, Node pointOfView, Direction defaultDirection);

    /**
     * Handle (i.e. cache) a deleted relationship.
     *
     * @param relationship     the has been deleted.
     * @param pointOfView      node whose point of view the deleted relationships is being handled, i.e. the one on which
     *                         the relationship count should be cached.
     * @param defaultDirection in case the relationship direction would be resolved to {@link Direction#BOTH}, what
     *                         should it actually be resolved to? This must be {@link Direction#OUTGOING} or {@link Direction#INCOMING},
     *                         never cache {@link Direction#BOTH}! (because its meaning would be unclear - is it just the
     *                         cyclical relationships, or all? Also, there would be trouble during compaction and eventually,
     *                         incoming and outgoing relationships could be compacted to BOTH, so it would be impossible
     *                         to find only incoming or outgoing.
     */
    protected abstract void handleDeletedRelationship(Relationship relationship, Node pointOfView, Direction defaultDirection);

    /**
     * Clear all cached counts. NOTE: This is a potentially very expensive operation as it traverses the
     * entire graph! Use with care.
     *
     * @param databaseService to perform the operation on.
     */
    public void clearCachedCounts(GraphDatabaseService databaseService) {
        new IterableInputBatchExecutor<>(
                databaseService,
                1000,
                GlobalGraphOperations.at(databaseService).getAllNodes(),
                new UnitOfWork<Node>() {
                    @Override
                    public void execute(GraphDatabaseService database, Node node) {
                        for (String key : node.getPropertyKeys()) {
                            if (key.startsWith(prefix())) {
                                node.removeProperty(key);
                            }
                        }
                    }
                }
        ).execute();
    }

    /**
     * Clear and rebuild all cached counts. NOTE: This is a potentially very expensive operation as it traverses the
     * entire graph! Use with care.
     *
     * @param databaseService to perform the operation on.
     */
    public void rebuildCachedCounts(GraphDatabaseService databaseService) {
        clearCachedCounts(databaseService);

        new IterableInputBatchExecutor<>(
                databaseService,
                1000,
                GlobalGraphOperations.at(databaseService).getAllRelationships(),
                new UnitOfWork<Relationship>() {
                    @Override
                    public void execute(GraphDatabaseService database, Relationship relationship) {
                        handleCreatedRelationship(relationship, relationship.getStartNode(), Direction.INCOMING);
                        handleCreatedRelationship(relationship, relationship.getEndNode(), Direction.OUTGOING);
                    }
                }).execute();

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
                String key = cachedRelationship.toString(prefix());
                node.setProperty(key, (Integer) node.getProperty(key) + delta);
                return false;
            }
        }

        node.setProperty(relationship.toString(prefix()), delta);
        return true;

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
                String key = cachedRelationship.toString(prefix());
                int newValue = (Integer) node.getProperty(key) - delta;
                node.setProperty(key, newValue);

                if (newValue <= 0) {
                    deleteCount(cachedRelationship, node);
                }

                return newValue >= 0;
            }
        }

        return false;
    }

    /**
     * Stop tracking relationship count for a node.
     *
     * @param relationship representation of the relationship to stop tracking.
     * @param node         on which to stop tracking.
     */
    public void deleteCount(DESCRIPTION relationship, Node node) {
        node.removeProperty(relationship.toString(prefix()));
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
     * Build a prefix that all properties on nodes written and read by this cache will get.
     *
     * @return prefix.
     */
    private String prefix() {
        return Constants.GA_PREFIX + id + "_";
    }
}
