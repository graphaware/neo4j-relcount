package com.graphaware.relcount.common.internal.node;

import com.graphaware.propertycontainer.dto.common.relationship.HasTypeAndDirection;
import com.graphaware.propertycontainer.dto.common.relationship.SerializableTypeAndDirection;
import org.neo4j.graphdb.Node;

/**
 * Base-class for {@link RelationshipCountingNode} implementations that count matching relationships by looking them up
 * cached as {@link Node}'s properties.
 *
 * @param <CACHED>      type of cached relationship representation.
 * @param <DESCRIPTION> type of relationship description that can be used to query relationship counts for nodes.
 */
public abstract class CachedRelationshipCountingNode<CACHED extends SerializableTypeAndDirection, DESCRIPTION extends HasTypeAndDirection> {

    protected final Node node;
    protected final String prefix;
    protected final String separator;

    /**
     * Construct a new counting node.
     *
     * @param node      backing Neo4j node.
     * @param prefix    of the cached relationship string representation.
     * @param separator of information in the cached relationship string representation.
     */
    protected CachedRelationshipCountingNode(Node node, String prefix, String separator) {
        this.node = node;
        this.prefix = prefix;
        this.separator = separator;
    }

    /**
     * @see {@link RelationshipCountingNode#getId()}
     */
    public long getId() {
        return node.getId();
    }

    /**
     * @see {@link RelationshipCountingNode#getRelationshipCount(com.graphaware.propertycontainer.dto.common.relationship.HasTypeAndDirection)}
     */
    public int getRelationshipCount(DESCRIPTION description) {
        int result = 0;

        for (String key : node.getPropertyKeys()) {
            if (key.startsWith(prefix)) {
                CACHED candidate = newCachedRelationship(key);
                if (candidateMatchesDescription(candidate, description)) {
                    result += (int) node.getProperty(key);
                }
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
    protected abstract CACHED newCachedRelationship(String string);

    /**
     * Does the given candidate match the relationship description?
     *
     * @param candidate   candidate that could correspond to the given relationship description.
     * @param description of the relationships being counted.
     * @return true iff the candidate matches the description and should thus be taken into account.
     */
    protected abstract boolean candidateMatchesDescription(CACHED candidate, DESCRIPTION description);
}
