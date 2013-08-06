package com.graphaware.relcount.simple.internal.node;

import com.graphaware.propertycontainer.dto.common.relationship.SerializableTypeAndDirection;
import com.graphaware.propertycontainer.dto.common.relationship.SerializableTypeAndDirectionImpl;
import com.graphaware.relcount.common.internal.node.BaseRelationshipCountCachingNode;
import com.graphaware.relcount.common.internal.node.RelationshipCountCachingNode;
import org.neo4j.graphdb.Node;

import java.util.HashMap;
import java.util.Map;

/**
 * {@link RelationshipCountCachingNode} that caches relationship counts with their "simple" details, i.e. {@link org.neo4j.graphdb.RelationshipType}
 * and {@link org.neo4j.graphdb.Direction}. It ignores properties.
 */
public class SimpleRelationshipCountCachingNode extends BaseRelationshipCountCachingNode<SerializableTypeAndDirection> implements RelationshipCountCachingNode<SerializableTypeAndDirection> {

    /**
     * Construct a new caching node.
     *
     * @param node      backing Neo4j node.
     * @param prefix    of the cached relationship string representation.
     * @param separator of information in the cached relationship string representation.
     */
    public SimpleRelationshipCountCachingNode(Node node, String prefix, String separator) {
        super(node, prefix, separator);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Map<SerializableTypeAndDirection, Integer> newMap() {
        return new HashMap<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected SerializableTypeAndDirection newCachedRelationship(String string) {
        return new SerializableTypeAndDirectionImpl(string, prefix, separator);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean cachedMatch(SerializableTypeAndDirection description, SerializableTypeAndDirection relationship) {
        return description.matches(relationship);
    }
}
