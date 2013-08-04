package com.graphaware.neo4j.relcount.common.internal.cache;

import com.graphaware.neo4j.wrapper.BasePropertyContainerWrapper;
import com.graphaware.neo4j.wrapper.NodeWrapper;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A {@link Node} decorator that keeps track of properties itself, until {@link #flush()} is called, which is when they
 * are actually written to the database.
 */
class BatchFriendlyNode extends BasePropertyContainerWrapper<Node> implements Node, NodeWrapper {

    private final Node wrapped;
    private final Map<String, Object> properties = new HashMap<>();
    private final Set<String> updatedProperties = new HashSet<>();
    private final Set<String> removedProperties = new HashSet<>();

    /**
     * Construct a new batch friendly node.
     *
     * @param node real Neo4j node the instance will be backed by.
     */
    public BatchFriendlyNode(Node node) {
        this.wrapped = node;
        copyPropertiesFromNode();
    }

    /**
     * Copy all properties from the backing node to this instance.
     */
    private void copyPropertiesFromNode() {
        for (String key : wrapped.getPropertyKeys()) {
            properties.put(key, wrapped.getProperty(key));
        }
    }

    /**
     * Copy all properties from this instance to the backing node.
     */
    private void copyPropertiesToNode() {
        for (String key : removedProperties) {
            wrapped.removeProperty(key);
        }

        for (String key : updatedProperties) {
            wrapped.setProperty(key, properties.get(key));
        }
    }

    /**
     * Write all properties to the database.
     */
    public void flush() {
        copyPropertiesToNode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Node self() {
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node getWrapped() {
        return wrapped;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasProperty(String key) {
        return properties.containsKey(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getProperty(String key) {
        if (!hasProperty(key)) {
            throw new NotFoundException();
        }
        return properties.get(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setProperty(String key, Object value) {
        properties.put(key, value);
        updatedProperties.add(key);
        removedProperties.remove(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object removeProperty(String key) {
        Object oldValue = properties.get(key);
        properties.remove(key);
        updatedProperties.remove(key);
        removedProperties.add(key);
        return oldValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterable<String> getPropertyKeys() {
        return properties.keySet();
    }
}
