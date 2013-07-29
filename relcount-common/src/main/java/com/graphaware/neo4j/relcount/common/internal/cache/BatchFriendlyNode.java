package com.graphaware.neo4j.relcount.common.internal.cache;

import org.neo4j.graphdb.*;

import java.util.HashMap;
import java.util.Map;

/**
 * A {@link Node} decorator that keeps track of properties itself, until {@link #flush()} is called, which is when they
 * are actually written to the database.
 */
class BatchFriendlyNode implements Node {

    private final Node node;
    private final Map<String, Object> properties = new HashMap<>();

    /**
     * Construct a new batch friendly node.
     *
     * @param node real Neo4j node the instance will be backed by.
     */
    public BatchFriendlyNode(Node node) {
        this.node = node;
        copyPropertiesFromNode();
    }

    /**
     * Copy all properties from the backing node to this instance.
     */
    private void copyPropertiesFromNode() {
        for (String key : node.getPropertyKeys()) {
            properties.put(key, node.getProperty(key));
        }
    }

    /**
     * Copy all properties from this instance to the backing node.
     */
    private void copyPropertiesToNode() {
        for (String key : properties.keySet()) {
            node.setProperty(key, properties.get(key));
        }
    }

    /**
     * Write all properties to the database.
     */
    public void flush() {
        copyPropertiesToNode();
    }

    //properly overridden

    @Override
    public boolean hasProperty(String key) {
        return properties.containsKey(key);
    }

    @Override
    public Object getProperty(String key) {
        if (!hasProperty(key)) {
            throw new NotFoundException();
        }
        return properties.get(key);
    }

    @Override
    public Object getProperty(String key, Object defaultValue) {
        if (!hasProperty(key)) {
            return defaultValue;
        }
        return properties.get(key);
    }

    @Override
    public void setProperty(String key, Object value) {
        properties.put(key, value);
    }

    @Override
    public Object removeProperty(String key) {
        Object oldValue = properties.get(key);
        properties.remove(key);
        return oldValue;
    }

    @Override
    public Iterable<String> getPropertyKeys() {
        return properties.keySet();
    }

    @Override
    @Deprecated
    public Iterable<Object> getPropertyValues() {
        return properties.values();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BatchFriendlyNode batchFriendlyNode = (BatchFriendlyNode) o;

        if (!node.equals(batchFriendlyNode.node)) return false;

        return true;
    }

    //just delegated

    @Override
    public int hashCode() {
        return node.hashCode();
    }

    @Override
    public long getId() {
        return node.getId();
    }

    @Override
    public void delete() {
        node.delete();
    }

    @Override
    public Iterable<Relationship> getRelationships() {
        return node.getRelationships();
    }

    @Override
    public boolean hasRelationship() {
        return node.hasRelationship();
    }

    @Override
    public Iterable<Relationship> getRelationships(RelationshipType... types) {
        return node.getRelationships(types);
    }

    @Override
    public Iterable<Relationship> getRelationships(Direction direction, RelationshipType... types) {
        return node.getRelationships(direction, types);
    }

    @Override
    public boolean hasRelationship(RelationshipType... types) {
        return node.hasRelationship(types);
    }

    @Override
    public boolean hasRelationship(Direction direction, RelationshipType... types) {
        return node.hasRelationship(direction, types);
    }

    @Override
    public Iterable<Relationship> getRelationships(Direction dir) {
        return node.getRelationships(dir);
    }

    @Override
    public boolean hasRelationship(Direction dir) {
        return node.hasRelationship(dir);
    }

    @Override
    public Iterable<Relationship> getRelationships(RelationshipType type, Direction dir) {
        return node.getRelationships(type, dir);
    }

    @Override
    public boolean hasRelationship(RelationshipType type, Direction dir) {
        return node.hasRelationship(type, dir);
    }

    @Override
    public Relationship getSingleRelationship(RelationshipType type, Direction dir) {
        return node.getSingleRelationship(type, dir);
    }

    @Override
    public Relationship createRelationshipTo(Node otherNode, RelationshipType type) {
        return node.createRelationshipTo(otherNode, type);
    }

    @Override
    public Traverser traverse(Traverser.Order traversalOrder, StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator, RelationshipType relationshipType, Direction direction) {
        return node.traverse(traversalOrder, stopEvaluator, returnableEvaluator, relationshipType, direction);
    }

    @Override
    public Traverser traverse(Traverser.Order traversalOrder, StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator, RelationshipType firstRelationshipType, Direction firstDirection, RelationshipType secondRelationshipType, Direction secondDirection) {
        return node.traverse(traversalOrder, stopEvaluator, returnableEvaluator, firstRelationshipType, firstDirection, secondRelationshipType, secondDirection);
    }

    @Override
    public Traverser traverse(Traverser.Order traversalOrder, StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator, Object... relationshipTypesAndDirections) {
        return node.traverse(traversalOrder, stopEvaluator, returnableEvaluator, relationshipTypesAndDirections);
    }

    @Override
    public GraphDatabaseService getGraphDatabase() {
        return node.getGraphDatabase();
    }
}
