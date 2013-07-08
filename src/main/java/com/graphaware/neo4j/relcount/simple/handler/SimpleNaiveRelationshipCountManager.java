package com.graphaware.neo4j.relcount.simple.handler;

import com.graphaware.neo4j.dto.common.relationship.HasTypeAndDirection;
import com.graphaware.neo4j.relcount.common.manager.BaseNaiveRelationshipCountManager;

/**
 * Naive {@link com.graphaware.neo4j.relcount.common.manager.RelationshipCountManager} that counts matching relationships by inspecting all {@link org.neo4j.graphdb.Node}'s {@link org.neo4j.graphdb.Relationship}s.
 */
public class SimpleNaiveRelationshipCountManager extends BaseNaiveRelationshipCountManager<HasTypeAndDirection> {
}
