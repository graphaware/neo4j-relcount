package com.graphaware.neo4j.relcount.full.logic;

import com.graphaware.neo4j.dto.common.relationship.DirectedRelationship;
import com.graphaware.neo4j.relcount.common.manager.BaseNaiveRelationshipCountManager;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * Naive {@link com.graphaware.neo4j.relcount.common.manager.RelationshipCountManager} that counts matching relationships by inspecting all {@link Node}'s {@link Relationship}s.
 */
public class FullNaiveRelationshipCountManager extends BaseNaiveRelationshipCountManager<DirectedRelationship> {
}
