package com.graphaware.neo4j.relcount.simple.api;

import com.graphaware.neo4j.relcount.common.api.RelationshipCounter;

/**
 * A {@link RelationshipCounter} capable of counting relationships based on their "simple" description, i.e. type and
 * direction only (no properties).
 */
public interface SimpleRelationshipCounter extends RelationshipCounter {
}
