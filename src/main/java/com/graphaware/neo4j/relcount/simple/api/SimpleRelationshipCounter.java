package com.graphaware.neo4j.relcount.simple.api;

import com.graphaware.neo4j.relcount.common.api.RelationshipCounter;

/**
 * A {@link RelationshipCounter} capable of counting relationships based on their "simple" description, i.e. type and
 * direction only (no properties).
 * <p/>
 * The "description" of which relationships are to be counted should be calling the constructor, providing a
 * {@link org.neo4j.graphdb.RelationshipType} and {@link org.neo4j.graphdb.Direction}.
 */
public interface SimpleRelationshipCounter extends RelationshipCounter {
}
