package com.graphaware.neo4j.relcount.simple.api;

import com.graphaware.neo4j.dto.common.relationship.HasTypeAndDirection;
import com.graphaware.neo4j.relcount.common.manager.RelationshipCountManager;
import com.graphaware.neo4j.relcount.simple.manager.SimpleCachingRelationshipCountManager;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.RelationshipType;

/**
 *
 */
public class SimpleCachingRelationshipCounter extends BaseSimpleRelationshipCounter {

    public SimpleCachingRelationshipCounter(RelationshipType type, Direction direction) {
        super(type, direction);
    }

    protected RelationshipCountManager<HasTypeAndDirection> getRelationshipCountManager() {
        return new SimpleCachingRelationshipCountManager();
    }
}
