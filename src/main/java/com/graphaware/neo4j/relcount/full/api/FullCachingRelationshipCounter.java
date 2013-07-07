package com.graphaware.neo4j.relcount.full.api;

import com.graphaware.neo4j.dto.common.relationship.DirectedRelationship;
import com.graphaware.neo4j.dto.string.property.CopyMakingSerializableProperties;
import com.graphaware.neo4j.relcount.common.manager.RelationshipCountManager;
import com.graphaware.neo4j.relcount.full.logic.FullCachingRelationshipCountManagerImpl;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.RelationshipType;

/**
 *
 */
public class FullCachingRelationshipCounter extends BaseFullRelationshipCounter {

    public FullCachingRelationshipCounter(RelationshipType type, Direction direction) {
        super(type, direction);
    }

    protected FullCachingRelationshipCounter(RelationshipType type, Direction direction, CopyMakingSerializableProperties properties) {
        super(type, direction, properties);
    }

    protected RelationshipCountManager<DirectedRelationship> getRelationshipCountManager() {
        return new FullCachingRelationshipCountManagerImpl();
    }

    @Override
    protected FullRelationshipCounter newRelationshipCounter(RelationshipType type, Direction direction, CopyMakingSerializableProperties properties) {
        return new FullCachingRelationshipCounter(type, direction, properties);
    }
}
