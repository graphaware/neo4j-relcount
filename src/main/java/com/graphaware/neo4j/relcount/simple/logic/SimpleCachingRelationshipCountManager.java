package com.graphaware.neo4j.relcount.simple.logic;

import com.graphaware.neo4j.dto.common.relationship.HasDirectionAndType;
import com.graphaware.neo4j.dto.common.relationship.SerializableDirectionAndType;
import com.graphaware.neo4j.relcount.common.manager.CachingRelationshipCountManager;

/**
 *
 */
public interface SimpleCachingRelationshipCountManager extends CachingRelationshipCountManager<HasDirectionAndType, SerializableDirectionAndType> {
}
