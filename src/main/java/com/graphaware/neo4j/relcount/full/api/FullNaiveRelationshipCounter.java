package com.graphaware.neo4j.relcount.full.api;

import com.graphaware.neo4j.dto.common.relationship.HasDirectionAndType;
import com.graphaware.neo4j.dto.string.property.CopyMakingSerializableProperties;
import com.graphaware.neo4j.relcount.common.manager.NaiveRelationshipCountManager;
import com.graphaware.neo4j.relcount.common.manager.RelationshipCountManager;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.RelationshipType;

/**
 *  A naive {@link FullRelationshipCounter} that counts the relationship
 */
public class FullNaiveRelationshipCounter extends BaseFullRelationshipCounter {

    public FullNaiveRelationshipCounter(RelationshipType type, Direction direction) {
        super(type, direction);
    }

    protected FullNaiveRelationshipCounter(RelationshipType type, Direction direction, CopyMakingSerializableProperties properties) {
        super(type, direction, properties);
    }

    protected RelationshipCountManager<HasDirectionAndType> getRelationshipCountManager() {
        return new NaiveRelationshipCountManager();
    }

    @Override
    protected FullRelationshipCounter newRelationshipCounter(RelationshipType type, Direction direction, CopyMakingSerializableProperties properties) {
        return new FullNaiveRelationshipCounter(type, direction, properties);
    }
}
