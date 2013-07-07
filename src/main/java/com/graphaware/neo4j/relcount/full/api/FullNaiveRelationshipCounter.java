package com.graphaware.neo4j.relcount.full.api;

import com.graphaware.neo4j.dto.common.relationship.DirectedRelationship;
import com.graphaware.neo4j.dto.string.property.CopyMakingSerializableProperties;
import com.graphaware.neo4j.relcount.common.manager.RelationshipCountManager;
import com.graphaware.neo4j.relcount.full.logic.FullNaiveRelationshipCountManager;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.RelationshipType;

/**
 * A naive {@link FullRelationshipCounter} that counts matching relationships by inspecting all
 * {@link org.neo4j.graphdb.Node}'s {@link org.neo4j.graphdb.Relationship}s. It delegates the work to {@link com.graphaware.neo4j.relcount.full.logic.FullNaiveRelationshipCountManager}.
 * <p/>
 * Because relationships are counted on the fly (no caching performed), this can be used without any {@link org.neo4j.graphdb.event.TransactionEventHandler}s
 * and on already existing graphs.
 */
public class FullNaiveRelationshipCounter extends BaseFullRelationshipCounter {

    /**
     * Construct a new relationship counter.
     *
     * @param type      type of the relationships to count.
     * @param direction direction of the relationships to count.
     */
    public FullNaiveRelationshipCounter(RelationshipType type, Direction direction) {
        super(type, direction);
    }

    /**
     * Construct a new relationship counter.
     *
     * @param type       type.
     * @param direction  direction.
     * @param properties props.
     */
    protected FullNaiveRelationshipCounter(RelationshipType type, Direction direction, CopyMakingSerializableProperties properties) {
        super(type, direction, properties);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected RelationshipCountManager<DirectedRelationship> getRelationshipCountManager() {
        return new FullNaiveRelationshipCountManager();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected FullRelationshipCounter newRelationshipCounter(RelationshipType type, Direction direction, CopyMakingSerializableProperties properties) {
        return new FullNaiveRelationshipCounter(type, direction, properties);
    }
}
