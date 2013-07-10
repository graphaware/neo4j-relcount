package com.graphaware.neo4j.relcount.simple.dto;

import com.graphaware.neo4j.dto.common.relationship.HasTypeAndDirection;
import com.graphaware.neo4j.dto.common.relationship.SerializableTypeAndDirectionImpl;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

/**
 *
 */
public class TypeAndDirectionDescriptionImpl extends SerializableTypeAndDirectionImpl implements TypeAndDirectionDescription {

    public TypeAndDirectionDescriptionImpl(Relationship relationship, Node pointOfView) {
        super(relationship, pointOfView);
    }

    public TypeAndDirectionDescriptionImpl(RelationshipType type, Direction direction) {
        super(type, direction);
    }

    public TypeAndDirectionDescriptionImpl(HasTypeAndDirection relationship) {
        super(relationship);
    }

    public TypeAndDirectionDescriptionImpl(String string) {
        super(string);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(TypeAndDirectionDescription o) {
        return this.toString().compareTo(o.toString());
    }
}
