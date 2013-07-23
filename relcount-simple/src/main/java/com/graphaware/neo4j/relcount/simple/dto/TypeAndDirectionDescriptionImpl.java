package com.graphaware.neo4j.relcount.simple.dto;

import com.graphaware.neo4j.dto.common.relationship.HasTypeAndDirection;
import com.graphaware.neo4j.dto.common.relationship.SerializableTypeAndDirectionImpl;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import static com.graphaware.neo4j.framework.config.FrameworkConfiguration.DEFAULT_SEPARATOR;

/**
 * A simple implementation of {@link TypeAndDirectionDescription}.
 */
public class TypeAndDirectionDescriptionImpl extends SerializableTypeAndDirectionImpl implements TypeAndDirectionDescription {

    /**
     * Construct a description. If the start node of this relationship is the same as the end node,
     * the direction will be resolved as {@link org.neo4j.graphdb.Direction#BOTH}.
     *
     * @param relationship Neo4j relationship serving as a template.
     * @param pointOfView  node which is looking at this relationship and thus determines its direction.
     */
    public TypeAndDirectionDescriptionImpl(Relationship relationship, Node pointOfView) {
        super(relationship, pointOfView);
    }

    /**
     * Construct a description.
     *
     * @param type      type.
     * @param direction direction.
     */
    public TypeAndDirectionDescriptionImpl(RelationshipType type, Direction direction) {
        super(type, direction);
    }

    /**
     * Construct a description from another one.
     *
     * @param description description.
     */
    public TypeAndDirectionDescriptionImpl(HasTypeAndDirection description) {
        super(description);
    }

    /**
     * Construct a description from a string.
     *
     * @param string    string to construct description from. Must be of the form prefix + type#direction
     *                  (assuming # separator).
     * @param prefix    of the string that should be removed before conversion.
     * @param separator of information.
     */
    public TypeAndDirectionDescriptionImpl(String string, String prefix, String separator) {
        super(string, prefix, separator);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(TypeAndDirectionDescription o) {
        return this.toString(DEFAULT_SEPARATOR).compareTo(o.toString(DEFAULT_SEPARATOR));
    }
}
