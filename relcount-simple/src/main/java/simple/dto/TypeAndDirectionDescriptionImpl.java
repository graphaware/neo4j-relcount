package simple.dto;

import com.graphaware.neo4j.dto.common.relationship.HasTypeAndDirection;
import com.graphaware.neo4j.dto.common.relationship.SerializableTypeAndDirectionImpl;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

/**
 * An simple implementation of {@link TypeAndDirectionDescription}.
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
     * @param string string to construct description from. Must be of the form prefix + type#direction
     *               (assuming the default {@link com.graphaware.neo4j.common.Constants#SEPARATOR}).
     * @param prefix of the string that should be removed before conversion.
     */
    public TypeAndDirectionDescriptionImpl(String string, String prefix) {
        super(string, prefix);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(TypeAndDirectionDescription o) {
        return this.toString().compareTo(o.toString());
    }
}
