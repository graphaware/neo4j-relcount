package com.graphaware.neo4j.relcount.full.api;

import com.graphaware.neo4j.dto.common.property.ImmutableProperties;
import com.graphaware.neo4j.dto.common.propertycontainer.MakesCopyWithProperty;
import com.graphaware.neo4j.dto.common.relationship.HasTypeDirectionAndProperties;
import com.graphaware.neo4j.relcount.common.api.RelationshipCounter;
import org.neo4j.graphdb.Node;

/**
 * A {@link RelationshipCounter} capable of counting relationships based on their "full" description,
 * i.e. {@link org.neo4j.graphdb.RelationshipType}, {@link org.neo4j.graphdb.Direction}, and an arbitrary number of
 * properties (key-value pairs).
 * <p/>
 * The fluent interface of this counter allows such "description" to be constructed by calling the constructor and then
 * successively {@link #with(String, Object)} to add properties. Finally, by calling {@link #count(org.neo4j.graphdb.Node)}
 * or {@link #countLiterally(org.neo4j.graphdb.Node)}, the number of matching relationships on the node is returned.
 * <p/>
 * Matching relationships counted by the {@link #count(org.neo4j.graphdb.Node)} method are all relationships at least as
 * specific as the relationship description provided to this counter. In other words, properties not added to the description
 * are treated as wildcards. For example, if this counter is configured to count all OUTGOING relationships of type
 * "FRIEND" with property "strength" equal to 2, all relationships with that specification <b>including those with other
 * properties</b> (such as "timestamp" = 123456) will be counted.
 * <p/>
 * On the other hand, matching relationships counted by the {@link #countLiterally(org.neo4j.graphdb.Node)} method are
 * all relationships that are exactly the same as the relationship description provided to this counter. In other words,
 * properties not added to this description are treated as "property required to be undefined".
 * For example, if this counter is configured to count all OUTGOING relationships of type "FRIEND" with property "strength"
 * equal to 2, only relationships with that specification <b>excluding those with other properties</b> (such as "timestamp" = 123456)
 * will be counted.
 */
public interface FullRelationshipCounter extends RelationshipCounter, HasTypeDirectionAndProperties<String, ImmutableProperties<String>>, MakesCopyWithProperty<FullRelationshipCounter> {

    /**
     * Count relationships at least as specific as the relationship described by this counter on the given node.
     *
     * @param node on which to count relationships.
     * @return number of relationships.
     * @throws com.graphaware.neo4j.relcount.common.api.UnableToCountException
     *          indicating that for some reason, relationships could not be counted.
     *          For example, when asking for a count purely based on cached values and the cached
     *          values are not present (e.g. have been compacted-out).
     */
    @Override
    int count(Node node);

    /**
     * Count relationships that are exactly the same as the relationship described by this counter on the given node.
     *
     * @param node on which to count relationships.
     * @return number of relationships.
     * @throws com.graphaware.neo4j.relcount.common.api.UnableToCountException
     *          indicating that for some reason, relationships could not be counted.
     *          For example, when asking for a count purely based on cached values and the cached
     *          values are not present (e.g. have been compacted-out).
     */
    int countLiterally(Node node);
}
