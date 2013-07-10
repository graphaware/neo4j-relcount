package com.graphaware.neo4j.relcount.full.api;

import com.graphaware.neo4j.dto.common.relationship.HasTypeDirectionAndProperties;
import com.graphaware.neo4j.dto.string.property.CopyMakingSerializableProperties;
import com.graphaware.neo4j.tx.event.strategy.ExtractAllRelationshipProperties;
import com.graphaware.neo4j.tx.event.strategy.RelationshipPropertiesExtractionStrategy;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.RelationshipType;

/**
 * Abstract base-class for potentially naive (not caching) {@link com.graphaware.neo4j.relcount.common.api.RelationshipCounter}
 * implementations that need a {@link com.graphaware.neo4j.tx.event.strategy.RelationshipPropertiesExtractionStrategy}
 * because they inspect {@link org.neo4j.graphdb.Relationship}s directly in the database and need to know how to extract
 * their properties.
 */
public abstract class BasePossiblyNaiveRelationshipCounter extends BaseFullRelationshipCounter {

    protected final RelationshipPropertiesExtractionStrategy extractionStrategy;

    /**
     * Construct a new relationship counter.
     * <p/>
     * Properties are extracted using {@link com.graphaware.neo4j.tx.event.strategy.ExtractAllRelationshipProperties}.
     *
     * @param type      type of the relationships to count.
     * @param direction direction of the relationships to count.
     */
    protected BasePossiblyNaiveRelationshipCounter(RelationshipType type, Direction direction) {
        this(type, direction, ExtractAllRelationshipProperties.getInstance());
    }

    /**
     * Construct a relationship representation from another one.
     * <p/>
     * Properties are extracted using {@link com.graphaware.neo4j.tx.event.strategy.ExtractAllRelationshipProperties}.
     *
     * @param relationship relationships representation.
     */
    protected BasePossiblyNaiveRelationshipCounter(HasTypeDirectionAndProperties<String, ?> relationship) {
        this(relationship, ExtractAllRelationshipProperties.getInstance());
    }

    /**
     * Construct a new relationship counter.
     *
     * @param type               type of the relationships to count.
     * @param direction          direction of the relationships to count.
     * @param extractionStrategy for extracting properties from relationships.
     */
    protected BasePossiblyNaiveRelationshipCounter(RelationshipType type, Direction direction, RelationshipPropertiesExtractionStrategy extractionStrategy) {
        super(type, direction);
        this.extractionStrategy = extractionStrategy;
    }

    /**
     * Construct a counter.
     *
     * @param type               type.
     * @param direction          direction.
     * @param properties         props.
     * @param extractionStrategy for extracting properties from relationships.
     */
    protected BasePossiblyNaiveRelationshipCounter(RelationshipType type, Direction direction, CopyMakingSerializableProperties properties, RelationshipPropertiesExtractionStrategy extractionStrategy) {
        super(type, direction, properties);
        this.extractionStrategy = extractionStrategy;
    }

    /**
     * Construct a counter from another relationship representation.
     *
     * @param relationship       relationships representation.
     * @param extractionStrategy for extracting properties from relationships.
     */
    protected BasePossiblyNaiveRelationshipCounter(HasTypeDirectionAndProperties<String, ?> relationship, RelationshipPropertiesExtractionStrategy extractionStrategy) {
        super(relationship);
        this.extractionStrategy = extractionStrategy;
    }
}
