/*
 * Copyright (c) 2013 GraphAware
 *
 * This file is part of GraphAware.
 *
 * GraphAware is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details. You should have received a copy of
 * the GNU General Public License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

package com.graphaware.module.relcount.count;

import com.graphaware.common.description.predicate.Predicates;
import com.graphaware.common.description.property.LazyPropertiesDescription;
import com.graphaware.common.description.property.PropertiesDescription;
import com.graphaware.common.description.relationship.RelationshipDescription;
import com.graphaware.module.relcount.RelationshipCountConfiguration;
import com.graphaware.module.relcount.RelationshipCountConfigurationImpl;
import com.graphaware.module.relcount.RelationshipCountModule;
import com.graphaware.runtime.config.DefaultRuntimeConfiguration;
import com.graphaware.runtime.config.RuntimeConfiguration;
import com.graphaware.runtime.metadata.ProductionSingleNodeMetadataRepository;
import com.graphaware.runtime.metadata.TxDrivenModuleMetadata;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.graphaware.common.description.predicate.Predicates.*;
import static org.neo4j.graphdb.Direction.BOTH;

/**
 * A naive {@link RelationshipCounter} that counts matching relationships by inspecting all {@link org.neo4j.graphdb.Node}'s
 * {@link org.neo4j.graphdb.Relationship}s. If possible, i.e. if only {@link org.neo4j.graphdb.RelationshipType} and
 * {@link org.neo4j.graphdb.Direction} (but no property constrains are specified), the {@link org.neo4j.graphdb.Node#getDegree()}
 * and related APIs are used.
 * <p/>
 * Because relationships are counted on the fly (no caching performed), this can be used without the
 * {@link com.graphaware.runtime.GraphAwareRuntime} and/or any {@link com.graphaware.runtime.module.RuntimeModule}s.
 * <p/>
 * This counter always returns a count, never throws {@link UnableToCountException}.
 */
public class NaiveRelationshipCounter implements RelationshipCounter {

    private final RelationshipCountConfiguration relationshipCountConfiguration;

    /**
     * Construct a new relationship counter with default strategies.
     *
     * @param database on which to count relationships.
     */
    public NaiveRelationshipCounter(GraphDatabaseService database) {
        this(database, RelationshipCountModule.FULL_RELCOUNT_DEFAULT_ID);
    }

    protected NaiveRelationshipCounter(GraphDatabaseService database, String id) {
        this(database, id, OneForEach.getInstance());
    }

    /**
     * Construct a new relationship counter with default strategies.
     *
     * @param database         on which to count relationships.
     * @param weighingStrategy strategy for weighing relationships.
     *                         Only taken into account if there is no {@link RelationshipCountModule} registered with the {@link com.graphaware.runtime.GraphAwareRuntime}. Otherwise the one configured for the module is used.
     */
    public NaiveRelationshipCounter(GraphDatabaseService database, WeighingStrategy weighingStrategy) {
        this(database, RelationshipCountModule.FULL_RELCOUNT_DEFAULT_ID, weighingStrategy);
    }

    /**
     * Construct a new relationship counter.
     */
    protected NaiveRelationshipCounter(GraphDatabaseService database, String id, WeighingStrategy weighingStrategy) {
        try (Transaction tx = database.beginTx()) {
            TxDrivenModuleMetadata moduleMetadata = new ProductionSingleNodeMetadataRepository(database, DefaultRuntimeConfiguration.getInstance(), RuntimeConfiguration.TX_MODULES_PROPERTY_PREFIX).getModuleMetadata(id);
            if (moduleMetadata == null || moduleMetadata.getConfig() == null) {
                this.relationshipCountConfiguration = RelationshipCountConfigurationImpl.defaultConfiguration().with(weighingStrategy);
            } else {
                this.relationshipCountConfiguration = (RelationshipCountConfiguration) moduleMetadata.getConfig();
            }
            tx.success();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int count(Node node, RelationshipDescription description) {
        int result = 0;

        for (Relationship candidateRelationship : node.getRelationships(description.getDirection(), description.getType())) {
            PropertiesDescription candidate = new LazyPropertiesDescription(candidateRelationship);

            if (candidate.isMoreSpecificThan(description.getPropertiesDescription())) {
                int relationshipWeight = relationshipCountConfiguration.getWeighingStrategy().getRelationshipWeight(candidateRelationship, node);
                result = result + relationshipWeight;

                //double count loops if looking for BOTH
                if (BOTH.equals(description.getDirection()) && candidateRelationship.getStartNode().getId() == candidateRelationship.getEndNode().getId()) {
                    result = result + relationshipWeight;
                }
            }
        }

        return result;
    }
}
