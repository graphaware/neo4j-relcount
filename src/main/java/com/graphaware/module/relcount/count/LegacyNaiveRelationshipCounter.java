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

import com.graphaware.common.description.property.LazyPropertiesDescription;
import com.graphaware.common.description.property.PropertiesDescription;
import com.graphaware.common.description.relationship.RelationshipDescription;
import com.graphaware.module.relcount.RelationshipCountConfiguration;
import com.graphaware.module.relcount.RelationshipCountConfigurationImpl;
import com.graphaware.module.relcount.RelationshipCountModule;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import static com.graphaware.runtime.ProductionRuntime.*;
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
 * <p/>
 * Note that it is called legacy, because it is superseded by {@link NaiveRelationshipCounter} as of Neo4j 2.1
 *
 * @deprecated in favour of {@link NaiveRelationshipCounter}
 */
@Deprecated
public class LegacyNaiveRelationshipCounter implements RelationshipCounter {

    protected final RelationshipCountConfiguration relationshipCountConfiguration;

    /**
     * Construct a new relationship counter. Use when no runtime or relationship count module is present.
     */
    protected LegacyNaiveRelationshipCounter() {
        this(OneForEach.getInstance());
    }

    /**
     * Construct a new relationship counter. Use when no runtime or relationship count module is present.
     *
     * @param weighingStrategy strategy for weighing relationships.
     */
    protected LegacyNaiveRelationshipCounter(WeighingStrategy weighingStrategy) {
        this.relationshipCountConfiguration = RelationshipCountConfigurationImpl.defaultConfiguration().with(weighingStrategy);
    }

    /**
     * Construct a new relationship counter. Use when runtime is started and a relationship count module registered.
     *
     * @param database with runtime.
     * @param id       of the relationship count module.
     */
    protected LegacyNaiveRelationshipCounter(GraphDatabaseService database, String id) {
        this.relationshipCountConfiguration = getStartedRuntime(database).getModule(id, RelationshipCountModule.class).getConfiguration();
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
