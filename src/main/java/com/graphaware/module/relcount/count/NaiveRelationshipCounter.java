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

import com.graphaware.common.description.property.PropertiesDescription;
import com.graphaware.common.description.relationship.RelationshipDescription;
import com.graphaware.module.relcount.RelationshipCountModule;
import com.graphaware.runtime.config.RuntimeConfiguration;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import static com.graphaware.common.description.predicate.Predicates.any;
import static org.neo4j.graphdb.Direction.*;

/**
 * An optimized {@link LegacyNaiveRelationshipCounter} that uses that {@link org.neo4j.graphdb.Node#getDegree()} methods,
 * present since Neo4j 2.1, when it can. That is when counting is not done using property values and weighing strategy
 * is {@link OneForEach}.
 */
public class NaiveRelationshipCounter extends LegacyNaiveRelationshipCounter {

    public static final String TEST_KEY = RuntimeConfiguration.GA_PREFIX + "test";

    /**
     * Construct a new relationship counter. Use when no runtime or relationship count module is present.
     */
    public NaiveRelationshipCounter() {
        super();
    }

    /**
     * Construct a new relationship counter. Use when no runtime or relationship count module is present.
     *
     * @param weighingStrategy strategy for weighing relationships.
     */
    public NaiveRelationshipCounter(WeighingStrategy weighingStrategy) {
        super(weighingStrategy);
    }

    /**
     * Construct a new relationship counter. Use when runtime is started and a relationship count module registered with the default ID.
     *
     * @param database with runtime.
     */
    public NaiveRelationshipCounter(GraphDatabaseService database) {
        this(database, RelationshipCountModule.FULL_RELCOUNT_DEFAULT_ID);
    }

    /**
     * Construct a new relationship counter. Use when runtime is started and a relationship count module registered.
     *
     * @param database with runtime.
     * @param id       of the relationship count module. The module will not be used for this counter, but its configuration
     *                 (like weighing strategies) will.
     */
    public NaiveRelationshipCounter(GraphDatabaseService database, String id) {
        super(database, id);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int count(Node node, RelationshipDescription description) {
        //performance optimization since 2.1
        if (doesNotCareAboutProperties(description) && OneForEach.getInstance().equals(relationshipCountConfiguration.getWeighingStrategy())) {
            if (BOTH.equals(description.getDirection())) {
                //Neo4j only counts loop as 1
                return node.getDegree(description.getType(), OUTGOING) + node.getDegree(description.getType(), INCOMING);
            }

            return node.getDegree(description.getType(), description.getDirection());
        }

        return super.count(node, description);
    }

    private boolean doesNotCareAboutProperties(RelationshipDescription description) {
        PropertiesDescription propertiesDescription = description.getPropertiesDescription();

        if (!propertiesDescription.getKeys().iterator().hasNext()) {
            return any().equals(propertiesDescription.get(TEST_KEY));
        }

        for (String key : propertiesDescription.getKeys()) {
            if (!any().equals(propertiesDescription.get(key))) {
                return false;
            }
        }

        return true;
    }
}
