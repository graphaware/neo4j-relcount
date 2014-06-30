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

/**
 * An optimized {@link NaiveRelationshipCounter} that uses that {@link org.neo4j.graphdb.Node#getDegree()} methods,
 * present since Neo4j 2.1, when it can.
 * <p/>
 * <b>WARNING!!! Loops are only counted as 1 towards the total node degree when using this counter. This is how Neo4j
 * implements degree counts. Also, any configured {@link WeighingStrategy} is ignored.</b>
 */
public class OptimizedNaiveRelationshipCounter extends NaiveRelationshipCounter {

    public static final String TEST_KEY = RuntimeConfiguration.GA_PREFIX + "test";

    /**
     * Construct a new relationship counter with default strategies.
     *
     * @param database on which to count relationships.
     */
    public OptimizedNaiveRelationshipCounter(GraphDatabaseService database) {
        super(database, RelationshipCountModule.FULL_RELCOUNT_DEFAULT_ID);
    }

    protected OptimizedNaiveRelationshipCounter(GraphDatabaseService database, String id) {
        super(database, id, OneForEach.getInstance());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int count(Node node, RelationshipDescription description) {
        //performance optimization since 2.1
        if (doesNotCareAboutProperties(description)) {
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
