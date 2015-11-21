/*
 * Copyright (c) 2013-2015 GraphAware
 *
 * This file is part of the GraphAware Framework.
 *
 * GraphAware Framework is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details. You should have received a copy of
 * the GNU General Public License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

package com.graphaware.module.relcount.bootstrap;

import com.graphaware.module.relcount.RelationshipCountConfigurationImpl;
import com.graphaware.module.relcount.RelationshipCountModule;
import com.graphaware.module.relcount.compact.ThresholdBasedCompactionStrategy;
import com.graphaware.runtime.config.function.StringToRelationshipInclusionPolicy;
import com.graphaware.runtime.config.function.StringToRelationshipPropertyInclusionPolicy;
import com.graphaware.runtime.module.RuntimeModule;
import com.graphaware.runtime.module.RuntimeModuleBootstrapper;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Map;

/**
 * {@link RuntimeModuleBootstrapper} for {@link com.graphaware.module.relcount.RelationshipCountModule}.
 */
public class RelcountModuleBootstrapper implements RuntimeModuleBootstrapper {

    private static final String THRESHOLD = "threshold";
    private static final String RELATIONSHIP = "relationship";
    private static final String RELATIONSHIP_PROPERTY = "relationship.property";

    /**
     * {@inheritDoc}
     */
    @Override
    public RuntimeModule bootstrapModule(String moduleId, Map<String, String> config, GraphDatabaseService database) {
        RelationshipCountConfigurationImpl relationshipCountStrategies = RelationshipCountConfigurationImpl.defaultConfiguration();

        if (config.containsKey(THRESHOLD)) {
            relationshipCountStrategies = relationshipCountStrategies.with(new ThresholdBasedCompactionStrategy(Integer.valueOf(config.get(THRESHOLD))));
        }

        if (config.containsKey(RELATIONSHIP)) {
            relationshipCountStrategies = relationshipCountStrategies.with(StringToRelationshipInclusionPolicy.getInstance().apply(config.get(RELATIONSHIP)));
        }

        if (config.containsKey(RELATIONSHIP_PROPERTY)) {
            relationshipCountStrategies = relationshipCountStrategies.with(StringToRelationshipPropertyInclusionPolicy.getInstance().apply(config.get(RELATIONSHIP_PROPERTY)));
        }

        return new RelationshipCountModule(moduleId, relationshipCountStrategies);
    }
}
