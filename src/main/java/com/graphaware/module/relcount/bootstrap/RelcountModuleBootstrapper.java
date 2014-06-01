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

package com.graphaware.module.relcount.bootstrap;

import com.graphaware.module.relcount.RelationshipCountConfigurationImpl;
import com.graphaware.module.relcount.RelationshipCountRuntimeModule;
import com.graphaware.module.relcount.compact.ThresholdBasedCompactionStrategy;
import com.graphaware.runtime.GraphAwareRuntimeModule;
import com.graphaware.runtime.GraphAwareRuntimeModuleBootstrapper;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Map;

/**
 * {@link com.graphaware.runtime.GraphAwareRuntimeModuleBootstrapper} for {@link com.graphaware.module.relcount.RelationshipCountRuntimeModule}.
 */
public class RelcountModuleBootstrapper implements GraphAwareRuntimeModuleBootstrapper {

    private static final String THRESHOLD = "threshold";

    @Override
    public GraphAwareRuntimeModule bootstrapModule(String moduleId, Map<String, String> config, GraphDatabaseService database) {
        RelationshipCountConfigurationImpl relationshipCountStrategies = RelationshipCountConfigurationImpl.defaultConfiguration();

        if (config.containsKey(THRESHOLD)) {
            relationshipCountStrategies = relationshipCountStrategies.with(new ThresholdBasedCompactionStrategy(Integer.valueOf(config.get(THRESHOLD))));
        }

        return new RelationshipCountRuntimeModule(moduleId, relationshipCountStrategies);
    }
}
