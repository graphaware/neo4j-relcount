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

package com.graphaware.module.relcount;


import com.graphaware.module.relcount.cache.DegreeCachingStrategy;
import com.graphaware.module.relcount.compact.CompactionStrategy;
import com.graphaware.module.relcount.count.WeighingStrategy;
import com.graphaware.runtime.config.TxDrivenModuleConfiguration;

/**
 * Container for strategies and configuration related to relationship counting.
 */
public interface RelationshipCountConfiguration extends TxDrivenModuleConfiguration {

    /**
     * @return contained caching strategy.
     */
    DegreeCachingStrategy getDegreeCachingStrategy();

    /**
     * @return contained compaction strategy.
     */
    CompactionStrategy getCompactionStrategy();

    /**
     * @return contained relationship weighing strategy.
     */
    WeighingStrategy getWeighingStrategy();
}
