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

package com.graphaware.module.relcount.count;

import com.graphaware.module.relcount.RelationshipCountModule;
import org.neo4j.graphdb.GraphDatabaseService;

/**
 * {@link BaseFallbackRelationshipCounter} using {@link NaiveRelationshipCounter}.
 */
public class FallbackRelationshipCounter extends BaseFallbackRelationshipCounter {

    /**
     * Construct a new relationship counter. Use this constructor when
     * only a single instance of {@link com.graphaware.module.relcount.RelationshipCountModule} is registered.
     *
     * @param database on which the module is running.
     */
    public FallbackRelationshipCounter(GraphDatabaseService database) {
        this(database, RelationshipCountModule.FULL_RELCOUNT_DEFAULT_ID);
    }

    /**
     * Construct a new relationship counter. Use this constructor when multiple instances of {@link com.graphaware.module.relcount.RelationshipCountModule}
     * have been registered with the {@link com.graphaware.runtime.GraphAwareRuntime}.
     * This should rarely be the case.
     *
     * @param database on which the module is running.
     * @param id       of the {@link com.graphaware.module.relcount.RelationshipCountModule} used to cache relationship counts.
     */
    public FallbackRelationshipCounter(GraphDatabaseService database, String id) {
        super(new NaiveRelationshipCounter(database, id), new CachedRelationshipCounter(database, id));
    }
}
