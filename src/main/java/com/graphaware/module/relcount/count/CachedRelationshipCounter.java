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

import com.graphaware.common.description.relationship.DetachedRelationshipDescription;
import com.graphaware.common.description.relationship.RelationshipDescription;
import com.graphaware.module.relcount.RelationshipCountConfiguration;
import com.graphaware.module.relcount.RelationshipCountModule;
import com.graphaware.module.relcount.cache.DegreeCachingNode;
import com.graphaware.runtime.config.RuntimeConfiguration;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import static com.graphaware.module.relcount.RelationshipCountModule.FULL_RELCOUNT_DEFAULT_ID;
import static com.graphaware.runtime.ProductionRuntime.getRuntime;

/**
 * {@link RelationshipCounter} that counts matching relationships by looking them up cached in {@link org.neo4j.graphdb.Node}'s properties.
 * It is designed to be used as a "singleton", i.e., do not create a new instance every time you want to count.
 * <p/>
 * It must be used in conjunction with {@link com.graphaware.module.relcount.RelationshipCountModule}
 * registered with {@link com.graphaware.runtime.GraphAwareRuntime}.
 * <p/>
 * This counter throws {@link UnableToCountException} if it detects it can not
 * reliably answer the question. This means compaction has taken place and this counter can't serve a request for
 * relationship count this specific. If you still want to count the relationship, either use {@link NaiveRelationshipCounter}
 * or consider increasing the compaction threshold.
 *
 * @see com.graphaware.module.relcount.compact.CompactionStrategy
 */
public class CachedRelationshipCounter implements RelationshipCounter {

    private final String id;
    private final RuntimeConfiguration config;
    private final RelationshipCountConfiguration relationshipCountConfiguration;

    /**
     * Construct a new relationship counter. Use this constructor when {@link com.graphaware.runtime.GraphAwareRuntime}
     * only a single instance of {@link com.graphaware.module.relcount.RelationshipCountModule}
     * is registered. This will be the case for most use cases.
     *
     * @param database on which the module is running.
     */
    public CachedRelationshipCounter(GraphDatabaseService database) {
        this(database, FULL_RELCOUNT_DEFAULT_ID);
    }

    /**
     * Construct a new relationship counter. Use this constructor when multiple instances of {@link com.graphaware.module.relcount.RelationshipCountModule}
     * have been registered with the {@link com.graphaware.runtime.GraphAwareRuntime}.
     * This should rarely be the case.
     *
     * @param database on which the module is running.
     * @param id       of the {@link com.graphaware.module.relcount.RelationshipCountModule} used to cache relationship counts.
     */
    public CachedRelationshipCounter(GraphDatabaseService database, String id) {
        this.id = id;
        this.config = getRuntime(database).getConfiguration();
        this.relationshipCountConfiguration = getRuntime(database).getModule(id, RelationshipCountModule.class).getConfiguration();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int count(Node node, RelationshipDescription description) {
        int result = 0;

        DegreeCachingNode cachingNode = new DegreeCachingNode(node, config.createPrefix(id), relationshipCountConfiguration);

        for (DetachedRelationshipDescription candidate : cachingNode.getCachedDegrees().keySet()) {

            boolean matches = candidate.isMoreSpecificThan(description);

            if (!matches && !candidate.isMutuallyExclusive(description)) {
                throw new UnableToCountException("Unable to count relationships with the following description: "
                        + description.toString()
                        + " Since there are potentially compacted out cached matches," +
                        " it looks like compaction has taken away the granularity you need. Please try to count this kind " +
                        "of relationship with a naive counter. Alternatively, increase the compaction threshold.");
            }

            if (matches) {
                result += cachingNode.getCachedDegrees().get(candidate);
            }
        }

        return result;
    }
}
