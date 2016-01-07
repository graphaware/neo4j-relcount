/*
 * Copyright (c) 2013-2016 GraphAware
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

import com.graphaware.common.description.relationship.RelationshipDescription;
import org.neo4j.graphdb.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link com.graphaware.module.relcount.count.RelationshipCounter} that counts matching relationships by first trying to use {@link com.graphaware.module.relcount.count.CachedRelationshipCounter}
 * and if that fails (i.e., throws a {@link com.graphaware.module.relcount.count.UnableToCountException}), resorts to {@link LegacyNaiveRelationshipCounter}.
 * It is designed to be used as a "singleton", i.e., do not create a new instance every time you want to count.
 * <p/>
 * It should be used in conjunction with {@link com.graphaware.module.relcount.RelationshipCountModule}
 * registered with {@link com.graphaware.runtime.GraphAwareRuntime}.
 * <p/>
 * This counter always returns a count, never throws {@link com.graphaware.module.relcount.count.UnableToCountException}.
 * <p/>
 * About fallback: Fallback to naive approach only happens if it is detected that compaction has taken place
 * (see {@link com.graphaware.module.relcount.cache.NodeBasedDegreeCache}) and the needed granularity has
 * been compacted out. There is a performance penalty to this fallback.
 * To avoid it, make sure the compaction threshold is set correctly. No fallback happens when a {@link com.graphaware.common.policy.RelationshipInclusionPolicy} has been used that explicitly excludes
 * the relationships being counted (0 is returned). If you prefer an exception to fallback, use {@link com.graphaware.module.relcount.count.CachedRelationshipCounter}.
 */
public abstract class BaseFallbackRelationshipCounter implements RelationshipCounter {

    private static final Logger LOG = LoggerFactory.getLogger(BaseFallbackRelationshipCounter.class);

    private final LegacyNaiveRelationshipCounter naiveRelationshipCounter;
    private final CachedRelationshipCounter cachedRelationshipCounter;

    protected BaseFallbackRelationshipCounter(LegacyNaiveRelationshipCounter naiveRelationshipCounter, CachedRelationshipCounter cachedRelationshipCounter) {
        this.naiveRelationshipCounter = naiveRelationshipCounter;
        this.cachedRelationshipCounter = cachedRelationshipCounter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int count(Node node, RelationshipDescription description) {
        try {
            return cachedRelationshipCounter.count(node, description);
        } catch (UnableToCountException e) {
            LOG.warn("Unable to count relationships with description: " + description.toString() +
                    " for node " + node.toString() + ". Falling back to naive approach");
            return naiveRelationshipCounter.count(node, description);
        }
    }
}
