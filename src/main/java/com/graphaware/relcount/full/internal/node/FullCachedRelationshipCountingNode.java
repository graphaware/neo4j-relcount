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

package com.graphaware.relcount.full.internal.node;

import com.graphaware.framework.config.FrameworkConfiguration;
import com.graphaware.relcount.common.counter.UnableToCountException;
import com.graphaware.relcount.common.internal.node.CachedRelationshipCountingNode;
import com.graphaware.relcount.common.internal.node.RelationshipCountingNode;
import com.graphaware.relcount.full.internal.dto.relationship.CacheableRelationshipDescription;
import com.graphaware.relcount.full.internal.dto.relationship.CacheableRelationshipDescriptionImpl;
import com.graphaware.relcount.full.internal.dto.relationship.RelationshipQueryDescription;
import org.neo4j.graphdb.Node;

/**
 * {@link com.graphaware.relcount.common.internal.node.RelationshipCountingNode} that count matching relationships by looking them up
 * cached as {@link org.neo4j.graphdb.Node}'s properties.  It is "full" in the sense that it cares about
 * {@link org.neo4j.graphdb.RelationshipType}s, {@link org.neo4j.graphdb.Direction}s, and properties.
 */
public class FullCachedRelationshipCountingNode extends CachedRelationshipCountingNode<CacheableRelationshipDescription, RelationshipQueryDescription> implements RelationshipCountingNode<RelationshipQueryDescription> {

    /**
     * Construct a new counting node.
     *
     * @param node      backing Neo4j node.
     * @param prefix    of the cached relationship string representation.
     * @param separator of information in the cached relationship string representation.
     */
    public FullCachedRelationshipCountingNode(Node node, String prefix, String separator) {
        super(node, prefix, separator);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean candidateMatchesDescription(CacheableRelationshipDescription candidate, RelationshipQueryDescription description) {
        boolean matches = candidate.isMoreSpecificThan(description);

        if (!matches && !candidate.isMutuallyExclusive(description)) {
            throw new UnableToCountException("Unable to count relationships with the following description: "
                    + description.toString(FrameworkConfiguration.DEFAULT_SEPARATOR)
                    + " Since there are potentially compacted out cached matches," +
                    " it looks like compaction has taken away the granularity you need. Please try to count this kind " +
                    "of relationship with a naive counter. Alternatively, increase the compaction threshold.");
        }

        return matches;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected CacheableRelationshipDescription newCachedRelationship(String string) {
        return new CacheableRelationshipDescriptionImpl(string, prefix, separator);
    }
}
