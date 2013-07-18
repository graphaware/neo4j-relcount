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

package com.graphaware.neo4j.relcount.full.logic;

import com.graphaware.neo4j.framework.config.FrameworkConfiguration;
import com.graphaware.neo4j.relcount.common.api.UnableToCountException;
import com.graphaware.neo4j.relcount.common.logic.CachedRelationshipCountReader;
import com.graphaware.neo4j.relcount.common.logic.RelationshipCountReader;
import com.graphaware.neo4j.relcount.full.dto.relationship.GeneralRelationshipDescription;
import com.graphaware.neo4j.relcount.full.dto.relationship.LiteralRelationshipDescription;
import com.graphaware.neo4j.relcount.full.dto.relationship.RelationshipDescription;
import org.neo4j.graphdb.Node;

import java.util.Map;

import static com.graphaware.neo4j.relcount.full.dto.property.LiteralPropertiesDescription.LITERAL;

/**
 * Full {@link CachedRelationshipCountReader}.  It is "full" in the sense that it cares about
 * {@link org.neo4j.graphdb.RelationshipType}s, {@link org.neo4j.graphdb.Direction}s, and properties.
 */
public class FullCachedRelationshipCountReader extends CachedRelationshipCountReader<RelationshipDescription> implements RelationshipCountReader<RelationshipDescription> {

    /**
     * Construct a new reader.
     *
     * @param id     of the {@link com.graphaware.neo4j.relcount.full.module.FullRelationshipCountModule} this reader belongs to.
     * @param config of the {@link com.graphaware.neo4j.framework.GraphAwareFramework} that the module is registered with.
     */
    public FullCachedRelationshipCountReader(String id, FrameworkConfiguration config) {
        super(id, config);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean candidateMatchesDescription(RelationshipDescription candidate, RelationshipDescription description) {
        return candidate.isMoreSpecificThan(description);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean continueAfterFirstLookupMatch() {
        return true; //need to continue, there might be other more general matches
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected RelationshipDescription newCachedRelationship(String string, String prefix, String separator) {
        RelationshipDescription result = new GeneralRelationshipDescription(string, prefix, separator);

        if (result.getProperties().containsKey(LITERAL)) {
            return new LiteralRelationshipDescription(result);
        }

        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void handleZeroResult(RelationshipDescription description, Node node) {
        for (Map.Entry<RelationshipDescription, Integer> candidateWithCount : getCandidates(description, node).entrySet()) {
            RelationshipDescription candidate = candidateWithCount.getKey();
            if (candidate.isMoreGeneralThan(description)) {
                throw new UnableToCountException("Unable to count relationships with the following description: "
                        + description.toString(FrameworkConfiguration.DEFAULT_SEPARATOR)
                        + " for node " + node.toString() + ". Since there are cached matches more general than your description," +
                        " it looks like compaction has taken away the granularity you need. Please try to count this kind " +
                        "of relationship with a naive counter. Alternatively, increase the compaction threshold.");
            }
        }
    }
}
