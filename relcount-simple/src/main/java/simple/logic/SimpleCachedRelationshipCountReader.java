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

package simple.logic;

import com.graphaware.neo4j.relcount.common.logic.CachedRelationshipCountReader;
import com.graphaware.neo4j.relcount.common.logic.RelationshipCountReader;
import org.apache.log4j.Logger;
import org.neo4j.graphdb.Node;
import simple.dto.TypeAndDirectionDescription;
import simple.dto.TypeAndDirectionDescriptionImpl;

/**
 * A simple {@link CachedRelationshipCountReader}. It is simple in the sense that it only cares about {@link org.neo4j.graphdb.RelationshipType}s
 * and {@link org.neo4j.graphdb.Direction}; it completely ignores {@link org.neo4j.graphdb.Relationship} properties.
 */
public class SimpleCachedRelationshipCountReader extends CachedRelationshipCountReader<TypeAndDirectionDescription> implements RelationshipCountReader<TypeAndDirectionDescription> {

    private static final Logger LOG = Logger.getLogger(SimpleCachedRelationshipCountReader.class);

    public SimpleCachedRelationshipCountReader(String id) {
        super(id);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean candidateMatchesDescription(TypeAndDirectionDescription candidate, TypeAndDirectionDescription description) {
        return candidate.matches(description);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean continueAfterFirstLookupMatch() {
        return false; //there can only be one cached value per type-direction combination => first match is all we need
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected TypeAndDirectionDescription newCachedRelationship(String string, String prefix) {
        return new TypeAndDirectionDescriptionImpl(string, prefix);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void handleZeroResult(TypeAndDirectionDescription description, Node node) {
        LOG.debug("No relationships with description " + description.toString() + " have been found. This could mean that either" +
                " there really are none, or that you are using a RelationshipInclusionStrategy that excludes relationships " +
                " with this description, or that that database has been running without RelationshipCountTransactionEventHandler" +
                " registered. If you're suspecting the last is the case, please register the handler and call the rebuildCachedCounts() " +
                " method on an instance of SimpleRelationshipCountCache once.");
    }
}
