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

package com.graphaware.neo4j.relcount.simple.manager;

import com.graphaware.neo4j.relcount.common.manager.BaseCachingRelationshipCountManager;
import com.graphaware.neo4j.relcount.common.manager.CachingRelationshipCountManager;
import com.graphaware.neo4j.relcount.simple.dto.TypeAndDirectionDescription;
import com.graphaware.neo4j.relcount.simple.dto.TypeAndDirectionDescriptionImpl;
import org.apache.log4j.Logger;
import org.neo4j.graphdb.Node;

/**
 * A simple implementation of {@link CachingRelationshipCountManager}. It is simple in
 * the sense that it only cares about {@link org.neo4j.graphdb.RelationshipType}s and {@link org.neo4j.graphdb.Direction};
 * it completely ignores {@link org.neo4j.graphdb.Relationship} properties.
 */
public class SimpleCachingRelationshipCountManager extends BaseCachingRelationshipCountManager<TypeAndDirectionDescription> implements CachingRelationshipCountManager<TypeAndDirectionDescription> {

    private static final Logger LOG = Logger.getLogger(SimpleCachingRelationshipCountManager.class);

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
    protected TypeAndDirectionDescription newCachedRelationship(String string) {
        return new TypeAndDirectionDescriptionImpl(string);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean cachedMatch(TypeAndDirectionDescription cached, TypeAndDirectionDescription relationship) {
        return cached.matches(relationship);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void handleZeroResult(TypeAndDirectionDescription description, Node node) {
        LOG.debug("No relationships with description " + description.toString() + " have been found. This could mean that either" +
                " there really are none, or that you are using a RelationshipInclusionStrategy that excludes relationships " +
                " with this description, or that that database has been running without SimpleRelationshipCountTransactionEventHandler" +
                " registered. If you're suspecting the last is the case, please register the handler and call the recalculate() " +
                " method on it once.");
    }
}
