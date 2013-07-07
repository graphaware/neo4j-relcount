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

import com.graphaware.neo4j.dto.common.relationship.DirectedRelationship;
import com.graphaware.neo4j.relcount.common.manager.BaseCachingRelationshipCountManager;
import com.graphaware.neo4j.relcount.full.dto.ComparableRelationship;

/**
 * Default production implementation of {@link FullCachingRelationshipCountManager}.
 */
public class FullCachingRelationshipCountManager extends BaseCachingRelationshipCountManager<DirectedRelationship, ComparableRelationship> {

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean shouldBeUsedForLookup(ComparableRelationship cached, DirectedRelationship relationship) {
        //use every more general match of the looked-up relationship
        return cached.isMoreSpecificThan(relationship);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean continueAfterFirstLookupMatch() {
        //need to continue, there might be other more general matches
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ComparableRelationship newCachedRelationship(String key) {
        return new ComparableRelationship(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean shouldBeUsedForCaching(ComparableRelationship cached, DirectedRelationship relationship) {
        //use the most specific match of the about-to-be-cached relationship
        return cached.isMoreGeneralThan(relationship);
    }
}
