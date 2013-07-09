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

package com.graphaware.neo4j.relcount.full.manager;

import com.graphaware.neo4j.dto.common.property.ImmutableProperties;
import com.graphaware.neo4j.dto.common.relationship.ImmutableDirectedRelationship;
import com.graphaware.neo4j.relcount.common.manager.BaseCachingRelationshipCountManager;
import com.graphaware.neo4j.relcount.common.manager.CachingRelationshipCountManager;
import com.graphaware.neo4j.relcount.full.dto.relationship.CountableRelationship;
import com.graphaware.neo4j.relcount.full.dto.relationship.GenerallyCountableRelationship;

/**
 * Default production implementation of {@link FullCachingRelationshipCountManager}.
 */
public class FullCachingRelationshipCountManager extends BaseCachingRelationshipCountManager<ImmutableDirectedRelationship<String, ? extends ImmutableProperties<String>>, CountableRelationship> implements CachingRelationshipCountManager<ImmutableDirectedRelationship<String, ? extends ImmutableProperties<String>>, CountableRelationship> {

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean candidateMatchesDescription(CountableRelationship candidate, ImmutableDirectedRelationship<String, ? extends ImmutableProperties<String>> description) {
        return candidate.isMoreSpecificThan(description);
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
    protected boolean cachedMatch(CountableRelationship cached, CountableRelationship relationship) {
        return cached.isMoreGeneralThan(relationship);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected GenerallyCountableRelationship newCachedRelationship(String string) {
        return new GenerallyCountableRelationship(string);
    }
}
