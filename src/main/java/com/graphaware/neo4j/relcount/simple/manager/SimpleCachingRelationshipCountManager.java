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

import com.graphaware.neo4j.dto.common.relationship.HasTypeAndDirection;
import com.graphaware.neo4j.dto.common.relationship.SerializableTypeAndDirection;
import com.graphaware.neo4j.relcount.common.manager.BaseCachingRelationshipCountManager;
import com.graphaware.neo4j.relcount.common.manager.CachingRelationshipCountManager;
import org.neo4j.graphdb.Node;

/**
 * Default production implementation of {@link com.graphaware.neo4j.relcount.full.manager.FullCachingRelationshipCountManager}.
 */
public class SimpleCachingRelationshipCountManager extends BaseCachingRelationshipCountManager<HasTypeAndDirection, SerializableTypeAndDirection> implements CachingRelationshipCountManager<HasTypeAndDirection, SerializableTypeAndDirection> {

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean candidateMatchesDescription(SerializableTypeAndDirection candidate, HasTypeAndDirection description) {
        return candidate.matches(description);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean continueAfterFirstLookupMatch() {
        //there is only one cached value per disjunt type => first match is all we need
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected SerializableTypeAndDirection newCachedRelationship(String string) {
        return new SerializableTypeAndDirection(string);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean cachedMatch(SerializableTypeAndDirection cached, SerializableTypeAndDirection relationship) {
        return cached.matches(relationship);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void handleZeroResult(HasTypeAndDirection hasTypeAndDirection, Node node) {
        //do nothing
    }
}
