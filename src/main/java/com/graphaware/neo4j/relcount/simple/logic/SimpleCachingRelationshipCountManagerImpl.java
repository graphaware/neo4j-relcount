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

package com.graphaware.neo4j.relcount.simple.logic;

import com.graphaware.neo4j.dto.common.relationship.HasDirectionAndType;
import com.graphaware.neo4j.dto.common.relationship.SerializableDirectionAndType;
import com.graphaware.neo4j.relcount.common.manager.BaseCachingRelationshipCountManager;

/**
 * Default production implementation of {@link com.graphaware.neo4j.relcount.full.logic.FullCachingRelationshipCountManager}.
 */
public class SimpleCachingRelationshipCountManagerImpl extends BaseCachingRelationshipCountManager<HasDirectionAndType, SerializableDirectionAndType> implements SimpleCachingRelationshipCountManager {

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean shouldBeUsedForLookup(SerializableDirectionAndType cached, HasDirectionAndType relationship) {
        return cached.equals(relationship);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected SerializableDirectionAndType newCachedRelationship(String key) {
        return new SerializableDirectionAndType(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean shouldBeUsedForCaching(SerializableDirectionAndType cached, HasDirectionAndType relationship) {
        return cached.equals(relationship);
    }
}
