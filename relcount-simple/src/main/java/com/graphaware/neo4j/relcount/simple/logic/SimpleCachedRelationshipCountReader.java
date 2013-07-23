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

import com.graphaware.neo4j.framework.config.FrameworkConfiguration;
import com.graphaware.neo4j.relcount.common.logic.CachedRelationshipCountReader;
import com.graphaware.neo4j.relcount.common.logic.RelationshipCountReader;
import com.graphaware.neo4j.relcount.simple.dto.TypeAndDirectionDescription;
import com.graphaware.neo4j.relcount.simple.dto.TypeAndDirectionDescriptionImpl;

/**
 * A simple {@link CachedRelationshipCountReader}. It must be used in conjunction with {@link com.graphaware.neo4j.relcount.simple.module.SimpleRelationshipCountModule}
 * registered with {@link com.graphaware.neo4j.framework.GraphAwareFramework}.
 * <p/>
 * It is simple in the sense that it only cares about {@link org.neo4j.graphdb.RelationshipType}s
 * and {@link org.neo4j.graphdb.Direction}; it completely ignores {@link org.neo4j.graphdb.Relationship} properties.
 */
public class SimpleCachedRelationshipCountReader extends CachedRelationshipCountReader<TypeAndDirectionDescription, TypeAndDirectionDescription> implements RelationshipCountReader<TypeAndDirectionDescription> {

    /**
     * Construct a new reader.
     *
     * @param id     of the {@link com.graphaware.neo4j.relcount.simple.module.SimpleRelationshipCountModule} this reader belongs to.
     * @param config of the {@link com.graphaware.neo4j.framework.GraphAwareFramework} that the module is registered with.
     */
    public SimpleCachedRelationshipCountReader(String id, FrameworkConfiguration config) {
        super(id, config);
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
        return true; //there can only be one cached value per type-direction combination, but if the user asks for BOTH direction, there could be 2
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected TypeAndDirectionDescription newCachedRelationship(String string, String prefix, String separator) {
        return new TypeAndDirectionDescriptionImpl(string, prefix, separator);
    }
}
