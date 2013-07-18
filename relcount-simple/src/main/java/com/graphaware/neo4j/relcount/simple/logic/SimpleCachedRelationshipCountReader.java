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
import org.apache.log4j.Logger;
import org.neo4j.graphdb.Node;

/**
 * A simple {@link CachedRelationshipCountReader}. It must be used in conjunction with {@link com.graphaware.neo4j.relcount.simple.module.SimpleRelationshipCountModule}
 * registered with {@link com.graphaware.neo4j.framework.GraphAwareFramework}.
 * <p/>
 * It is simple in the sense that it only cares about {@link org.neo4j.graphdb.RelationshipType}s
 * and {@link org.neo4j.graphdb.Direction}; it completely ignores {@link org.neo4j.graphdb.Relationship} properties.
 */
public class SimpleCachedRelationshipCountReader extends CachedRelationshipCountReader<TypeAndDirectionDescription> implements RelationshipCountReader<TypeAndDirectionDescription> {

    private static final Logger LOG = Logger.getLogger(SimpleCachedRelationshipCountReader.class);

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
        return false; //there can only be one cached value per type-direction combination => first match is all we need
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected TypeAndDirectionDescription newCachedRelationship(String string, String prefix, String separator) {
        return new TypeAndDirectionDescriptionImpl(string, prefix, separator);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void handleZeroResult(TypeAndDirectionDescription description, Node node) {
        LOG.debug("No relationships with description " + description.toString() + " have been found. This could mean that either" +
                " there really are none, or that you are using a RelationshipInclusionStrategy that excludes relationships " +
                " with this description, or that that database has been running without SimpleRelationshipCountModule" +
                " registered. If you're suspecting the last is the case, please register the module with GraphAwareFramework " +
                " and force re-initialization (refer to GraphAwareFramework javadoc)");
    }
}
