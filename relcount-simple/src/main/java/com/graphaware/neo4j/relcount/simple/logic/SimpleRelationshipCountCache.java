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

import com.graphaware.neo4j.framework.NeedsInitializationException;
import com.graphaware.neo4j.framework.config.FrameworkConfiguration;
import com.graphaware.neo4j.relcount.common.logic.BaseRelationshipCountCache;
import com.graphaware.neo4j.relcount.common.logic.RelationshipCountCache;
import com.graphaware.neo4j.relcount.simple.dto.TypeAndDirectionDescription;
import com.graphaware.neo4j.relcount.simple.dto.TypeAndDirectionDescriptionImpl;
import com.graphaware.neo4j.utils.DirectionUtils;
import org.apache.log4j.Logger;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * A simple implementation of {@link com.graphaware.neo4j.relcount.common.logic.RelationshipCountCache}. It is simple in
 * the sense that it only cares about {@link org.neo4j.graphdb.RelationshipType}s and {@link org.neo4j.graphdb.Direction}s;
 * it completely ignores {@link org.neo4j.graphdb.Relationship} properties.
 */
public class SimpleRelationshipCountCache extends BaseRelationshipCountCache<TypeAndDirectionDescription> implements RelationshipCountCache {

    private static final Logger LOG = Logger.getLogger(SimpleRelationshipCountCache.class);

    /**
     * Construct a new cache.
     *
     * @param id of the module this cache belongs to.
     */
    public SimpleRelationshipCountCache(String id) {
        super(id);
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
    protected boolean cachedMatch(TypeAndDirectionDescription cached, TypeAndDirectionDescription relationship) {
        return cached.matches(relationship);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleCreatedRelationship(Relationship relationship, Node pointOfView, Direction defaultDirection) {
        throwExceptionIfDirectionIsNullOrBoth(defaultDirection);

        TypeAndDirectionDescription createdRelationship = new TypeAndDirectionDescriptionImpl(relationship.getType(), DirectionUtils.resolveDirection(relationship, pointOfView, defaultDirection));

        incrementCount(createdRelationship, pointOfView, 1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleDeletedRelationship(Relationship relationship, Node pointOfView, Direction defaultDirection) {
        throwExceptionIfDirectionIsNullOrBoth(defaultDirection);

        TypeAndDirectionDescription deletedRelationship = new TypeAndDirectionDescriptionImpl(relationship.getType(), DirectionUtils.resolveDirection(relationship, pointOfView, defaultDirection));

        if (!decrementCount(deletedRelationship, pointOfView, 1)) {
            LOG.warn(deletedRelationship.toString(FrameworkConfiguration.DEFAULT_SEPARATOR) + " was out of sync on node " + pointOfView.getId());
            throw new NeedsInitializationException(deletedRelationship.toString(FrameworkConfiguration.DEFAULT_SEPARATOR) + " was out of sync on node " + pointOfView.getId());
        }
    }
}
