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

import com.graphaware.neo4j.relcount.common.manager.BaseNaiveRelationshipCountManager;
import com.graphaware.neo4j.relcount.common.manager.RelationshipCountManager;
import com.graphaware.neo4j.relcount.simple.dto.TypeAndDirectionDescription;
import com.graphaware.neo4j.relcount.simple.dto.TypeAndDirectionDescriptionImpl;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * {@link RelationshipCountManager} that counts relationships by traversing them (performs no caching). It is simple in
 * the sense that it only cares about {@link org.neo4j.graphdb.RelationshipType}s and {@link org.neo4j.graphdb.Direction};
 * it completely ignores {@link Relationship} properties.
 */
public class SimpleNaiveRelationshipCountManager extends BaseNaiveRelationshipCountManager<TypeAndDirectionDescription> implements RelationshipCountManager<TypeAndDirectionDescription> {

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
        return true; //it is naive => need to traverse all relationship to find all matches.
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected TypeAndDirectionDescription newCandidate(Relationship relationship, Node pointOfView) {
        return new TypeAndDirectionDescriptionImpl(relationship, pointOfView);
    }
}
