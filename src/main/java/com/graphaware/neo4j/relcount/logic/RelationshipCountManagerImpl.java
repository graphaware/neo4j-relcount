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

package com.graphaware.neo4j.relcount.logic;

import com.graphaware.neo4j.relcount.representation.ComparableRelationship;
import com.graphaware.neo4j.representation.relationship.Relationship;
import org.neo4j.graphdb.Node;

import java.util.Map;
import java.util.TreeMap;

import static com.graphaware.neo4j.utils.Constants.GA_REL_PREFIX;

/**
 * Default production implementation of {@link RelationshipCountManager}.
 */
public class RelationshipCountManagerImpl implements RelationshipCountManager {

    /**
     * {@inheritDoc}
     */
    @Override
    public int getRelationshipCount(Relationship relationship, Node node) {
        int result = 0;
        for (Map.Entry<ComparableRelationship, Integer> cachedRelationshipCount : getRelationshipCounts(node).entrySet()) {
            if (cachedRelationshipCount.getKey().isMoreSpecificThan(relationship)) {
                result += cachedRelationshipCount.getValue();
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<ComparableRelationship, Integer> getRelationshipCounts(Node node) {
        Map<ComparableRelationship, Integer> result = new TreeMap<ComparableRelationship, Integer>();
        for (String key : node.getPropertyKeys()) {
            if (key.startsWith(GA_REL_PREFIX)) {
                result.put(new ComparableRelationship(key), (Integer) node.getProperty(key));
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean incrementCount(ComparableRelationship relationship, Node node) {
        return incrementCount(relationship, node, 1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean incrementCount(ComparableRelationship relationship, Node node, int delta) {

        //Increment count for the most specific match of the new relationship
        for (ComparableRelationship cachedRelationship : getRelationshipCounts(node).keySet()) {
            if (cachedRelationship.isMoreGeneralThan(relationship)) {
                node.setProperty(cachedRelationship.toString(), (Integer) node.getProperty(cachedRelationship.toString()) + delta);
                return false;
            }
        }

        node.setProperty(relationship.toString(), delta);
        return true;

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean decrementCount(ComparableRelationship relationship, Node node) {
        return decrementCount(relationship, node, 1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean decrementCount(ComparableRelationship relationship, Node node, int delta) {

        //Decrement count for the most specific match of the new relationship
        for (ComparableRelationship cachedRelationship : getRelationshipCounts(node).keySet()) {
            if (cachedRelationship.isMoreGeneralThan(relationship)) {
                int newValue = (Integer) node.getProperty(cachedRelationship.toString()) - delta;
                node.setProperty(cachedRelationship.toString(), newValue);

                if (newValue <= 0) {
                    deleteCount(cachedRelationship, node);
                }

                return newValue >= 0;
            }
        }

        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteCount(ComparableRelationship relationship, Node node) {
        node.removeProperty(relationship.toString());
    }
}
