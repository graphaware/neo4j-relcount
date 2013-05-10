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

package com.graphaware.neo4j.relcount.api;

import com.graphaware.neo4j.representation.property.MakesCopyWithProperty;
import com.graphaware.neo4j.representation.property.MakesCopyWithPropertyProperties;
import com.graphaware.neo4j.representation.relationship.Relationship;
import org.neo4j.graphdb.Node;

/**
 * A relationship counter with fluent interface allowing a "description" of a relationship to be constructed by calling
 * the constructor and then successively {@link #with(String, String)} to add properties. Finally, by calling
 * {@link #count(org.neo4j.graphdb.Node)}, relationship count for the specified relationship on the node is counted.
 * <p/>
 * If the relationship description doesn't correspond to any cached relationship count, 0 will be returned.
 */
public interface RelationshipCounter extends Relationship<MakesCopyWithPropertyProperties>, MakesCopyWithProperty<RelationshipCounter> {

    /**
     * Count relationships described by this counter on the given node.
     *
     * @param node on which to count relationships.
     * @return number of relationships or 0 if not cached.
     */
    int count(Node node);
}
