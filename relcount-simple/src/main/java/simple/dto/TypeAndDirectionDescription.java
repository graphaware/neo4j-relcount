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

package simple.dto;

import com.graphaware.neo4j.dto.common.relationship.SerializableTypeAndDirection;

/**
 * A description of a {@link org.neo4j.graphdb.Relationship}'s {@link org.neo4j.graphdb.RelationshipType} and {@link org.neo4j.graphdb.Direction}
 * for the purposes of counting {@link org.neo4j.graphdb.Relationship}s with that description, caching such counts, etc.
 */
public interface TypeAndDirectionDescription extends SerializableTypeAndDirection, Comparable<TypeAndDirectionDescription> {
}
