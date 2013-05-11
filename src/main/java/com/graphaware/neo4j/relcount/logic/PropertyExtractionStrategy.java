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

import org.neo4j.graphdb.Node;

import java.util.Map;

/**
 * A strategy for extracting properties from information about an actual {@link org.neo4j.graphdb.Relationship}.
 * <p/>
 * The intention is to allow subclasses to explicitly ignore certain relationship properties, or create "derived" or
 * "fake" properties by (for example) taking some properties or even labels? from the other node participating in the
 * relationship.
 */
public interface PropertyExtractionStrategy {

    /**
     * Extract properties from a relationship for the purposes of caching the relationship's count on a node (a.k.a. "this node").
     *
     * @param properties attached to the relationship. Don't modify these (you'll get an exception), create a new map instead.
     * @param otherNode  the other node participating in the relationship. By "other", we mean NOT the node on which
     *                   the relationship counts for this relationship are being updated as a part of this call.
     * @return extracted properties for count caching.
     */
    Map<String, String> extractProperties(Map<String, String> properties, Node otherNode);
}
