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

package com.graphaware.neo4j.relcount.common.logic;

import com.graphaware.neo4j.dto.common.relationship.HasTypeAndDirection;
import org.neo4j.graphdb.Node;

import java.util.Map;

/**
 * Base class for {@link RelationshipCountReader} implementations.
 *
 * @param <CANDIDATE>   type of candidate relationship representation.
 * @param <DESCRIPTION> type of relationship description that can be used to query relationship counts for nodes.
 */
public abstract class BaseRelationshipCountReader<CANDIDATE extends HasTypeAndDirection, DESCRIPTION extends HasTypeAndDirection> {

    /**
     * Get a relationship count for a node. The count is the sum of all the counts where {@link #candidateMatchesDescription(com.graphaware.neo4j.dto.common.relationship.HasTypeAndDirection, com.graphaware.neo4j.dto.common.relationship.HasTypeAndDirection)},
     * unless {@link #continueAfterFirstLookupMatch()} returns <code>false</code>, in which case it is just the first
     * matching value found.
     *
     * @param description of the relationship for which to get count.
     * @param node        for which to get relationship count.
     * @return count, 0 if there are no such relationships.
     */
    public int getRelationshipCount(DESCRIPTION description, Node node) {
        int result = 0;
        for (Map.Entry<CANDIDATE, Integer> candidateWithCount : getCandidates(description, node).entrySet()) {
            CANDIDATE candidate = candidateWithCount.getKey();
            if (candidateMatchesDescription(candidate, description)) {
                result += candidateWithCount.getValue();
                if (!continueAfterFirstLookupMatch()) {
                    return result;
                }
            }
        }

        return result;
    }

    /**
     * Get all candidate relationship counts for a node.
     *
     * @param description description of the relationship for which to get count. Can be used to guide search,
     *                    but can well be ignored. Thus, there is no guarantee that all returned candidates match the description.
     * @param node        for which to get relationship count.
     * @return relationship counts (key = candidate relationship, value = count).
     */
    protected abstract Map<CANDIDATE, Integer> getCandidates(DESCRIPTION description, Node node);

    /**
     * Does the given candidate match the relationship description?
     *
     * @param candidate   candidate that could correspond to the given relationship description.
     * @param description of the relationships being counted.
     * @return true iff the candidate matches the description and should thus be taken into account.
     */
    protected abstract boolean candidateMatchesDescription(CANDIDATE candidate, DESCRIPTION description);

    /**
     * When counting relationships, should the search through candidates continue after the candidate has been found?
     *
     * @return true to continue, false to return the first candidate's count.
     */
    protected abstract boolean continueAfterFirstLookupMatch();
}
