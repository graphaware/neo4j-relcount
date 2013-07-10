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

package com.graphaware.neo4j.relcount.full.manager;

import com.graphaware.neo4j.relcount.common.api.UnableToCountException;
import com.graphaware.neo4j.relcount.common.manager.BaseCachingRelationshipCountManager;
import com.graphaware.neo4j.relcount.common.manager.CachingRelationshipCountManager;
import com.graphaware.neo4j.relcount.full.dto.relationship.GeneralRelationshipDescription;
import com.graphaware.neo4j.relcount.full.dto.relationship.LiteralRelationshipDescription;
import com.graphaware.neo4j.relcount.full.dto.relationship.RelationshipDescription;
import org.neo4j.graphdb.Node;

import java.util.Map;

import static com.graphaware.neo4j.relcount.full.dto.property.LiteralPropertiesDescription.LITERAL;

public class FullCachingRelationshipCountManager extends BaseCachingRelationshipCountManager<RelationshipDescription> implements CachingRelationshipCountManager<RelationshipDescription> {

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean candidateMatchesDescription(RelationshipDescription candidate, RelationshipDescription description) {
        return candidate.isMoreSpecificThan(description);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean continueAfterFirstLookupMatch() {
        //need to continue, there might be other more general matches
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean cachedMatch(RelationshipDescription cached, RelationshipDescription relationship) {
        return cached.isMoreGeneralThan(relationship);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected RelationshipDescription newCachedRelationship(String string) {
        RelationshipDescription result = new GeneralRelationshipDescription(string);

        if (result.getProperties().containsKey(LITERAL)) {
            return new LiteralRelationshipDescription(result);
        }

        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void handleZeroResult(RelationshipDescription description, Node node) {
        for (Map.Entry<RelationshipDescription, Integer> candidateWithCount : getRelationshipCounts(description, node).entrySet()) {
            RelationshipDescription candidate = candidateWithCount.getKey();
            if (candidate.isMoreGeneralThan(description)) {
                throw new UnableToCountException("Unable to count relationships with the following description: "
                        + description.toString()
                        + " for node " + node.toString());
            }
        }
    }

}
