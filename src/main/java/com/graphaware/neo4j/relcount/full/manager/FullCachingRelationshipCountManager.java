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

import com.graphaware.neo4j.dto.common.relationship.HasTypeDirectionAndProperties;
import com.graphaware.neo4j.dto.string.relationship.SerializableDirectedRelationship;
import com.graphaware.neo4j.dto.string.relationship.SerializableDirectedRelationshipImpl;
import com.graphaware.neo4j.relcount.common.api.UnableToCountException;
import com.graphaware.neo4j.relcount.common.manager.BaseCachingRelationshipCountManager;
import com.graphaware.neo4j.relcount.common.manager.CachingRelationshipCountManager;
import com.graphaware.neo4j.relcount.full.dto.relationship.CandidateGeneralizedRelationship;
import com.graphaware.neo4j.relcount.full.dto.relationship.CandidateLiteralRelationship;
import com.graphaware.neo4j.relcount.full.dto.relationship.CandidateRelationship;
import org.neo4j.graphdb.Node;

import java.util.Map;

import static com.graphaware.neo4j.relcount.full.dto.property.CandidateLiteralProperties.LITERAL;

public class FullCachingRelationshipCountManager extends BaseCachingRelationshipCountManager<HasTypeDirectionAndProperties<String, ?>, CandidateRelationship> implements CachingRelationshipCountManager<HasTypeDirectionAndProperties<String, ?>, CandidateRelationship> {

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean candidateMatchesDescription(CandidateRelationship candidate, HasTypeDirectionAndProperties<String, ?> description) {
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
    protected boolean cachedMatch(CandidateRelationship cached, CandidateRelationship relationship) {
        return cached.isMoreGeneralThan(relationship);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected CandidateRelationship newCachedRelationship(String string) {
        CandidateRelationship result = new CandidateGeneralizedRelationship(string);

        if (result.getProperties().containsKey(LITERAL)) {
            return new CandidateLiteralRelationship(result);
        }

        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void handleZeroResult(HasTypeDirectionAndProperties<String, ?> description, Node node) {
        for (Map.Entry<CandidateRelationship, Integer> candidateWithCount : getRelationshipCounts(description, node).entrySet()) {
            CandidateRelationship candidate = candidateWithCount.getKey();
            if (candidate.isMoreGeneralThan(description)) {
                throw new UnableToCountException("Unable to count relationships with the following description: "
                        + (description instanceof SerializableDirectedRelationship ? description.toString() : new SerializableDirectedRelationshipImpl(description).toString())
                        + " for node " + node.toString());
            }
        }
    }

}
