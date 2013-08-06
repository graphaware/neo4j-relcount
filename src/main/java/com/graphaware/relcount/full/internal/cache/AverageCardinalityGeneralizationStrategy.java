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

package com.graphaware.relcount.full.internal.cache;

import com.graphaware.propertycontainer.dto.common.relationship.HasType;
import com.graphaware.propertycontainer.dto.common.relationship.HasTypeAndDirection;
import com.graphaware.propertycontainer.dto.common.relationship.Type;
import com.graphaware.propertycontainer.dto.common.relationship.TypeAndDirection;
import com.graphaware.relcount.full.internal.dto.property.CompactiblePropertiesImpl;
import com.graphaware.relcount.full.internal.dto.relationship.CompactibleRelationship;

import java.util.*;

/**
 * A {@link GeneralizationStrategy} with an "average property cardinality" heuristic.
 * <p/>
 * A human-friendly explanation of what this strategy is trying to achieve is getting rid of (generalizing) properties with
 * frequently changing values (like timestamp on a relationship), whilst keeping the ones that change less frequently,
 * thus providing more value (like strength of a friendship).
 */
public class AverageCardinalityGeneralizationStrategy implements GeneralizationStrategy {

    /**
     * All possible property keys of relationships with the given type and direction.
     */
    private final Map<HasTypeAndDirection, Collection<String>> keysByTypeAndDirection = new HashMap<>();

    /**
     * All possible property keys of relationships with the given type.
     */
    private final Map<HasType, Collection<String>> keysByType = new HashMap<>();

    /**
     * Number of a relationships with the given type.
     */
    private final Map<HasType, Integer> degreeByType = new HashMap<>();

    /**
     * All possible values of a property of relationships with the given type. Null is also a possible value.
     * Wildcard isn't a possible value for this Map, see below.
     */
    private final Map<HasType, Map<String, Set<String>>> valuesByTypeAndKey = new HashMap<>();

    /**
     * Maximum possible number of different values of a property of relationships with the given type, judged
     * by looking at wildcards in the cached counts. For example, if there is a cached count for relationship
     * FRIEND_OF#OUTGOING#TIMESTAMP#_ANY_ = 5, then 5 different timestamp values are assumed.
     */
    private final Map<HasType, Map<String, Integer>> wildcardsByTypeAndKey = new HashMap<>();

    /**
     * {@inheritDoc}
     */
    @Override
    public List<CompactibleRelationship> produceGeneralizations(Map<CompactibleRelationship, Integer> cachedCounts) {
        populateKeysAndDegree(cachedCounts);
        populateValuesAndWildcards(cachedCounts);

        List<CompactibleRelationship> result = new LinkedList<>();
        for (ScoredCompactibleRelationship scoredGeneralization : produceScoredGeneralizations(cachedCounts)) {
            result.add(scoredGeneralization.getCompactibleRelationship());
        }

        return result;
    }

    /**
     * Pass through the cached counts and populate {@link #keysByTypeAndDirection} and {@link #keysByType}.
     *
     * @param cachedCounts to analyze.
     */
    private void populateKeysAndDegree(Map<CompactibleRelationship, Integer> cachedCounts) {
        for (Map.Entry<CompactibleRelationship, Integer> cachedCountEntry : cachedCounts.entrySet()) {
            CompactibleRelationship cachedCount = cachedCountEntry.getKey();

            HasTypeAndDirection typeAndDirection = new TypeAndDirection(cachedCount);
            if (!keysByTypeAndDirection.containsKey(typeAndDirection)) {
                keysByTypeAndDirection.put(typeAndDirection, new HashSet<String>());
            }
            keysByTypeAndDirection.get(typeAndDirection).addAll(cachedCount.getProperties().keySet());

            HasType type = new Type(cachedCount);
            if (!keysByType.containsKey(type)) {
                keysByType.put(type, new HashSet<String>());
            }
            keysByType.get(type).addAll(cachedCount.getProperties().keySet());

            if (!degreeByType.containsKey(type)) {
                degreeByType.put(type, 0);
            }
            degreeByType.put(type, degreeByType.get(type) + cachedCountEntry.getValue());
        }
    }

    /**
     * Pass through the cached counts and populate {@link #valuesByTypeAndKey} and {@link #wildcardsByTypeAndKey}.
     *
     * @param cachedCounts to analyze.
     */
    private void populateValuesAndWildcards(Map<CompactibleRelationship, Integer> cachedCounts) {
        for (Map.Entry<CompactibleRelationship, Integer> cachedCountEntry : cachedCounts.entrySet()) {
            CompactibleRelationship cachedCount = cachedCountEntry.getKey();
            Type cachedCountType = new Type(cachedCount);

            if (!valuesByTypeAndKey.containsKey(cachedCountType)) {
                valuesByTypeAndKey.put(cachedCountType, new HashMap<String, Set<String>>());
            }
            if (!wildcardsByTypeAndKey.containsKey(cachedCountType)) {
                wildcardsByTypeAndKey.put(cachedCountType, new HashMap<String, Integer>());
            }

            for (String key : keysByType.get(cachedCountType)) {
                Map<String, Set<String>> valuesByKey = valuesByTypeAndKey.get(cachedCountType);
                Map<String, Integer> wildcardsByKey = wildcardsByTypeAndKey.get(cachedCountType);

                if (!valuesByKey.containsKey(key)) {
                    valuesByKey.put(key, new HashSet<String>());
                }

                if (!wildcardsByKey.containsKey(key)) {
                    wildcardsByKey.put(key, 0);
                }

                if (!cachedCount.getProperties().containsKey(key)) {
                    valuesByKey.get(key).add(null);
                    continue;
                }

                if (cachedCount.getProperties().get(key).equals(CompactiblePropertiesImpl.ANY_VALUE)) {
                    wildcardsByKey.put(key, wildcardsByKey.get(key) + cachedCountEntry.getValue());
                    continue;
                }

                valuesByKey.get(key).add(cachedCount.getProperties().get(key));
            }
        }
    }

    /**
     * Produce all generalizations of the cached counts and score them.
     *
     * @param cachedCounts to generalize.
     * @return scored generalizations sorted by descending score.
     */
    private Set<ScoredCompactibleRelationship> produceScoredGeneralizations(Map<CompactibleRelationship, Integer> cachedCounts) {
        Set<ScoredCompactibleRelationship> result = new TreeSet<>();

        //Generate all possible generalizations
        Set<CompactibleRelationship> generalizations = new HashSet<>();
        for (CompactibleRelationship cached : cachedCounts.keySet()) {
            generalizations.addAll(cached.generateAllMoreGeneral(keysByTypeAndDirection.get(new TypeAndDirection(cached))));
        }

        for (CompactibleRelationship generalization : generalizations) {
            double score = 1.0;

            HasType type = new Type(generalization);
            Map<String, Integer> wildcardsByKey = wildcardsByTypeAndKey.get(type);
            Map<String, Set<String>> valuesByKey = valuesByTypeAndKey.get(type);

            Collection<String> keys = keysByType.get(type);
            for (String key : keys) {
                if (!generalization.getProperties().containsKey(key) || !CompactiblePropertiesImpl.ANY_VALUE.equals(generalization.getProperties().get(key))) {
                    score = score + 1;
                } else {
                    int totalCount = degreeByType.get(type);
                    int noWildcardValues = wildcardsByKey.get(key); //maximum number of values "contained" by the wildcard
                    int noConcreteValues = valuesByKey.get(key).size();
                    score = score + (((double) (noWildcardValues + noConcreteValues)) / (totalCount + 1));
                }
            }

            score = score / (keys.size() + 1);

            result.add(new ScoredCompactibleRelationship(generalization, score));
        }

        return result;
    }

    /**
     * Encapsulation of a {@link CompactibleRelationship} (a generalization) and its score. Comparable so that objects
     * with the highest score come first.
     */
    private class ScoredCompactibleRelationship implements Comparable<ScoredCompactibleRelationship> {

        private final CompactibleRelationship compactibleRelationship;
        private final double score;

        private ScoredCompactibleRelationship(CompactibleRelationship compactibleRelationship, double score) {
            this.compactibleRelationship = compactibleRelationship;
            this.score = score;
        }

        public CompactibleRelationship getCompactibleRelationship() {
            return compactibleRelationship;
        }

        @Override
        public int compareTo(ScoredCompactibleRelationship o) {
            if (score > o.score) {
                return -1;
            }
            if (score < o.score) {
                return 1;
            }
            return compactibleRelationship.compareTo(o.compactibleRelationship);
        }
    }
}
