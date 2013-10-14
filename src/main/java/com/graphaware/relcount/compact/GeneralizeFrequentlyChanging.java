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

package com.graphaware.relcount.compact;

import com.graphaware.description.predicate.Predicate;
import com.graphaware.description.relationship.DetachedRelationshipDescription;

import java.util.*;

import static com.graphaware.description.predicate.Predicates.any;
import static com.graphaware.description.predicate.Predicates.undefined;

/**
 * A {@link com.graphaware.relcount.compact.GeneralizationStrategy} with a "property change frequency" heuristic.
 * <p/>
 * A human-friendly explanation of what this strategy is trying to achieve is getting rid of (generalizing) properties with
 * frequently changing values (like timestamp on a relationship), whilst keeping the ones that change less frequently,
 * thus providing more value (like strength of a friendship).
 */
public class GeneralizeFrequentlyChanging implements GeneralizationStrategy {

    /**
     * {@inheritDoc}
     */
    @Override
    public DetachedRelationshipDescription produceGeneralization(Map<DetachedRelationshipDescription, Integer> cachedDegrees) {
        Map<String, Map<String, Set<Predicate>>> valuesByTypeAndKey = new HashMap<>();
        Map<String, Integer> degreeByType = new HashMap<>();
        Map<String, Map<String, Integer>> wildcardsByTypeAndKey = new HashMap<>();

        for (DetachedRelationshipDescription description : cachedDegrees.keySet()) {
            String type = description.getType().name();

            if (!degreeByType.containsKey(type)) {
                degreeByType.put(type, 0);
            }
            if (!valuesByTypeAndKey.containsKey(type)) {
                valuesByTypeAndKey.put(type, new HashMap<String, Set<Predicate>>());
            }
            if (!wildcardsByTypeAndKey.containsKey(type)) {
                wildcardsByTypeAndKey.put(type, new HashMap<String, Integer>());
            }

            degreeByType.put(type, degreeByType.get(type) + cachedDegrees.get(description));

            Set<String> keysSoFar = new HashSet<>();
            keysSoFar.addAll(valuesByTypeAndKey.get(type).keySet());
            keysSoFar.addAll(wildcardsByTypeAndKey.get(type).keySet());

            for (String key : description.getPropertiesDescription().getKeys()) {
                if (!valuesByTypeAndKey.get(type).containsKey(key)) {
                    valuesByTypeAndKey.get(type).put(key, new HashSet<Predicate>());
                    if (degreeByType.get(type) > cachedDegrees.get(description)) {
                        valuesByTypeAndKey.get(type).get(key).add(undefined());
                    }
                }

                if (!wildcardsByTypeAndKey.get(type).containsKey(key)) {
                    wildcardsByTypeAndKey.get(type).put(key, 0);
                }

                keysSoFar.remove(key);
                Predicate value = description.getPropertiesDescription().get(key);
                if (any().equals(value)) {
                    wildcardsByTypeAndKey.get(type).put(key, wildcardsByTypeAndKey.get(type).get(key) + cachedDegrees.get(description));
                } else {
                    valuesByTypeAndKey.get(type).get(key).add(value);
                }
            }

            for (String newKey : keysSoFar) {
                valuesByTypeAndKey.get(type).get(newKey).add(undefined());
            }
        }

        Set<PropertyChangeFrequency> propertyChangeFrequencies = new TreeSet<>();
        for (String type : degreeByType.keySet()) {
            Map<String, Set<Predicate>> valuesByKey = valuesByTypeAndKey.get(type);
            Map<String, Integer> wildcardsByKey = wildcardsByTypeAndKey.get(type);

            for (String key : valuesByKey.keySet()) {
                propertyChangeFrequencies.add(new PropertyChangeFrequency(type, key,
                        ((double) valuesByKey.get(key).size() + wildcardsByKey.get(key)) / ((double) degreeByType.get(type) + 1)));
            }
        }

        return new LazyGeneralizer(cachedDegrees.keySet(), new ArrayList<>(propertyChangeFrequencies)).generate();
    }
}
