package com.graphaware.relcount.compact;

import com.graphaware.description.relationship.DetachedRelationshipDescription;

import java.util.*;

import static com.graphaware.description.predicate.Predicates.any;

/**
 * Component that lazily produces generalizations of given {@link DetachedRelationshipDescription}s based on their
 * {@link PropertyChangeFrequency}s.
 * <p/>
 * Given properties A of relationship type T1, B of T2, and C of T1 with property change frequencies decreasing in this
 * order, the following generalizations are produced in this order:
 * <p/>
 * 1) generalizations of all {@link DetachedRelationshipDescription}s with relationship type = T1 with A set to {@link com.graphaware.description.predicate.Any}.
 * 2) generalizations of all {@link DetachedRelationshipDescription}s with relationship type = T2 with B set to {@link com.graphaware.description.predicate.Any}.
 * 3) generalizations of all {@link DetachedRelationshipDescription}s with relationship type = T1 with C set to {@link com.graphaware.description.predicate.Any}.
 * 4) generalizations of all {@link DetachedRelationshipDescription}s with relationship type = T1 with A and C set to {@link com.graphaware.description.predicate.Any}.
 */
class LazyGeneralizer {

    private final Set<DetachedRelationshipDescription> descriptions;
    private final Iterator<PropertyChangeFrequency> frequencies;

    private final Map<String, List<Set<String>>> usedPropertySetsByType = new HashMap<>();

    /**
     * Construct a new generalizer.
     *
     * @param descriptions from which to create generalizations.
     * @param frequencies  of properties of the above descriptions.
     */
    LazyGeneralizer(Set<DetachedRelationshipDescription> descriptions, List<PropertyChangeFrequency> frequencies) {
        this.descriptions = descriptions;
        this.frequencies = frequencies.iterator();
    }

    /**
     * {@inheritDoc}
     */
    protected DetachedRelationshipDescription generate() {
        if (!frequencies.hasNext()) {
            return null;
        }

        DetachedRelationshipDescription result = generateResult();

        if (result == null) {
            return generate();
        }

        return result;
    }

    private DetachedRelationshipDescription generateResult() {
        PropertyChangeFrequency frequency = frequencies.next();

        if (!usedPropertySetsByType.containsKey(frequency.getType())) {
            usedPropertySetsByType.put(frequency.getType(), new LinkedList<Set<String>>());
        }

        List<Set<String>> newPropertySets = createNewPropertySets(frequency);
        usedPropertySetsByType.get(frequency.getType()).addAll(newPropertySets);

        return generateResult(frequency.getType(), newPropertySets);
    }

    private List<Set<String>> createNewPropertySets(PropertyChangeFrequency frequency) {
        List<Set<String>> newPropertySets = new LinkedList<>();

        newPropertySets.add(Collections.singleton(frequency.getProperty()));
        for (Set<String> usedPropertySet : usedPropertySetsByType.get(frequency.getType())) {
            Set<String> newPropertySet = new HashSet<>(usedPropertySet);
            newPropertySet.add(frequency.getProperty());
            newPropertySets.add(newPropertySet);
        }

        return newPropertySets;
    }

    private DetachedRelationshipDescription generateResult(String type, List<Set<String>> newPropertySets) {
        int maxMaches = 1;
        DetachedRelationshipDescription result = null;

        for (Set<String> newPropertySet : newPropertySets) {
            for (DetachedRelationshipDescription candidate : descriptions) {
                if (!candidate.getType().name().equals(type)) {
                    continue;
                }
                DetachedRelationshipDescription generalizedDescription = candidate;
                for (String property : newPropertySet) {
                    generalizedDescription = generalizedDescription.with(property, any());
                }

                int matches = 0;
                for (DetachedRelationshipDescription description : descriptions) {
                    if (description.isMoreSpecificThan(generalizedDescription)) {
                        matches++;
                    }
                }

                if (matches > maxMaches) {
                    maxMaches = matches;
                    result = generalizedDescription;
                }
            }
            if (result != null) {
                return result;
            }
        }
        return result;
    }
}
