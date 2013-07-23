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

package com.graphaware.neo4j.relcount.full.strategy;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

/**
 * Unit test for {@link RelationshipCountStrategiesImpl}.
 */
public class RelationshipCountStrategiesImplTest {

    @Test
    public void sameStrategiesShouldHaveSameHashCode() {
        assertEquals(RelationshipCountStrategiesImpl.defaultStrategies().with(2).hashCode(),
                RelationshipCountStrategiesImpl.defaultStrategies().with(2).hashCode());

        assertEquals(RelationshipCountStrategiesImpl.defaultStrategies().with(2).with(new CustomPropertyExtractionStrategy(2)).hashCode(),
                RelationshipCountStrategiesImpl.defaultStrategies().with(new CustomPropertyExtractionStrategy(2)).with(2).hashCode());
    }

    @Test
    public void differentStrategiesShouldHaveADifferentHashCode() {
        assertNotSame(RelationshipCountStrategiesImpl.defaultStrategies().with(2).hashCode(),
                RelationshipCountStrategiesImpl.defaultStrategies().with(3).hashCode());

        assertNotSame(RelationshipCountStrategiesImpl.defaultStrategies().with(2).with(new CustomPropertyExtractionStrategy(3)).hashCode(),
                RelationshipCountStrategiesImpl.defaultStrategies().with(new CustomPropertyExtractionStrategy(2)).with(2).hashCode());
    }

    private class CustomPropertyExtractionStrategy implements RelationshipPropertiesExtractionStrategy {

        private final int someConfig;

        private CustomPropertyExtractionStrategy(int someConfig) {
            this.someConfig = someConfig;
        }

        @Override
        public Map<String, String> extractProperties(Relationship relationship, Node pointOfView) {
            return Collections.emptyMap();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CustomPropertyExtractionStrategy that = (CustomPropertyExtractionStrategy) o;

            if (someConfig != that.someConfig) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return someConfig;
        }
    }
}
