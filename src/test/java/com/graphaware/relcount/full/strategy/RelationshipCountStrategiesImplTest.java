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

package com.graphaware.relcount.full.strategy;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

/**
 * Unit test for {@link com.graphaware.relcount.full.strategy.RelationshipCountStrategiesImpl}.
 */
public class RelationshipCountStrategiesImplTest {

    @Test
    public void sameStrategiesShouldProduceSameString() {
        assertEquals(RelationshipCountStrategiesImpl.defaultStrategies().with(2).asString(),
                RelationshipCountStrategiesImpl.defaultStrategies().with(2).asString());

        assertEquals(RelationshipCountStrategiesImpl.defaultStrategies().with(2).with(new CustomPropertyExtractionStrategy(2)).asString(),
                RelationshipCountStrategiesImpl.defaultStrategies().with(new CustomPropertyExtractionStrategy(2)).with(2).asString());
    }

    @Test
    public void differentStrategiesShouldHaveADifferentHashCode() {
        assertNotSame(RelationshipCountStrategiesImpl.defaultStrategies().with(2).asString(),
                RelationshipCountStrategiesImpl.defaultStrategies().with(3).asString());

        assertNotSame(RelationshipCountStrategiesImpl.defaultStrategies().with(2).with(new CustomPropertyExtractionStrategy(3)).asString(),
                RelationshipCountStrategiesImpl.defaultStrategies().with(new CustomPropertyExtractionStrategy(2)).with(2).asString());
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
        public String asString() {
            return "custom" + someConfig;
        }
    }
}
