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

package com.graphaware.neo4j.relcount.full.dto.property;

import org.junit.Test;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import static junit.framework.Assert.*;

/**
 * Unit test for {@link LiteralPropertiesDescription}.
 */
public class LiteralPropertiesDescriptionPropertiesTest {

    @Test
    public void shouldCorrectlyJudgeMoreGeneral() {
        assertFalse(lit("key1#value1#key2#value2").isMoreGeneralThan(gen("key1#value1#key2#value2")));
        assertTrue(lit("key1#value1#key2#value2").isMoreGeneralThan(lit("key1#value1#key2#value2")));
        assertFalse(lit("key1#value1#key2#value2").isStrictlyMoreGeneralThan(gen("key1#value1#key2#value2")));
        assertFalse(lit("key1#value1#key2#value2").isStrictlyMoreGeneralThan(lit("key1#value1#key2#value2")));

        assertFalse(lit("key1#value1").isMoreGeneralThan(gen("key1#value1#key2#value2")));
        assertFalse(lit("key1#value1").isMoreGeneralThan(lit("key1#value1#key2#value2")));
        assertFalse(lit("key1#value1").isStrictlyMoreGeneralThan(gen("key1#value1#key2#value2")));
        assertFalse(lit("key1#value1").isStrictlyMoreGeneralThan(lit("key1#value1#key2#value2")));

        assertFalse(lit("").isMoreGeneralThan(gen("key1#value1#key2#value2")));
        assertFalse(lit("").isMoreGeneralThan(lit("key1#value1#key2#value2")));
        assertFalse(lit("").isStrictlyMoreGeneralThan(gen("key1#value1#key2#value2")));
        assertFalse(lit("").isStrictlyMoreGeneralThan(lit("key1#value1#key2#value2")));

        assertFalse(lit("").isMoreGeneralThan(gen("")));
        assertTrue(lit("").isMoreGeneralThan(lit("")));
        assertFalse(lit("").isStrictlyMoreGeneralThan(gen("")));
        assertFalse(lit("").isStrictlyMoreGeneralThan(lit("")));

        assertFalse(lit("key1#value1#key2#value3").isMoreGeneralThan(gen("key1#value1#key2#value2")));
        assertFalse(lit("key1#value1#key2#value3").isMoreGeneralThan(lit("key1#value1#key2#value2")));
        assertFalse(lit("key1#value1#key2#value3").isStrictlyMoreGeneralThan(gen("key1#value1#key2#value2")));
        assertFalse(lit("key1#value1#key2#value3").isStrictlyMoreGeneralThan(lit("key1#value1#key2#value2")));

        assertFalse(lit("key1#value1#key2#value2").isMoreGeneralThan(gen("key1#value1#key2#value3")));
        assertFalse(lit("key1#value1#key2#value2").isMoreGeneralThan(lit("key1#value1#key2#value3")));
        assertFalse(lit("key1#value1#key2#value2").isStrictlyMoreGeneralThan(gen("key1#value1#key2#value3")));
        assertFalse(lit("key1#value1#key2#value2").isStrictlyMoreGeneralThan(lit("key1#value1#key2#value3")));

        assertFalse(lit("key1#value1#key2#value2").isMoreGeneralThan(gen("key1#value1")));
        assertFalse(lit("key1#value1#key2#value2").isMoreGeneralThan(lit("key1#value1")));
        assertFalse(lit("key1#value1#key2#value2").isStrictlyMoreGeneralThan(gen("key1#value1")));
        assertFalse(lit("key1#value1#key2#value2").isStrictlyMoreGeneralThan(lit("key1#value1")));

        assertFalse(lit("key1#value1").isMoreGeneralThan(gen("")));
        assertFalse(lit("key1#value1").isMoreGeneralThan(lit("")));
        assertFalse(lit("key1#value1").isStrictlyMoreGeneralThan(gen("")));
        assertFalse(lit("key1#value1").isStrictlyMoreGeneralThan(lit("")));

        assertFalse(lit("key1#value1#key3#value2").isMoreGeneralThan(gen("key1#value1#key2#value2")));
        assertFalse(lit("key1#value1#key3#value2").isMoreGeneralThan(lit("key1#value1#key2#value2")));
        assertFalse(lit("key1#value1#key3#value2").isStrictlyMoreGeneralThan(gen("key1#value1#key2#value2")));
        assertFalse(lit("key1#value1#key3#value2").isStrictlyMoreGeneralThan(lit("key1#value1#key2#value2")));
    }

    @Test
    public void shouldCorrectlyJudgeMoreSpecific() {
        assertTrue(lit("key1#value1#key2#value2").isMoreSpecificThan(gen("key1#value1#key2#value2")));
        assertTrue(lit("key1#value1#key2#value2").isMoreSpecificThan(lit("key1#value1#key2#value2")));
        assertFalse(lit("key1#value1#key2#value2").isStrictlyMoreSpecificThan(gen("key1#value1#key2#value2")));
        assertFalse(lit("key1#value1#key2#value2").isStrictlyMoreSpecificThan(lit("key1#value1#key2#value2")));

        assertTrue(lit("key1#value1#key2#value2").isMoreSpecificThan(gen("key1#value1")));
        assertFalse(lit("key1#value1#key2#value2").isMoreSpecificThan(lit("key1#value1")));
        assertTrue(lit("key1#value1#key2#value2").isStrictlyMoreSpecificThan(gen("key1#value1")));
        assertFalse(lit("key1#value1#key2#value2").isStrictlyMoreSpecificThan(lit("key1#value1")));

        assertTrue(lit("key1#value1#key2#value2").isMoreSpecificThan(gen("")));
        assertFalse(lit("key1#value1#key2#value2").isMoreSpecificThan(lit("")));
        assertTrue(lit("key1#value1#key2#value2").isStrictlyMoreSpecificThan(gen("")));
        assertFalse(lit("key1#value1#key2#value2").isStrictlyMoreSpecificThan(lit("")));

        assertTrue(lit("").isMoreSpecificThan(gen("")));
        assertTrue(lit("").isMoreSpecificThan(lit("")));
        assertFalse(lit("").isStrictlyMoreSpecificThan(gen("")));
        assertFalse(lit("").isStrictlyMoreSpecificThan(lit("")));

        assertFalse(lit("key1#value1#key2#value3").isMoreSpecificThan(gen("key1#value1#key2#value2")));
        assertFalse(lit("key1#value1#key2#value3").isMoreSpecificThan(lit("key1#value1#key2#value2")));
        assertFalse(lit("key1#value1#key2#value3").isStrictlyMoreSpecificThan(gen("key1#value1#key2#value2")));
        assertFalse(lit("key1#value1#key2#value3").isStrictlyMoreSpecificThan(lit("key1#value1#key2#value2")));

        assertFalse(lit("key1#value1#key2#value2").isMoreSpecificThan(gen("key1#value1#key2#value3")));
        assertFalse(lit("key1#value1#key2#value2").isMoreSpecificThan(lit("key1#value1#key2#value3")));
        assertFalse(lit("key1#value1#key2#value2").isStrictlyMoreSpecificThan(gen("key1#value1#key2#value3")));
        assertFalse(lit("key1#value1#key2#value2").isStrictlyMoreSpecificThan(lit("key1#value1#key2#value3")));

        assertFalse(lit("key1#value1").isMoreSpecificThan(gen("key1#value1#key2#value2")));
        assertFalse(lit("key1#value1").isMoreSpecificThan(lit("key1#value1#key2#value2")));
        assertFalse(lit("key1#value1").isStrictlyMoreSpecificThan(gen("key1#value1#key2#value2")));
        assertFalse(lit("key1#value1").isStrictlyMoreSpecificThan(lit("key1#value1#key2#value2")));

        assertFalse(lit("").isMoreSpecificThan(gen("key1#value1")));
        assertFalse(lit("").isMoreSpecificThan(lit("key1#value1")));
        assertFalse(lit("").isStrictlyMoreSpecificThan(gen("key1#value1")));
        assertFalse(lit("").isStrictlyMoreSpecificThan(lit("key1#value1")));

        assertFalse(lit("key1#value1#key3#value2").isMoreSpecificThan(gen("key1#value1#key2#value2")));
        assertFalse(lit("key1#value1#key3#value2").isMoreSpecificThan(lit("key1#value1#key2#value2")));
        assertFalse(lit("key1#value1#key3#value2").isStrictlyMoreSpecificThan(gen("key1#value1#key2#value2")));
        assertFalse(lit("key1#value1#key3#value2").isStrictlyMoreSpecificThan(lit("key1#value1#key2#value2")));
    }

    @Test
    public void shouldGenerateMoreGeneralAllMoreGeneral() {
        Set<PropertiesDescription> result = lit("key1#value1#key2#value2#key3#value3").generateAllMoreGeneral();

        assertEquals(9, result.size());
        Iterator<PropertiesDescription> iterator = result.iterator();
        assertEquals(iterator.next(), lit("key1#value1#key2#value2#key3#value3"));
        assertEquals(iterator.next(), gen("key1#value1#key2#value2#key3#value3"));
        assertEquals(iterator.next(), gen("key1#value1#key2#value2"));
        assertEquals(iterator.next(), gen("key1#value1#key3#value3"));
        assertEquals(iterator.next(), gen("key1#value1"));
        assertEquals(iterator.next(), gen("key2#value2#key3#value3"));
        assertEquals(iterator.next(), gen("key2#value2"));
        assertEquals(iterator.next(), gen("key3#value3"));
        assertEquals(iterator.next(), gen(""));
    }

    @Test
    public void shouldGenerateMoreGeneralOneMoreGeneral() {
        Set<PropertiesDescription> result = lit("key1#value1#key2#value2#key3#value3").generateOneMoreGeneral();

        assertEquals(1, result.size());
        Iterator<PropertiesDescription> iterator = result.iterator();
        assertEquals(iterator.next(), gen("key1#value1#key2#value2#key3#value3"));
    }

    @Test
    public void shouldGenerateMoreGeneralOneMoreGeneral2() {
        Set<PropertiesDescription> result = lit("key1#value1").generateOneMoreGeneral();

        assertEquals(1, result.size());
        Iterator<PropertiesDescription> iterator = result.iterator();
        assertEquals(iterator.next(), gen("key1#value1"));
    }

    @Test
    public void equalityTest() {
        assertTrue(lit("key1#value1").equals(lit("key1#value1")));
        assertFalse(gen("key1#value1").equals(lit("key1#value1")));
        assertFalse(lit("key1#value1").equals(gen("key1#value1")));
    }

    @Test
    public void shouldAchieveSpecificToGeneralOrderingForProperties() {
        Set<PropertiesDescription> properties = new TreeSet<>();

        properties.add(lit(""));
        properties.add(lit("key1#value1#key2#value2"));
        properties.add(lit("key2#value2"));
        properties.add(lit("key2#value2"));
        properties.add(lit("key2#value2"));
        properties.add(gen(""));
        properties.add(gen("key1#value1#key2#value2"));
        properties.add(gen("key2#value2"));
        properties.add(gen("key2#value2"));
        properties.add(gen("key2#value2"));

        Iterator<PropertiesDescription> iterator = properties.iterator();
        assertEquals(lit(""), iterator.next());
        assertEquals(lit("key1#value1#key2#value2"), iterator.next());
        assertEquals(lit("key2#value2"), iterator.next());
        assertEquals(gen("key1#value1#key2#value2"), iterator.next());
        assertEquals(gen("key2#value2"), iterator.next());
        assertEquals(gen(""), iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void propertiesShouldBehaveProperlyInTreeSets() {
        Set<PropertiesDescription> properties = new TreeSet<>();

        properties.add(lit(""));
        properties.add(lit("key1#value1#key2#value2"));
        properties.add(lit("key2#value2"));
        properties.add(lit("key1#value2"));
        properties.add(lit("key2#value1"));
        properties.add(gen(""));
        properties.add(gen("key1#value1#key2#value2"));
        properties.add(gen("key2#value2"));
        properties.add(gen("key1#value2"));
        properties.add(gen("key2#value1"));

        assertTrue(properties.contains(lit("")));
        assertTrue(properties.contains(lit("key1#value1#key2#value2")));
        assertTrue(properties.contains(lit("key2#value2")));
        assertTrue(properties.contains(lit("key1#value2")));
        assertTrue(properties.contains(lit("key2#value1")));
        assertFalse(properties.contains(lit("key1#value1")));
        assertTrue(properties.contains(gen("")));
        assertTrue(properties.contains(gen("key1#value1#key2#value2")));
        assertTrue(properties.contains(gen("key2#value2")));
        assertTrue(properties.contains(gen("key1#value2")));
        assertTrue(properties.contains(gen("key2#value1")));
        assertFalse(properties.contains(gen("key1#value1")));
    }

    /**
     * just for readability
     */
    private PropertiesDescription lit(String s) {
        return new LiteralPropertiesDescription(s, "#");
    }

    private PropertiesDescription gen(String s) {
        return new GeneralPropertiesDescription(s, "#");
    }
}
