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
 * Unit test for {@link GeneralPropertiesDescription}.
 */
public class GeneralPropertiesDescriptionTest {

    @Test
    public void shouldCorrectlyJudgeMoreGeneral() {
        assertTrue(gen("key1#value1#key2#value2").isMoreGeneralThan(gen("key1#value1#key2#value2")));
        assertTrue(gen("key1#value1#key2#value2").isMoreGeneralThan(lit("key1#value1#key2#value2")));
        assertFalse(gen("key1#value1#key2#value2").isStrictlyMoreGeneralThan(gen("key1#value1#key2#value2")));
        assertTrue(gen("key1#value1#key2#value2").isStrictlyMoreGeneralThan(lit("key1#value1#key2#value2")));

        assertTrue(gen("key1#value1").isMoreGeneralThan(gen("key1#value1#key2#value2")));
        assertTrue(gen("key1#value1").isMoreGeneralThan(lit("key1#value1#key2#value2")));
        assertTrue(gen("key1#value1").isStrictlyMoreGeneralThan(gen("key1#value1#key2#value2")));
        assertTrue(gen("key1#value1").isStrictlyMoreGeneralThan(lit("key1#value1#key2#value2")));

        assertTrue(gen("").isMoreGeneralThan(gen("key1#value1#key2#value2")));
        assertTrue(gen("").isMoreGeneralThan(lit("key1#value1#key2#value2")));
        assertTrue(gen("").isStrictlyMoreGeneralThan(gen("key1#value1#key2#value2")));
        assertTrue(gen("").isStrictlyMoreGeneralThan(lit("key1#value1#key2#value2")));

        assertTrue(gen("").isMoreGeneralThan(gen("")));
        assertTrue(gen("").isMoreGeneralThan(lit("")));
        assertFalse(gen("").isStrictlyMoreGeneralThan(gen("")));
        assertTrue(gen("").isStrictlyMoreGeneralThan(lit("")));

        assertFalse(gen("key1#value1#key2#value3").isMoreGeneralThan(gen("key1#value1#key2#value2")));
        assertFalse(gen("key1#value1#key2#value3").isMoreGeneralThan(lit("key1#value1#key2#value2")));
        assertFalse(gen("key1#value1#key2#value3").isStrictlyMoreGeneralThan(gen("key1#value1#key2#value2")));
        assertFalse(gen("key1#value1#key2#value3").isStrictlyMoreGeneralThan(lit("key1#value1#key2#value2")));

        assertFalse(gen("key1#value1#key2#value2").isMoreGeneralThan(gen("key1#value1#key2#value3")));
        assertFalse(gen("key1#value1#key2#value2").isMoreGeneralThan(lit("key1#value1#key2#value3")));
        assertFalse(gen("key1#value1#key2#value2").isStrictlyMoreGeneralThan(gen("key1#value1#key2#value3")));
        assertFalse(gen("key1#value1#key2#value2").isStrictlyMoreGeneralThan(lit("key1#value1#key2#value3")));

        assertFalse(gen("key1#value1#key2#value2").isMoreGeneralThan(gen("key1#value1")));
        assertFalse(gen("key1#value1#key2#value2").isMoreGeneralThan(lit("key1#value1")));
        assertFalse(gen("key1#value1#key2#value2").isStrictlyMoreGeneralThan(gen("key1#value1")));
        assertFalse(gen("key1#value1#key2#value2").isStrictlyMoreGeneralThan(lit("key1#value1")));

        assertFalse(gen("key1#value1").isMoreGeneralThan(gen("")));
        assertFalse(gen("key1#value1").isMoreGeneralThan(lit("")));
        assertFalse(gen("key1#value1").isStrictlyMoreGeneralThan(gen("")));
        assertFalse(gen("key1#value1").isStrictlyMoreGeneralThan(lit("")));

        assertFalse(gen("key1#value1#key3#value2").isMoreGeneralThan(gen("key1#value1#key2#value2")));
        assertFalse(gen("key1#value1#key3#value2").isMoreGeneralThan(lit("key1#value1#key2#value2")));
        assertFalse(gen("key1#value1#key3#value2").isStrictlyMoreGeneralThan(gen("key1#value1#key2#value2")));
        assertFalse(gen("key1#value1#key3#value2").isStrictlyMoreGeneralThan(lit("key1#value1#key2#value2")));
    }

    @Test
    public void shouldCorrectlyJudgeMoreSpecific() {
        assertTrue(gen("key1#value1#key2#value2").isMoreSpecificThan(gen("key1#value1#key2#value2")));
        assertFalse(gen("key1#value1#key2#value2").isMoreSpecificThan(lit("key1#value1#key2#value2")));
        assertFalse(gen("key1#value1#key2#value2").isStrictlyMoreSpecificThan(gen("key1#value1#key2#value2")));
        assertFalse(gen("key1#value1#key2#value2").isStrictlyMoreSpecificThan(lit("key1#value1#key2#value2")));

        assertTrue(gen("key1#value1#key2#value2").isMoreSpecificThan(gen("key1#value1")));
        assertFalse(gen("key1#value1#key2#value2").isMoreSpecificThan(lit("key1#value1")));
        assertTrue(gen("key1#value1#key2#value2").isStrictlyMoreSpecificThan(gen("key1#value1")));
        assertFalse(gen("key1#value1#key2#value2").isStrictlyMoreSpecificThan(lit("key1#value1")));

        assertTrue(gen("key1#value1#key2#value2").isMoreSpecificThan(gen("")));
        assertFalse(gen("key1#value1#key2#value2").isMoreSpecificThan(lit("")));
        assertTrue(gen("key1#value1#key2#value2").isStrictlyMoreSpecificThan(gen("")));
        assertFalse(gen("key1#value1#key2#value2").isStrictlyMoreSpecificThan(lit("")));

        assertTrue(gen("").isMoreSpecificThan(gen("")));
        assertFalse(gen("").isMoreSpecificThan(lit("")));
        assertFalse(gen("").isStrictlyMoreSpecificThan(gen("")));
        assertFalse(gen("").isStrictlyMoreSpecificThan(lit("")));

        assertFalse(gen("key1#value1#key2#value3").isMoreSpecificThan(gen("key1#value1#key2#value2")));
        assertFalse(gen("key1#value1#key2#value3").isMoreSpecificThan(lit("key1#value1#key2#value2")));
        assertFalse(gen("key1#value1#key2#value3").isStrictlyMoreSpecificThan(gen("key1#value1#key2#value2")));
        assertFalse(gen("key1#value1#key2#value3").isStrictlyMoreSpecificThan(lit("key1#value1#key2#value2")));

        assertFalse(gen("key1#value1#key2#value2").isMoreSpecificThan(gen("key1#value1#key2#value3")));
        assertFalse(gen("key1#value1#key2#value2").isMoreSpecificThan(lit("key1#value1#key2#value3")));
        assertFalse(gen("key1#value1#key2#value2").isStrictlyMoreSpecificThan(gen("key1#value1#key2#value3")));
        assertFalse(gen("key1#value1#key2#value2").isStrictlyMoreSpecificThan(lit("key1#value1#key2#value3")));

        assertFalse(gen("key1#value1").isMoreSpecificThan(gen("key1#value1#key2#value2")));
        assertFalse(gen("key1#value1").isMoreSpecificThan(lit("key1#value1#key2#value2")));
        assertFalse(gen("key1#value1").isStrictlyMoreSpecificThan(gen("key1#value1#key2#value2")));
        assertFalse(gen("key1#value1").isStrictlyMoreSpecificThan(lit("key1#value1#key2#value2")));

        assertFalse(gen("").isMoreSpecificThan(gen("key1#value1")));
        assertFalse(gen("").isMoreSpecificThan(lit("key1#value1")));
        assertFalse(gen("").isStrictlyMoreSpecificThan(gen("key1#value1")));
        assertFalse(gen("").isStrictlyMoreSpecificThan(lit("key1#value1")));

        assertFalse(gen("key1#value1#key3#value2").isMoreSpecificThan(gen("key1#value1#key2#value2")));
        assertFalse(gen("key1#value1#key3#value2").isMoreSpecificThan(lit("key1#value1#key2#value2")));
        assertFalse(gen("key1#value1#key3#value2").isStrictlyMoreSpecificThan(gen("key1#value1#key2#value2")));
        assertFalse(gen("key1#value1#key3#value2").isStrictlyMoreSpecificThan(lit("key1#value1#key2#value2")));
    }

    @Test
    public void shouldGenerateAllMoreGeneral() {
        Set<PropertiesDescription> result = gen("key1#value1#key2#value2#key3#value3").generateAllMoreGeneral();

        assertEquals(8, result.size());
        Iterator<PropertiesDescription> iterator = result.iterator();
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
    public void shouldGenerateOneMoreGeneral() {
        Set<PropertiesDescription> result = gen("key1#value1#key2#value2#key3#value3").generateOneMoreGeneral();

        assertEquals(4, result.size());
        Iterator<PropertiesDescription> iterator = result.iterator();
        assertEquals(iterator.next(), gen("key1#value1#key2#value2#key3#value3"));
        assertEquals(iterator.next(), gen("key1#value1#key2#value2"));
        assertEquals(iterator.next(), gen("key1#value1#key3#value3"));
        assertEquals(iterator.next(), gen("key2#value2#key3#value3"));
    }

    @Test
    public void shouldGenerateOneMoreGeneral2() {
        Set<PropertiesDescription> result = gen("key1#value1").generateOneMoreGeneral();

        assertEquals(2, result.size());
        Iterator<PropertiesDescription> iterator = result.iterator();
        assertEquals(iterator.next(), gen("key1#value1"));
        assertEquals(iterator.next(), gen(""));
    }

    @Test
    public void equalityTest() {
        assertTrue(gen("key1#value1").equals(gen("key1#value1")));
        assertFalse(gen("key1#value1").equals(gen("key1#value2")));
        assertFalse(gen("key1#value1").equals(gen("")));
    }

    @Test
    public void shouldAchieveSpecificToGeneralOrderingForProperties() {
        Set<PropertiesDescription> properties = new TreeSet<>();

        properties.add(gen(""));
        properties.add(gen("key1#value1#key2#value2"));
        properties.add(gen("key2#value2"));
        properties.add(gen("key2#value2"));
        properties.add(gen("key2#value2"));

        Iterator<PropertiesDescription> iterator = properties.iterator();
        assertEquals(gen("key1#value1#key2#value2"), iterator.next());
        assertEquals(gen("key2#value2"), iterator.next());
        assertEquals(gen(""), iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void propertiesShouldBehaveProperlyInTreeSets() {
        Set<PropertiesDescription> properties = new TreeSet<>();

        properties.add(gen(""));
        properties.add(gen("key1#value1#key2#value2"));
        properties.add(gen("key2#value2"));
        properties.add(gen("key1#value2"));
        properties.add(gen("key2#value1"));

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
    private PropertiesDescription gen(String s) {
        return new GeneralPropertiesDescription(s, "#");
    }

    private PropertiesDescription lit(String s) {
        return new LiteralPropertiesDescription(s, "#");
    }
}
