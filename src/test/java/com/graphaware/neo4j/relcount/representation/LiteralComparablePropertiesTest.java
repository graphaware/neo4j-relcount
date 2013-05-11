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

package com.graphaware.neo4j.relcount.representation;

import org.junit.Test;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import static com.graphaware.neo4j.relcount.representation.LiteralComparableProperties.LITERAL;
import static junit.framework.Assert.*;

/**
 * Unit test for {@link ComparableProperties}.
 */
public class LiteralComparablePropertiesTest {

    @Test
    public void shouldCorrectlyJudgeMoreGeneral() {
        assertTrue(lprops("key1#value1#key2#value2").isMoreGeneralThan(cprops("key1#value1#key2#value2")));
        assertFalse(lprops("key1#value1#key2#value2").isStrictlyMoreGeneralThan(cprops("key1#value1#key2#value2")));

        assertFalse(lprops("key1#value1").isMoreGeneralThan(cprops("key1#value1#key2#value2")));
        assertFalse(lprops("key1#value1").isStrictlyMoreGeneralThan(cprops("key1#value1#key2#value2")));

        assertTrue(cprops("key1#value1#key2#value2").isMoreGeneralThan(lprops("key1#value1#key2#value2")));
        assertFalse(cprops("key1#value1#key2#value2").isStrictlyMoreGeneralThan(lprops("key1#value1#key2#value2")));

        assertTrue(cprops("key1#value1").isMoreGeneralThan(lprops("key1#value1#key2#value2")));
        assertTrue(cprops("key1#value1").isStrictlyMoreGeneralThan(lprops("key1#value1#key2#value2")));

        assertFalse(lprops("").isMoreGeneralThan(lprops("key1#value1#key2#value2")));
        assertFalse(lprops("").isStrictlyMoreGeneralThan(lprops("key1#value1#key2#value2")));

        assertTrue(lprops("").isMoreGeneralThan(cprops("")));
        assertFalse(lprops("").isStrictlyMoreGeneralThan(cprops("")));

        assertTrue(cprops("").isMoreGeneralThan(lprops("")));
        assertFalse(cprops("").isStrictlyMoreGeneralThan(lprops("")));

        assertFalse(lprops("key1#value1#key2#value3").isMoreGeneralThan(cprops("key1#value1#key2#value2")));
        assertFalse(lprops("key1#value1#key2#value3").isStrictlyMoreGeneralThan(cprops("key1#value1#key2#value2")));

        assertFalse(cprops("key1#value1#key2#value3").isMoreGeneralThan(lprops("key1#value1#key2#value2")));
        assertFalse(cprops("key1#value1#key2#value3").isStrictlyMoreGeneralThan(lprops("key1#value1#key2#value2")));

        assertFalse(lprops("key1#value1#key2#value2").isMoreGeneralThan(cprops("key1#value1#key2#value3")));
        assertFalse(lprops("key1#value1#key2#value2").isStrictlyMoreGeneralThan(cprops("key1#value1#key2#value3")));

        assertFalse(cprops("key1#value1#key2#value2").isMoreGeneralThan(lprops("key1#value1#key2#value3")));
        assertFalse(cprops("key1#value1#key2#value2").isStrictlyMoreGeneralThan(lprops("key1#value1#key2#value3")));

        assertFalse(lprops("key1#value1#key2#value2").isMoreGeneralThan(lprops("key1#value1")));
        assertFalse(lprops("key1#value1#key2#value2").isStrictlyMoreGeneralThan(lprops("key1#value1")));

        assertFalse(lprops("key1#value1#key2#value2").isMoreGeneralThan(lprops("key1#value1")));
        assertFalse(lprops("key1#value1#key2#value2").isStrictlyMoreGeneralThan(lprops("key1#value1")));

        assertFalse(lprops("key1#value1").isMoreGeneralThan(cprops("")));
        assertFalse(lprops("key1#value1").isStrictlyMoreGeneralThan(cprops("")));

        assertFalse(cprops("key1#value1").isMoreGeneralThan(lprops("")));
        assertFalse(cprops("key1#value1").isStrictlyMoreGeneralThan(lprops("")));

        assertFalse(lprops("key1#value1#key3#value2").isMoreGeneralThan(cprops("key1#value1#key2#value2")));
        assertFalse(lprops("key1#value1#key3#value2").isStrictlyMoreGeneralThan(cprops("key1#value1#key2#value2")));

        assertFalse(cprops("key1#value1#key3#value2").isMoreGeneralThan(lprops("key1#value1#key2#value2")));
        assertFalse(cprops("key1#value1#key3#value2").isStrictlyMoreGeneralThan(lprops("key1#value1#key2#value2")));
    }

    @Test
    public void shouldCorrectlyJudgeMoreSpecific() {
        assertTrue(lprops("key1#value1#key2#value2").isMoreSpecificThan(cprops("key1#value1#key2#value2")));
        assertFalse(lprops("key1#value1#key2#value2").isStrictlyMoreSpecificThan(cprops("key1#value1#key2#value2")));

        assertTrue(cprops("key1#value1#key2#value2").isMoreSpecificThan(lprops("key1#value1#key2#value2")));
        assertFalse(cprops("key1#value1#key2#value2").isStrictlyMoreSpecificThan(lprops("key1#value1#key2#value2")));

        assertTrue(lprops("key1#value1#key2#value2").isMoreSpecificThan(cprops("key1#value1")));
        assertTrue(lprops("key1#value1#key2#value2").isStrictlyMoreSpecificThan(cprops("key1#value1")));

        assertFalse(cprops("key1#value1#key2#value2").isMoreSpecificThan(lprops("key1#value1")));
        assertFalse(cprops("key1#value1#key2#value2").isStrictlyMoreSpecificThan(lprops("key1#value1")));

        assertFalse(lprops("key1#value1#key2#value2").isMoreSpecificThan(lprops("")));
        assertFalse(lprops("key1#value1#key2#value2").isStrictlyMoreSpecificThan(lprops("")));

        assertTrue(lprops("").isMoreSpecificThan(cprops("")));
        assertFalse(lprops("").isStrictlyMoreSpecificThan(cprops("")));

        assertTrue(cprops("").isMoreSpecificThan(lprops("")));
        assertFalse(cprops("").isStrictlyMoreSpecificThan(lprops("")));

        assertFalse(lprops("key1#value1#key2#value3").isMoreSpecificThan(cprops("key1#value1#key2#value2")));
        assertFalse(lprops("key1#value1#key2#value3").isStrictlyMoreSpecificThan(cprops("key1#value1#key2#value2")));

        assertFalse(cprops("key1#value1#key2#value3").isMoreSpecificThan(lprops("key1#value1#key2#value2")));
        assertFalse(cprops("key1#value1#key2#value3").isStrictlyMoreSpecificThan(lprops("key1#value1#key2#value2")));

        assertFalse(lprops("key1#value1#key2#value2").isMoreSpecificThan(lprops("key1#value1#key2#value3")));
        assertFalse(lprops("key1#value1#key2#value2").isStrictlyMoreSpecificThan(lprops("key1#value1#key2#value3")));

        assertFalse(lprops("key1#value1").isMoreSpecificThan(cprops("key1#value1#key2#value2")));
        assertFalse(lprops("key1#value1").isStrictlyMoreSpecificThan(cprops("key1#value1#key2#value2")));

        assertFalse(cprops("key1#value1").isMoreSpecificThan(lprops("key1#value1#key2#value2")));
        assertFalse(cprops("key1#value1").isStrictlyMoreSpecificThan(lprops("key1#value1#key2#value2")));

        assertFalse(lprops("").isMoreSpecificThan(cprops("key1#value1")));
        assertFalse(lprops("").isStrictlyMoreSpecificThan(cprops("key1#value1")));

        assertFalse(cprops("").isMoreSpecificThan(lprops("key1#value1")));
        assertFalse(cprops("").isStrictlyMoreSpecificThan(lprops("key1#value1")));

        assertFalse(lprops("key1#value1#key3#value2").isMoreSpecificThan(cprops("key1#value1#key2#value2")));
        assertFalse(lprops("key1#value1#key3#value2").isStrictlyMoreSpecificThan(cprops("key1#value1#key2#value2")));

        assertFalse(cprops("key1#value1#key3#value2").isMoreSpecificThan(lprops("key1#value1#key2#value2")));
        assertFalse(cprops("key1#value1#key3#value2").isStrictlyMoreSpecificThan(lprops("key1#value1#key2#value2")));
    }

    @Test
    public void shouldGenerateMoreGeneralAllMoreGeneral() {
        Set<ComparableProperties> result = lprops("key1#value1#key2#value2#key3#value3").generateAllMoreGeneral();

        assertEquals(9, result.size());
        Iterator<ComparableProperties> iterator = result.iterator();
        assertEquals(iterator.next(), lprops("key1#value1#key2#value2#key3#value3"));
        assertEquals(iterator.next(), cprops("key1#value1#key2#value2#key3#value3"));
        assertEquals(iterator.next(), cprops("key1#value1#key2#value2"));
        assertEquals(iterator.next(), cprops("key1#value1#key3#value3"));
        assertEquals(iterator.next(), cprops("key1#value1"));
        assertEquals(iterator.next(), cprops("key2#value2#key3#value3"));
        assertEquals(iterator.next(), cprops("key2#value2"));
        assertEquals(iterator.next(), cprops("key3#value3"));
        assertEquals(iterator.next(), cprops(""));
    }

    @Test
    public void shouldGenerateMoreGeneralOneMoreGeneral() {
        Set<ComparableProperties> result = lprops("key1#value1#key2#value2#key3#value3").generateOneMoreGeneral();

        assertEquals(1, result.size());
        Iterator<ComparableProperties> iterator = result.iterator();
        assertEquals(iterator.next(), cprops("key1#value1#key2#value2#key3#value3"));
    }

    @Test
    public void shouldGenerateMoreGeneralOneMoreGeneral2() {
        Set<ComparableProperties> result = lprops("key1#value1").generateOneMoreGeneral();

        assertEquals(1, result.size());
        Iterator<ComparableProperties> iterator = result.iterator();
        assertEquals(iterator.next(), cprops("key1#value1"));
    }

    @Test
    public void equalityTest() {
        assertTrue(lprops("key1#value1").equals(lprops("key1#value1")));
        assertFalse(cprops("key1#value1").equals(lprops("key1#value1")));
        assertFalse(lprops("key1#value1").equals(cprops("key1#value1")));
    }

    @Test
    public void shouldAchieveSpecificToGeneralOrderingForProperties() {
        Set<ComparableProperties> properties = new TreeSet<ComparableProperties>();

        properties.add(lprops(""));
        properties.add(lprops("key1#value1#key2#value2"));
        properties.add(lprops("key2#value2"));
        properties.add(lprops("key2#value2"));
        properties.add(lprops("key2#value2"));
        properties.add(cprops(""));
        properties.add(cprops("key1#value1#key2#value2"));
        properties.add(cprops("key2#value2"));
        properties.add(cprops("key2#value2"));
        properties.add(cprops("key2#value2"));

        Iterator<ComparableProperties> iterator = properties.iterator();
        assertEquals(lprops(""), iterator.next());
        assertEquals(lprops("key1#value1#key2#value2"), iterator.next());
        assertEquals(lprops("key2#value2"), iterator.next());
        assertEquals(cprops("key1#value1#key2#value2"), iterator.next());
        assertEquals(cprops("key2#value2"), iterator.next());
        assertEquals(cprops(""), iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void propertiesShouldBehaveProperlyInTreeSets() {
        Set<ComparableProperties> properties = new TreeSet<ComparableProperties>();

        properties.add(lprops(""));
        properties.add(lprops("key1#value1#key2#value2"));
        properties.add(lprops("key2#value2"));
        properties.add(lprops("key1#value2"));
        properties.add(lprops("key2#value1"));
        properties.add(cprops(""));
        properties.add(cprops("key1#value1#key2#value2"));
        properties.add(cprops("key2#value2"));
        properties.add(cprops("key1#value2"));
        properties.add(cprops("key2#value1"));

        assertTrue(properties.contains(lprops("")));
        assertTrue(properties.contains(lprops("key1#value1#key2#value2")));
        assertTrue(properties.contains(lprops("key2#value2")));
        assertTrue(properties.contains(lprops("key1#value2")));
        assertTrue(properties.contains(lprops("key2#value1")));
        assertFalse(properties.contains(lprops("key1#value1")));
        assertTrue(properties.contains(cprops("")));
        assertTrue(properties.contains(cprops("key1#value1#key2#value2")));
        assertTrue(properties.contains(cprops("key2#value2")));
        assertTrue(properties.contains(cprops("key1#value2")));
        assertTrue(properties.contains(cprops("key2#value1")));
        assertFalse(properties.contains(cprops("key1#value1")));
    }

    /**
     * just for readability
     */
    private ComparableProperties lprops(String s) {
        return new LiteralComparableProperties(LITERAL + s);
    }

    private ComparableProperties cprops(String s) {
        return new ComparableProperties(s);
    }
}
