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

import com.graphaware.neo4j.dto.common.property.ImmutableProperties;
import com.graphaware.neo4j.dto.string.property.SerializablePropertiesImpl;
import org.junit.Test;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import static junit.framework.Assert.*;

/**
 * Unit test for {@link CandidateGeneralizedProperties}.
 */
public class ComparablePropertiesTest {

    @Test
    public void shouldCorrectlyJudgeMoreGeneral() {
        assertTrue(props("key1#value1#key2#value2").isMoreGeneralThan(props2("key1#value1#key2#value2")));
        assertFalse(props("key1#value1#key2#value2").isStrictlyMoreGeneralThan(props2("key1#value1#key2#value2")));

        assertTrue(props("key1#value1").isMoreGeneralThan(props2("key1#value1#key2#value2")));
        assertTrue(props("key1#value1").isStrictlyMoreGeneralThan(props2("key1#value1#key2#value2")));

        assertTrue(props("").isMoreGeneralThan(props("key1#value1#key2#value2")));
        assertTrue(props("").isStrictlyMoreGeneralThan(props("key1#value1#key2#value2")));

        assertTrue(props("").isMoreGeneralThan(props2("")));
        assertFalse(props("").isStrictlyMoreGeneralThan(props2("")));

        assertFalse(props("key1#value1#key2#value3").isMoreGeneralThan(props2("key1#value1#key2#value2")));
        assertFalse(props("key1#value1#key2#value3").isStrictlyMoreGeneralThan(props2("key1#value1#key2#value2")));

        assertFalse(props("key1#value1#key2#value2").isMoreGeneralThan(props2("key1#value1#key2#value3")));
        assertFalse(props("key1#value1#key2#value2").isStrictlyMoreGeneralThan(props2("key1#value1#key2#value3")));

        assertFalse(props("key1#value1#key2#value2").isMoreGeneralThan(props("key1#value1")));
        assertFalse(props("key1#value1#key2#value2").isStrictlyMoreGeneralThan(props("key1#value1")));

        assertFalse(props("key1#value1").isMoreGeneralThan(props2("")));
        assertFalse(props("key1#value1").isStrictlyMoreGeneralThan(props2("")));

        assertFalse(props("key1#value1#key3#value2").isMoreGeneralThan(props2("key1#value1#key2#value2")));
        assertFalse(props("key1#value1#key3#value2").isStrictlyMoreGeneralThan(props2("key1#value1#key2#value2")));
    }

    @Test
    public void shouldCorrectlyJudgeMoreSpecific() {
        assertTrue(props("key1#value1#key2#value2").isMoreSpecificThan(props2("key1#value1#key2#value2")));
        assertFalse(props("key1#value1#key2#value2").isStrictlyMoreSpecificThan(props2("key1#value1#key2#value2")));

        assertTrue(props("key1#value1#key2#value2").isMoreSpecificThan(props2("key1#value1")));
        assertTrue(props("key1#value1#key2#value2").isStrictlyMoreSpecificThan(props2("key1#value1")));

        assertTrue(props("key1#value1#key2#value2").isMoreSpecificThan(props("")));
        assertTrue(props("key1#value1#key2#value2").isStrictlyMoreSpecificThan(props("")));

        assertTrue(props("").isMoreSpecificThan(props2("")));
        assertFalse(props("").isStrictlyMoreSpecificThan(props2("")));

        assertFalse(props("key1#value1#key2#value3").isMoreSpecificThan(props2("key1#value1#key2#value2")));
        assertFalse(props("key1#value1#key2#value3").isStrictlyMoreSpecificThan(props2("key1#value1#key2#value2")));

        assertFalse(props("key1#value1#key2#value2").isMoreSpecificThan(props("key1#value1#key2#value3")));
        assertFalse(props("key1#value1#key2#value2").isStrictlyMoreSpecificThan(props("key1#value1#key2#value3")));

        assertFalse(props("key1#value1").isMoreSpecificThan(props2("key1#value1#key2#value2")));
        assertFalse(props("key1#value1").isStrictlyMoreSpecificThan(props2("key1#value1#key2#value2")));

        assertFalse(props("").isMoreSpecificThan(props2("key1#value1")));
        assertFalse(props("").isStrictlyMoreSpecificThan(props2("key1#value1")));

        assertFalse(props("key1#value1#key3#value2").isMoreSpecificThan(props2("key1#value1#key2#value2")));
        assertFalse(props("key1#value1#key3#value2").isStrictlyMoreSpecificThan(props2("key1#value1#key2#value2")));
    }

    @Test
    public void shouldGenerateMoreGeneralAllMoreGeneral() {
        Set<CandidateProperties> result = props("key1#value1#key2#value2#key3#value3").generateAllMoreGeneral();

        assertEquals(8, result.size());
        Iterator<CandidateProperties> iterator = result.iterator();
        assertEquals(iterator.next(), props("key1#value1#key2#value2#key3#value3"));
        assertEquals(iterator.next(), props("key1#value1#key2#value2"));
        assertEquals(iterator.next(), props("key1#value1#key3#value3"));
        assertEquals(iterator.next(), props("key1#value1"));
        assertEquals(iterator.next(), props("key2#value2#key3#value3"));
        assertEquals(iterator.next(), props("key2#value2"));
        assertEquals(iterator.next(), props("key3#value3"));
        assertEquals(iterator.next(), props(""));
    }

    @Test
    public void shouldGenerateMoreGeneralOneMoreGeneral() {
        Set<CandidateProperties> result = props("key1#value1#key2#value2#key3#value3").generateOneMoreGeneral();

        assertEquals(4, result.size());
        Iterator<CandidateProperties> iterator = result.iterator();
        assertEquals(iterator.next(), props("key1#value1#key2#value2#key3#value3"));
        assertEquals(iterator.next(), props("key1#value1#key2#value2"));
        assertEquals(iterator.next(), props("key1#value1#key3#value3"));
        assertEquals(iterator.next(), props("key2#value2#key3#value3"));
    }

    @Test
    public void shouldGenerateMoreGeneralOneMoreGeneral2() {
        Set<CandidateProperties> result = props("key1#value1").generateOneMoreGeneral();

        assertEquals(2, result.size());
        Iterator<CandidateProperties> iterator = result.iterator();
        assertEquals(iterator.next(), props("key1#value1"));
        assertEquals(iterator.next(), props(""));
    }

    @Test
    public void equalityTest() {
        assertTrue(props("key1#value1").equals(props("key1#value1")));
    }

    @Test
    public void shouldAchieveSpecificToGeneralOrderingForProperties() {
        Set<CandidateProperties> properties = new TreeSet<>();

        properties.add(props(""));
        properties.add(props("key1#value1#key2#value2"));
        properties.add(props("key2#value2"));
        properties.add(props("key2#value2"));
        properties.add(props("key2#value2"));

        Iterator<CandidateProperties> iterator = properties.iterator();
        assertEquals(props("key1#value1#key2#value2"), iterator.next());
        assertEquals(props("key2#value2"), iterator.next());
        assertEquals(props(""), iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void propertiesShouldBehaveProperlyInTreeSets() {
        Set<CandidateProperties> properties = new TreeSet<>();

        properties.add(props(""));
        properties.add(props("key1#value1#key2#value2"));
        properties.add(props("key2#value2"));
        properties.add(props("key1#value2"));
        properties.add(props("key2#value1"));

        assertTrue(properties.contains(props("")));
        assertTrue(properties.contains(props("key1#value1#key2#value2")));
        assertTrue(properties.contains(props("key2#value2")));
        assertTrue(properties.contains(props("key1#value2")));
        assertTrue(properties.contains(props("key2#value1")));
        assertFalse(properties.contains(props("key1#value1")));
    }

    /**
     * just for readability
     */
    private CandidateProperties props(String s) {
        return new CandidateGeneralizedProperties(s);
    }

    private ImmutableProperties<String> props2(String s) {
        return new SerializablePropertiesImpl(s);
    }
}
