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
import org.junit.Test;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import static junit.framework.Assert.*;

/**
 * Unit test for {@link CompactiblePropertiesImpl}.
 */
public class CompactiblePropertiesImplTest {

    @Test
    public void shouldCorrectlyJudgeMoreGeneral() {
        assertTrue(compactible("key1#value1#key2#value2").isMoreGeneralThan(compactible("key1#value1#key2#value2")));
        assertTrue(compactible("key1#value1#key2#value2").isMoreGeneralThan(literal("key1#value1#key2#value2")));
        assertTrue(compactible("key1#value1#key2#value2").isMoreGeneralThan(wildcard("key1#value1#key2#value2")));

        assertFalse(compactible("key1#value1#key2#value2").isStrictlyMoreGeneralThan(compactible("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1#key2#value2").isStrictlyMoreGeneralThan(literal("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1#key2#value2").isStrictlyMoreGeneralThan(wildcard("key1#value1#key2#value2")));

        assertTrue(compactible("key1#value1#key2#_ANY_").isMoreGeneralThan(compactible("key1#value1#key2#value2")));
        assertTrue(compactible("key1#value1#key2#_ANY_").isMoreGeneralThan(literal("key1#value1#key2#value2")));
        assertTrue(compactible("key1#value1#key2#_ANY_").isMoreGeneralThan(wildcard("key1#value1#key2#value2")));

        assertTrue(compactible("key1#value1#key2#_ANY_").isStrictlyMoreGeneralThan(compactible("key1#value1#key2#value2")));
        assertTrue(compactible("key1#value1#key2#_ANY_").isStrictlyMoreGeneralThan(literal("key1#value1#key2#value2")));
        assertTrue(compactible("key1#value1#key2#_ANY_").isStrictlyMoreGeneralThan(wildcard("key1#value1#key2#value2")));

        assertTrue(compactible("key1#value1#key2#_ANY_").isMoreGeneralThan(compactible("key1#value1#key2#_ANY_")));
        assertTrue(compactible("key1#value1#key2#_ANY_").isMoreGeneralThan(literal("key1#value1#key2#_ANY_")));
        assertTrue(compactible("key1#value1#key2#_ANY_").isMoreGeneralThan(wildcard("key1#value1#key2#_ANY_")));

        assertFalse(compactible("key1#value1#key2#_ANY_").isStrictlyMoreGeneralThan(compactible("key1#value1#key2#_ANY_")));
        assertFalse(compactible("key1#value1#key2#_ANY_").isStrictlyMoreGeneralThan(literal("key1#value1#key2#_ANY_")));
        assertFalse(compactible("key1#value1#key2#_ANY_").isStrictlyMoreGeneralThan(wildcard("key1#value1#key2#_ANY_")));

        assertFalse(compactible("key1#value1#key2#_ANY_").isMoreGeneralThan(compactible("key1#_ANY_#key2#value2")));
        assertFalse(compactible("key1#value1#key2#_ANY_").isMoreGeneralThan(literal("key1#_ANY_#key2#value2")));
        assertFalse(compactible("key1#value1#key2#_ANY_").isMoreGeneralThan(wildcard("key1#_ANY_#key2#value2")));

        assertFalse(compactible("key1#value1#key2#_ANY_").isStrictlyMoreGeneralThan(compactible("key1#_ANY_#key2#value2")));
        assertFalse(compactible("key1#value1#key2#_ANY_").isStrictlyMoreGeneralThan(literal("key1#_ANY_#key2#value2")));
        assertFalse(compactible("key1#value1#key2#_ANY_").isStrictlyMoreGeneralThan(wildcard("key1#_ANY_#key2#value2")));

        assertTrue(compactible("key1#value1#key2#_ANY_").isMoreGeneralThan(compactible("key1#value1")));
        assertTrue(compactible("key1#value1#key2#_ANY_").isMoreGeneralThan(literal("key1#value1")));
        assertTrue(compactible("key1#value1#key2#_ANY_").isMoreGeneralThan(wildcard("key1#value1")));

        assertTrue(compactible("key1#value1#key2#_ANY_").isStrictlyMoreGeneralThan(compactible("key1#value1")));
        assertTrue(compactible("key1#value1#key2#_ANY_").isStrictlyMoreGeneralThan(literal("key1#value1")));
        assertFalse(compactible("key1#value1#key2#_ANY_").isStrictlyMoreGeneralThan(wildcard("key1#value1")));

        assertTrue(compactible("key1#_ANY_#key2#_ANY_").isMoreGeneralThan(compactible("key1#value1#key2#value2")));
        assertTrue(compactible("key1#_ANY_#key2#_ANY_").isMoreGeneralThan(literal("key1#value1#key2#value2")));
        assertTrue(compactible("key1#_ANY_#key2#_ANY_").isMoreGeneralThan(wildcard("key1#value1#key2#value2")));

        assertTrue(compactible("key1#_ANY_#key2#_ANY_").isStrictlyMoreGeneralThan(compactible("key1#value1#key2#value2")));
        assertTrue(compactible("key1#_ANY_#key2#_ANY_").isStrictlyMoreGeneralThan(literal("key1#value1#key2#value2")));
        assertTrue(compactible("key1#_ANY_#key2#_ANY_").isStrictlyMoreGeneralThan(wildcard("key1#value1#key2#value2")));

        assertTrue(compactible("key1#_ANY_#key2#_ANY_").isMoreGeneralThan(compactible("key2#value2")));
        assertTrue(compactible("key1#_ANY_#key2#_ANY_").isMoreGeneralThan(literal("key2#value2")));
        assertTrue(compactible("key1#_ANY_#key2#_ANY_").isMoreGeneralThan(wildcard("key2#value2")));

        assertTrue(compactible("key1#_ANY_#key2#_ANY_").isStrictlyMoreGeneralThan(compactible("key2#value2")));
        assertTrue(compactible("key1#_ANY_#key2#_ANY_").isStrictlyMoreGeneralThan(literal("key2#value2")));
        assertTrue(compactible("key1#_ANY_#key2#_ANY_").isStrictlyMoreGeneralThan(wildcard("key2#value2")));

        assertFalse(compactible("key1#value1").isMoreGeneralThan(compactible("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1").isMoreGeneralThan(literal("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1").isMoreGeneralThan(wildcard("key1#value1#key2#value2")));

        assertFalse(compactible("key1#value1").isStrictlyMoreGeneralThan(compactible("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1").isStrictlyMoreGeneralThan(literal("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1").isStrictlyMoreGeneralThan(wildcard("key1#value1#key2#value2")));

        assertFalse(compactible("key1#value1#key2#value2").isMoreGeneralThan(compactible("key1#value1")));
        assertFalse(compactible("key1#value1#key2#value2").isMoreGeneralThan(literal("key1#value1")));
        assertFalse(compactible("key1#value1#key2#value2").isMoreGeneralThan(wildcard("key1#value1")));

        assertFalse(compactible("key1#value1#key2#value2").isStrictlyMoreGeneralThan(compactible("key1#value1")));
        assertFalse(compactible("key1#value1#key2#value2").isStrictlyMoreGeneralThan(literal("key1#value1")));
        assertFalse(compactible("key1#value1#key2#value2").isStrictlyMoreGeneralThan(wildcard("key1#value1")));

        assertFalse(compactible("key1#value1#key2#value2").isMoreGeneralThan(compactible("key1#value1#key2#_ANY_")));
        assertFalse(compactible("key1#value1#key2#value2").isMoreGeneralThan(literal("key1#value1#key2#_ANY_")));
        assertFalse(compactible("key1#value1#key2#value2").isMoreGeneralThan(wildcard("key1#value1#key2#_ANY_")));

        assertFalse(compactible("key1#value1#key2#value2").isStrictlyMoreGeneralThan(compactible("key1#value1#key2#_ANY_")));
        assertFalse(compactible("key1#value1#key2#value2").isStrictlyMoreGeneralThan(literal("key1#value1#key2#_ANY_")));
        assertFalse(compactible("key1#value1#key2#value2").isStrictlyMoreGeneralThan(wildcard("key1#value1#key2#_ANY_")));

        assertFalse(compactible("key1#value1#key2#value2").isMoreGeneralThan(compactible("")));
        assertFalse(compactible("key1#value1#key2#value2").isMoreGeneralThan(literal("")));
        assertFalse(compactible("key1#value1#key2#value2").isMoreGeneralThan(wildcard("")));

        assertFalse(compactible("key1#value1#key2#value2").isStrictlyMoreGeneralThan(compactible("")));
        assertFalse(compactible("key1#value1#key2#value2").isStrictlyMoreGeneralThan(literal("")));
        assertFalse(compactible("key1#value1#key2#value2").isStrictlyMoreGeneralThan(wildcard("")));

        assertFalse(compactible("").isMoreGeneralThan(compactible("key1#value1#key2#value2")));
        assertFalse(compactible("").isMoreGeneralThan(literal("key1#value1#key2#value2")));
        assertFalse(compactible("").isMoreGeneralThan(wildcard("key1#value1#key2#value2")));

        assertFalse(compactible("").isStrictlyMoreGeneralThan(compactible("key1#value1#key2#value2")));
        assertFalse(compactible("").isStrictlyMoreGeneralThan(literal("key1#value1#key2#value2")));
        assertFalse(compactible("").isStrictlyMoreGeneralThan(wildcard("key1#value1#key2#value2")));

        assertTrue(compactible("").isMoreGeneralThan(compactible("")));
        assertTrue(compactible("").isMoreGeneralThan(literal("")));
        assertTrue(compactible("").isMoreGeneralThan(wildcard("")));

        assertFalse(compactible("").isStrictlyMoreGeneralThan(compactible("")));
        assertFalse(compactible("").isStrictlyMoreGeneralThan(literal("")));
        assertFalse(compactible("").isStrictlyMoreGeneralThan(wildcard("")));

        assertFalse(compactible("key1#value1#key2#value3").isMoreGeneralThan(compactible("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1#key2#value3").isMoreGeneralThan(literal("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1#key2#value3").isMoreGeneralThan(wildcard("key1#value1#key2#value2")));

        assertFalse(compactible("key1#value1#key2#value3").isStrictlyMoreGeneralThan(compactible("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1#key2#value3").isStrictlyMoreGeneralThan(literal("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1#key2#value3").isStrictlyMoreGeneralThan(wildcard("key1#value1#key2#value2")));

        assertFalse(compactible("key1#value1#key3#value3").isMoreGeneralThan(compactible("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1#key3#value3").isMoreGeneralThan(literal("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1#key3#value3").isMoreGeneralThan(wildcard("key1#value1#key2#value2")));

        assertFalse(compactible("key1#value1#key3#value3").isStrictlyMoreGeneralThan(compactible("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1#key3#value3").isStrictlyMoreGeneralThan(literal("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1#key3#value3").isStrictlyMoreGeneralThan(wildcard("key1#value1#key2#value2")));
    }

    @Test
    public void shouldCorrectlyJudgeMoreSpecific() {
        assertTrue(compactible("key1#value1#key2#value2").isMoreSpecificThan(compactible("key1#value1#key2#value2")));
        assertTrue(compactible("key1#value1#key2#value2").isMoreSpecificThan(literal("key1#value1#key2#value2")));
        assertTrue(compactible("key1#value1#key2#value2").isMoreSpecificThan(wildcard("key1#value1#key2#value2")));

        assertFalse(compactible("key1#value1#key2#value2").isStrictlyMoreSpecificThan(compactible("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1#key2#value2").isStrictlyMoreSpecificThan(literal("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1#key2#value2").isStrictlyMoreSpecificThan(wildcard("key1#value1#key2#value2")));

        assertFalse(compactible("key1#value1#key2#_ANY_").isMoreSpecificThan(compactible("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1#key2#_ANY_").isMoreSpecificThan(literal("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1#key2#_ANY_").isMoreSpecificThan(wildcard("key1#value1#key2#value2")));

        assertFalse(compactible("key1#value1#key2#_ANY_").isStrictlyMoreSpecificThan(compactible("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1#key2#_ANY_").isStrictlyMoreSpecificThan(literal("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1#key2#_ANY_").isStrictlyMoreSpecificThan(wildcard("key1#value1#key2#value2")));

        assertTrue(compactible("key1#value1#key2#_ANY_").isMoreSpecificThan(compactible("key1#value1#key2#_ANY_")));
        assertTrue(compactible("key1#value1#key2#_ANY_").isMoreSpecificThan(literal("key1#value1#key2#_ANY_")));
        assertTrue(compactible("key1#value1#key2#_ANY_").isMoreSpecificThan(wildcard("key1#value1#key2#_ANY_")));

        assertFalse(compactible("key1#value1#key2#_ANY_").isStrictlyMoreSpecificThan(compactible("key1#value1#key2#_ANY_")));
        assertFalse(compactible("key1#value1#key2#_ANY_").isStrictlyMoreSpecificThan(literal("key1#value1#key2#_ANY_")));
        assertFalse(compactible("key1#value1#key2#_ANY_").isStrictlyMoreSpecificThan(wildcard("key1#value1#key2#_ANY_")));

        assertFalse(compactible("key1#value1#key2#_ANY_").isMoreSpecificThan(compactible("key1#_ANY_#key2#value2")));
        assertFalse(compactible("key1#value1#key2#_ANY_").isMoreSpecificThan(literal("key1#_ANY_#key2#value2")));
        assertFalse(compactible("key1#value1#key2#_ANY_").isMoreSpecificThan(wildcard("key1#_ANY_#key2#value2")));

        assertFalse(compactible("key1#value1#key2#_ANY_").isStrictlyMoreSpecificThan(compactible("key1#_ANY_#key2#value2")));
        assertFalse(compactible("key1#value1#key2#_ANY_").isStrictlyMoreSpecificThan(literal("key1#_ANY_#key2#value2")));
        assertFalse(compactible("key1#value1#key2#_ANY_").isStrictlyMoreSpecificThan(wildcard("key1#_ANY_#key2#value2")));

        assertFalse(compactible("key1#value1#key2#_ANY_").isMoreSpecificThan(compactible("key1#value1")));
        assertFalse(compactible("key1#value1#key2#_ANY_").isMoreSpecificThan(literal("key1#value1")));
        assertTrue(compactible("key1#value1#key2#_ANY_").isMoreSpecificThan(wildcard("key1#value1")));

        assertFalse(compactible("key1#value1#key2#_ANY_").isStrictlyMoreSpecificThan(compactible("key1#value1")));
        assertFalse(compactible("key1#value1#key2#_ANY_").isStrictlyMoreSpecificThan(literal("key1#value1")));
        assertFalse(compactible("key1#value1#key2#_ANY_").isStrictlyMoreSpecificThan(wildcard("key1#value1")));

        assertFalse(compactible("key1#_ANY_#key2#_ANY_").isMoreSpecificThan(compactible("key1#value1#key2#value2")));
        assertFalse(compactible("key1#_ANY_#key2#_ANY_").isMoreSpecificThan(literal("key1#value1#key2#value2")));
        assertFalse(compactible("key1#_ANY_#key2#_ANY_").isMoreSpecificThan(wildcard("key1#value1#key2#value2")));

        assertFalse(compactible("key1#_ANY_#key2#_ANY_").isStrictlyMoreSpecificThan(compactible("key1#value1#key2#value2")));
        assertFalse(compactible("key1#_ANY_#key2#_ANY_").isStrictlyMoreSpecificThan(literal("key1#value1#key2#value2")));
        assertFalse(compactible("key1#_ANY_#key2#_ANY_").isStrictlyMoreSpecificThan(wildcard("key1#value1#key2#value2")));

        assertFalse(compactible("key1#_ANY_#key2#_ANY_").isMoreSpecificThan(compactible("key2#value2")));
        assertFalse(compactible("key1#_ANY_#key2#_ANY_").isMoreSpecificThan(literal("key2#value2")));
        assertFalse(compactible("key1#_ANY_#key2#_ANY_").isMoreSpecificThan(wildcard("key2#value2")));

        assertFalse(compactible("key1#_ANY_#key2#_ANY_").isStrictlyMoreSpecificThan(compactible("key2#value2")));
        assertFalse(compactible("key1#_ANY_#key2#_ANY_").isStrictlyMoreSpecificThan(literal("key2#value2")));
        assertFalse(compactible("key1#_ANY_#key2#_ANY_").isStrictlyMoreSpecificThan(wildcard("key2#value2")));

        assertFalse(compactible("key1#value1").isMoreSpecificThan(compactible("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1").isMoreSpecificThan(literal("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1").isMoreSpecificThan(wildcard("key1#value1#key2#value2")));

        assertFalse(compactible("key1#value1").isStrictlyMoreSpecificThan(compactible("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1").isStrictlyMoreSpecificThan(literal("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1").isStrictlyMoreSpecificThan(wildcard("key1#value1#key2#value2")));

        assertFalse(compactible("key1#value1#key2#value2").isMoreSpecificThan(compactible("key1#value1")));
        assertFalse(compactible("key1#value1#key2#value2").isMoreSpecificThan(literal("key1#value1")));
        assertTrue(compactible("key1#value1#key2#value2").isMoreSpecificThan(wildcard("key1#value1")));

        assertFalse(compactible("key1#value1#key2#value2").isStrictlyMoreSpecificThan(compactible("key1#value1")));
        assertFalse(compactible("key1#value1#key2#value2").isStrictlyMoreSpecificThan(literal("key1#value1")));
        assertTrue(compactible("key1#value1#key2#value2").isStrictlyMoreSpecificThan(wildcard("key1#value1")));

        assertTrue(compactible("key1#value1#key2#value2").isMoreSpecificThan(compactible("key1#value1#key2#_ANY_")));
        assertTrue(compactible("key1#value1#key2#value2").isMoreSpecificThan(literal("key1#value1#key2#_ANY_")));
        assertTrue(compactible("key1#value1#key2#value2").isMoreSpecificThan(wildcard("key1#value1#key2#_ANY_")));

        assertTrue(compactible("key1#value1#key2#value2").isStrictlyMoreSpecificThan(compactible("key1#value1#key2#_ANY_")));
        assertTrue(compactible("key1#value1#key2#value2").isStrictlyMoreSpecificThan(literal("key1#value1#key2#_ANY_")));
        assertTrue(compactible("key1#value1#key2#value2").isStrictlyMoreSpecificThan(wildcard("key1#value1#key2#_ANY_")));

        assertFalse(compactible("key1#value1#key2#value2").isMoreSpecificThan(compactible("")));
        assertFalse(compactible("key1#value1#key2#value2").isMoreSpecificThan(literal("")));
        assertTrue(compactible("key1#value1#key2#value2").isMoreSpecificThan(wildcard("")));

        assertFalse(compactible("key1#value1#key2#value2").isStrictlyMoreSpecificThan(compactible("")));
        assertFalse(compactible("key1#value1#key2#value2").isStrictlyMoreSpecificThan(literal("")));
        assertTrue(compactible("key1#value1#key2#value2").isStrictlyMoreSpecificThan(wildcard("")));

        assertFalse(compactible("").isMoreSpecificThan(compactible("key1#value1#key2#value2")));
        assertFalse(compactible("").isMoreSpecificThan(literal("key1#value1#key2#value2")));
        assertFalse(compactible("").isMoreSpecificThan(wildcard("key1#value1#key2#value2")));

        assertFalse(compactible("").isStrictlyMoreSpecificThan(compactible("key1#value1#key2#value2")));
        assertFalse(compactible("").isStrictlyMoreSpecificThan(literal("key1#value1#key2#value2")));
        assertFalse(compactible("").isStrictlyMoreSpecificThan(wildcard("key1#value1#key2#value2")));

        assertTrue(compactible("").isMoreSpecificThan(compactible("")));
        assertTrue(compactible("").isMoreSpecificThan(literal("")));
        assertTrue(compactible("").isMoreSpecificThan(wildcard("")));

        assertFalse(compactible("").isStrictlyMoreSpecificThan(compactible("")));
        assertFalse(compactible("").isStrictlyMoreSpecificThan(literal("")));
        assertFalse(compactible("").isStrictlyMoreSpecificThan(wildcard("")));

        assertFalse(compactible("key1#value1#key2#value3").isMoreSpecificThan(compactible("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1#key2#value3").isMoreSpecificThan(literal("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1#key2#value3").isMoreSpecificThan(wildcard("key1#value1#key2#value2")));

        assertFalse(compactible("key1#value1#key2#value3").isStrictlyMoreSpecificThan(compactible("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1#key2#value3").isStrictlyMoreSpecificThan(literal("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1#key2#value3").isStrictlyMoreSpecificThan(wildcard("key1#value1#key2#value2")));
    }

    @Test
    public void shouldGenerateAllMoreGeneral() {
        Set<CompactibleProperties> result = compactible("key1#value1#key2#value2#key3#value3").generateAllMoreGeneral();

        assertEquals(8, result.size());
        Iterator<CompactibleProperties> iterator = result.iterator();
        assertEquals(iterator.next(), compactible("key1#value1#key2#value2#key3#value3"));
        assertEquals(iterator.next(), compactible("key1#_ANY_#key2#value2#key3#value3"));
        assertEquals(iterator.next(), compactible("key1#value1#key2#_ANY_#key3#value3"));
        assertEquals(iterator.next(), compactible("key1#_ANY_#key2#_ANY_#key3#value3"));
        assertEquals(iterator.next(), compactible("key1#value1#key2#value2#key3#_ANY_"));
        assertEquals(iterator.next(), compactible("key1#_ANY_#key2#value2#key3#_ANY_"));
        assertEquals(iterator.next(), compactible("key1#value1#key2#_ANY_#key3#_ANY_"));
        assertEquals(iterator.next(), compactible("key1#_ANY_#key2#_ANY_#key3#_ANY_"));
    }

    @Test
    public void shouldGenerateOneMoreGeneral() {
        Set<CompactibleProperties> result = compactible("key1#value1#key2#value2#key3#value3").generateOneMoreGeneral();

        assertEquals(4, result.size());
        Iterator<CompactibleProperties> iterator = result.iterator();
        assertEquals(iterator.next(), compactible("key1#value1#key2#value2#key3#value3"));
        assertEquals(iterator.next(), compactible("key1#_ANY_#key2#value2#key3#value3"));
        assertEquals(iterator.next(), compactible("key1#value1#key2#_ANY_#key3#value3"));
        assertEquals(iterator.next(), compactible("key1#value1#key2#value2#key3#_ANY_"));
    }

    @Test
    public void shouldGenerateOneMoreGeneral2() {
        Set<CompactibleProperties> result = compactible("key1#value1").generateOneMoreGeneral();

        assertEquals(2, result.size());
        Iterator<CompactibleProperties> iterator = result.iterator();
        assertEquals(iterator.next(), compactible("key1#value1"));
        assertEquals(iterator.next(), compactible("key1#_ANY_"));
    }

    @Test
    public void equalityTest() {
        assertTrue(compactible("key1#value1").equals(compactible("key1#value1")));
        assertTrue(compactible("key1#_ANY_").equals(compactible("key1#_ANY_")));
        assertFalse(compactible("key1#value1").equals(compactible("key1#value2")));
        assertFalse(compactible("key1#value1").equals(compactible("key1#_ANY_")));
        assertFalse(compactible("key1#value1").equals(compactible("")));
    }

    @Test
    public void shouldAchieveSpecificToGeneralOrderingForProperties() {
        Set<CompactibleProperties> properties = new TreeSet<>();

        properties.add(compactible("key1#value1#key2#value2"));
        properties.add(compactible("key1#_ANY_#key2#value2"));
        properties.add(compactible("key1#_ANY_#key2#value2"));
        properties.add(compactible("key1#_ANY_#key2#value2"));
        properties.add(compactible("key1#_ANY_#key2#_ANY_"));
        properties.add(compactible(""));

        Iterator<CompactibleProperties> iterator = properties.iterator();
        assertEquals(compactible(""), iterator.next());
        assertEquals(compactible("key1#value1#key2#value2"), iterator.next());
        assertEquals(compactible("key1#_ANY_#key2#value2"), iterator.next());
        assertEquals(compactible("key1#_ANY_#key2#_ANY_"), iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void propertiesShouldBehaveProperlyInTreeSets() {
        Set<CompactibleProperties> properties = new TreeSet<>();

        properties.add(compactible("key1#_ANY_#key2#_ANY_"));
        properties.add(compactible("key1#value1#key2#value2"));
        properties.add(compactible("key1#_ANY_#key2#value2"));
        properties.add(compactible("key2#_ANY_#key1#value2"));
        properties.add(compactible("key2#value1#key1#_ANY_"));
        properties.add(compactible(""));

        assertTrue(properties.contains(compactible("key1#_ANY_#key2#_ANY_")));
        assertTrue(properties.contains(compactible("key1#value1#key2#value2")));
        assertTrue(properties.contains(compactible("key1#_ANY_#key2#value2")));
        assertTrue(properties.contains(compactible("key1#value2#key2#_ANY_#")));
        assertTrue(properties.contains(compactible("key1#_ANY_#key2#value1")));
        assertTrue(properties.contains(compactible("")));
        assertFalse(properties.contains(compactible("key1#value1#key2#_ANY_#")));
    }

    @Test
    public void verifyMutualExclusion() {
        assertFalse(compactible("").isMutuallyExclusive(compactible("")));
        assertFalse(compactible("").isMutuallyExclusive(literal("")));
        assertFalse(compactible("").isMutuallyExclusive(wildcard("")));

        assertTrue(compactible("key1#value1").isMutuallyExclusive(compactible("")));
        assertTrue(compactible("key1#value1").isMutuallyExclusive(literal("")));
        assertFalse(compactible("key1#value1").isMutuallyExclusive(wildcard("")));

        assertFalse(compactible("key1#value1").isMutuallyExclusive(compactible("key1#value1")));
        assertFalse(compactible("key1#value1").isMutuallyExclusive(literal("key1#value1")));
        assertFalse(compactible("key1#value1").isMutuallyExclusive(wildcard("key1#value1")));

        assertTrue(compactible("key1#value1").isMutuallyExclusive(compactible("key1#value2")));
        assertTrue(compactible("key1#value1").isMutuallyExclusive(literal("key1#value2")));
        assertTrue(compactible("key1#value1").isMutuallyExclusive(wildcard("key1#value2")));

        assertTrue(compactible("").isMutuallyExclusive(compactible("key1#value1")));
        assertTrue(compactible("").isMutuallyExclusive(literal("key1#value1")));
        assertTrue(compactible("").isMutuallyExclusive(wildcard("key1#value1")));

        assertFalse(compactible("key1#value1").isMutuallyExclusive(compactible("key1#_ANY_")));
        assertFalse(compactible("key1#value1").isMutuallyExclusive(literal("key1#_ANY_")));
        assertFalse(compactible("key1#value1").isMutuallyExclusive(wildcard("key1#_ANY_")));

        assertFalse(compactible("key1#_ANY_").isMutuallyExclusive(compactible("key1#value1")));
        assertFalse(compactible("key1#_ANY_").isMutuallyExclusive(literal("key1#value1")));
        assertFalse(compactible("key1#_ANY_").isMutuallyExclusive(wildcard("key1#value1")));

        assertFalse(compactible("key1#value1#key2#value2").isMutuallyExclusive(compactible("key1#_ANY_#key2#value2")));
        assertFalse(compactible("key1#value1#key2#value2").isMutuallyExclusive(literal("key1#_ANY_#key2#value2")));
        assertFalse(compactible("key1#value1#key2#value2").isMutuallyExclusive(wildcard("key1#_ANY_#key2#value2")));

        assertTrue(compactible("key1#value1#key2#value3").isMutuallyExclusive(compactible("key1#_ANY_#key2#value2")));
        assertTrue(compactible("key1#value1#key2#value3").isMutuallyExclusive(literal("key1#_ANY_#key2#value2")));
        assertTrue(compactible("key1#value1#key2#value3").isMutuallyExclusive(wildcard("key1#_ANY_#key2#value2")));
    }

    /**
     * just for readability
     */
    private CompactibleProperties compactible(String s) {
        return new CompactiblePropertiesImpl(s, "#");
    }

    private ImmutableProperties<String> literal(String s) {
        return new LiteralPropertiesDescription(s, "#");
    }

    private ImmutableProperties<String> wildcard(String s) {
        return new WildcardPropertiesDescription(s, "#");
    }
}
