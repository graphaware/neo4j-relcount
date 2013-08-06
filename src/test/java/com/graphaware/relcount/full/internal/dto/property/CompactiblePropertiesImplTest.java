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

package com.graphaware.relcount.full.internal.dto.property;

import com.graphaware.propertycontainer.dto.common.property.ImmutableProperties;
import junit.framework.Assert;
import org.junit.Test;

import java.util.*;

import static junit.framework.Assert.*;

/**
 * Unit test for {@link com.graphaware.relcount.full.internal.dto.property.CompactiblePropertiesImpl}.
 */
public class CompactiblePropertiesImplTest {

    @Test
    public void shouldCorrectlyJudgeMoreGeneral() {
        assertTrue(compactible("key1#value1#key2#value2").isMoreGeneralThan(compactible("key1#value1#key2#value2")));
        assertTrue(compactible("key1#value1#key2#value2").isMoreGeneralThan(literal("key1#value1#key2#value2")));
        assertTrue(compactible("key1#value1#key2#value2").isMoreGeneralThan(wildcard("key1#value1#key2#value2")));

        assertTrue(compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreGeneralThan(compactible("key1#value1#key2#value2")));
        assertTrue(compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreGeneralThan(literal("key1#value1#key2#value2")));
        assertTrue(compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreGeneralThan(wildcard("key1#value1#key2#value2")));

        assertTrue(compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreGeneralThan(compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE)));
        assertTrue(compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreGeneralThan(literal("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE)));
        assertTrue(compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreGeneralThan(wildcard("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE)));

        assertFalse(compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreGeneralThan(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#value2")));
        assertFalse(compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreGeneralThan(literal("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#value2")));
        assertFalse(compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreGeneralThan(wildcard("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#value2")));

        assertTrue(compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreGeneralThan(compactible("key1#value1")));
        assertTrue(compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreGeneralThan(literal("key1#value1")));
        assertTrue(compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreGeneralThan(wildcard("key1#value1")));

        assertTrue(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreGeneralThan(compactible("key1#value1#key2#value2")));
        assertTrue(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreGeneralThan(literal("key1#value1#key2#value2")));
        assertTrue(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreGeneralThan(wildcard("key1#value1#key2#value2")));

        assertTrue(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreGeneralThan(compactible("key2#value2")));
        assertTrue(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreGeneralThan(literal("key2#value2")));
        assertTrue(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreGeneralThan(wildcard("key2#value2")));

        assertFalse(compactible("key1#value1").isMoreGeneralThan(compactible("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1").isMoreGeneralThan(literal("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1").isMoreGeneralThan(wildcard("key1#value1#key2#value2")));

        assertFalse(compactible("key1#value1#key2#value2").isMoreGeneralThan(compactible("key1#value1")));
        assertFalse(compactible("key1#value1#key2#value2").isMoreGeneralThan(literal("key1#value1")));
        assertFalse(compactible("key1#value1#key2#value2").isMoreGeneralThan(wildcard("key1#value1")));

        assertFalse(compactible("key1#value1#key2#value2").isMoreGeneralThan(compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE)));
        assertFalse(compactible("key1#value1#key2#value2").isMoreGeneralThan(literal("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE)));
        assertFalse(compactible("key1#value1#key2#value2").isMoreGeneralThan(wildcard("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE)));

        assertFalse(compactible("key1#value1#key2#value2").isMoreGeneralThan(compactible("")));
        assertFalse(compactible("key1#value1#key2#value2").isMoreGeneralThan(literal("")));
        assertFalse(compactible("key1#value1#key2#value2").isMoreGeneralThan(wildcard("")));

        assertFalse(compactible("").isMoreGeneralThan(compactible("key1#value1#key2#value2")));
        assertFalse(compactible("").isMoreGeneralThan(literal("key1#value1#key2#value2")));
        assertFalse(compactible("").isMoreGeneralThan(wildcard("key1#value1#key2#value2")));

        assertTrue(compactible("").isMoreGeneralThan(compactible("")));
        assertTrue(compactible("").isMoreGeneralThan(literal("")));
        assertTrue(compactible("").isMoreGeneralThan(wildcard("")));

        assertFalse(compactible("key1#value1#key2#value3").isMoreGeneralThan(compactible("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1#key2#value3").isMoreGeneralThan(literal("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1#key2#value3").isMoreGeneralThan(wildcard("key1#value1#key2#value2")));

        assertFalse(compactible("key1#value1#key3#value3").isMoreGeneralThan(compactible("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1#key3#value3").isMoreGeneralThan(literal("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1#key3#value3").isMoreGeneralThan(wildcard("key1#value1#key2#value2")));

        assertFalse(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key3#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreGeneralThan(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#" + CompactiblePropertiesImpl.ANY_VALUE)));
    }

    @Test
    public void shouldCorrectlyJudgeMoreSpecific() {
        assertTrue(compactible("key1#value1#key2#value2").isMoreSpecificThan(compactible("key1#value1#key2#value2")));
        assertTrue(compactible("key1#value1#key2#value2").isMoreSpecificThan(literal("key1#value1#key2#value2")));
        assertTrue(compactible("key1#value1#key2#value2").isMoreSpecificThan(wildcard("key1#value1#key2#value2")));

        assertFalse(compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreSpecificThan(compactible("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreSpecificThan(literal("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreSpecificThan(wildcard("key1#value1#key2#value2")));

        assertTrue(compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreSpecificThan(compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE)));
        assertTrue(compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreSpecificThan(literal("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE)));
        assertTrue(compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreSpecificThan(wildcard("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE)));

        assertFalse(compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreSpecificThan(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#value2")));
        assertFalse(compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreSpecificThan(literal("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#value2")));
        assertFalse(compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreSpecificThan(wildcard("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#value2")));

        assertFalse(compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreSpecificThan(compactible("key1#value1")));
        assertFalse(compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreSpecificThan(literal("key1#value1")));
        assertTrue(compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreSpecificThan(wildcard("key1#value1")));

        assertFalse(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreSpecificThan(compactible("key1#value1#key2#value2")));
        assertFalse(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreSpecificThan(literal("key1#value1#key2#value2")));
        assertFalse(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreSpecificThan(wildcard("key1#value1#key2#value2")));

        assertFalse(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreSpecificThan(compactible("key2#value2")));
        assertFalse(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreSpecificThan(literal("key2#value2")));
        assertFalse(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#" + CompactiblePropertiesImpl.ANY_VALUE).isMoreSpecificThan(wildcard("key2#value2")));

        assertFalse(compactible("key1#value1").isMoreSpecificThan(compactible("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1").isMoreSpecificThan(literal("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1").isMoreSpecificThan(wildcard("key1#value1#key2#value2")));

        assertFalse(compactible("key1#value1#key2#value2").isMoreSpecificThan(compactible("key1#value1")));
        assertFalse(compactible("key1#value1#key2#value2").isMoreSpecificThan(literal("key1#value1")));
        assertTrue(compactible("key1#value1#key2#value2").isMoreSpecificThan(wildcard("key1#value1")));

        assertTrue(compactible("key1#value1#key2#value2").isMoreSpecificThan(compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE)));
        assertTrue(compactible("key1#value1#key2#value2").isMoreSpecificThan(literal("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE)));
        assertTrue(compactible("key1#value1#key2#value2").isMoreSpecificThan(wildcard("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE)));

        assertFalse(compactible("key1#value1#key2#value2").isMoreSpecificThan(compactible("")));
        assertFalse(compactible("key1#value1#key2#value2").isMoreSpecificThan(literal("")));
        assertTrue(compactible("key1#value1#key2#value2").isMoreSpecificThan(wildcard("")));

        assertFalse(compactible("").isMoreSpecificThan(compactible("key1#value1#key2#value2")));
        assertFalse(compactible("").isMoreSpecificThan(literal("key1#value1#key2#value2")));
        assertFalse(compactible("").isMoreSpecificThan(wildcard("key1#value1#key2#value2")));

        assertTrue(compactible("").isMoreSpecificThan(compactible("")));
        assertTrue(compactible("").isMoreSpecificThan(literal("")));
        assertTrue(compactible("").isMoreSpecificThan(wildcard("")));

        assertFalse(compactible("key1#value1#key2#value3").isMoreSpecificThan(compactible("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1#key2#value3").isMoreSpecificThan(literal("key1#value1#key2#value2")));
        assertFalse(compactible("key1#value1#key2#value3").isMoreSpecificThan(wildcard("key1#value1#key2#value2")));
    }

    @Test
    public void shouldGenerateAllMoreGeneral() {
        Set<CompactibleProperties> result = compactible("key1#value1#key2#value2#key3#value3").generateAllMoreGeneral(Collections.<String>emptySet());

        assertEquals(7, result.size());
        Iterator<CompactibleProperties> iterator = result.iterator();
        Assert.assertEquals(iterator.next(), compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#value2#key3#value3"));
        Assert.assertEquals(iterator.next(), compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE + "#key3#value3"));
        Assert.assertEquals(iterator.next(), compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#" + CompactiblePropertiesImpl.ANY_VALUE + "#key3#value3"));
        Assert.assertEquals(iterator.next(), compactible("key1#value1#key2#value2#key3#" + CompactiblePropertiesImpl.ANY_VALUE));
        Assert.assertEquals(iterator.next(), compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#value2#key3#" + CompactiblePropertiesImpl.ANY_VALUE));
        Assert.assertEquals(iterator.next(), compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE + "#key3#" + CompactiblePropertiesImpl.ANY_VALUE));
        Assert.assertEquals(iterator.next(), compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#" + CompactiblePropertiesImpl.ANY_VALUE + "#key3#" + CompactiblePropertiesImpl.ANY_VALUE));
    }

    @Test
    public void shouldGenerateAllMoreGeneralWithExtraKeys() {
        Set<CompactibleProperties> result = compactible("key1#value1#key2#value2").generateAllMoreGeneral(Collections.singleton("key3"));

        assertEquals(7, result.size());
        Iterator<CompactibleProperties> iterator = result.iterator();
        Assert.assertEquals(iterator.next(), compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#value2"));
        Assert.assertEquals(iterator.next(), compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE));
        Assert.assertEquals(iterator.next(), compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#" + CompactiblePropertiesImpl.ANY_VALUE));
        Assert.assertEquals(iterator.next(), compactible("key1#value1#key2#value2#key3#" + CompactiblePropertiesImpl.ANY_VALUE));
        Assert.assertEquals(iterator.next(), compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#value2#key3#" + CompactiblePropertiesImpl.ANY_VALUE));
        Assert.assertEquals(iterator.next(), compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE + "#key3#" + CompactiblePropertiesImpl.ANY_VALUE));
        Assert.assertEquals(iterator.next(), compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#" + CompactiblePropertiesImpl.ANY_VALUE + "#key3#" + CompactiblePropertiesImpl.ANY_VALUE));
    }

    @Test
    public void shouldGenerateAllMoreGeneralWithRedundantKeys() {
        Set<CompactibleProperties> result = compactible("key1#value1#key2#value2").generateAllMoreGeneral(new HashSet<>(Arrays.asList("key1", "key2", "key3")));

        assertEquals(7, result.size());
        Iterator<CompactibleProperties> iterator = result.iterator();
        Assert.assertEquals(iterator.next(), compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#value2"));
        Assert.assertEquals(iterator.next(), compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE));
        Assert.assertEquals(iterator.next(), compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#" + CompactiblePropertiesImpl.ANY_VALUE));
        Assert.assertEquals(iterator.next(), compactible("key1#value1#key2#value2#key3#" + CompactiblePropertiesImpl.ANY_VALUE));
        Assert.assertEquals(iterator.next(), compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#value2#key3#" + CompactiblePropertiesImpl.ANY_VALUE));
        Assert.assertEquals(iterator.next(), compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE + "#key3#" + CompactiblePropertiesImpl.ANY_VALUE));
        Assert.assertEquals(iterator.next(), compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#" + CompactiblePropertiesImpl.ANY_VALUE + "#key3#" + CompactiblePropertiesImpl.ANY_VALUE));
    }

    @Test
    public void shouldGenerateNoneWhenItIsTheMostGeneral() {
        Set<CompactibleProperties> result = compactible("\"key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#" + CompactiblePropertiesImpl.ANY_VALUE).generateAllMoreGeneral(Collections.<String>emptySet());

        assertEquals(0, result.size());
    }

    @Test
    public void equalityTest() {
        assertTrue(compactible("key1#value1").equals(compactible("key1#value1")));
        assertTrue(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE).equals(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE)));
        assertFalse(compactible("key1#value1").equals(compactible("key1#value2")));
        assertFalse(compactible("key1#value1").equals(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE)));
        assertFalse(compactible("key1#value1").equals(compactible("")));
    }

    @Test
    public void shouldAchieveSpecificToGeneralOrderingForProperties() {
        Set<CompactibleProperties> properties = new TreeSet<>();

        properties.add(compactible("key1#value1#key2#value2"));
        properties.add(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#value2"));
        properties.add(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#value2"));
        properties.add(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#value2"));
        properties.add(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#" + CompactiblePropertiesImpl.ANY_VALUE));
        properties.add(compactible(""));

        Iterator<CompactibleProperties> iterator = properties.iterator();
        Assert.assertEquals(compactible(""), iterator.next());
        Assert.assertEquals(compactible("key1#value1#key2#value2"), iterator.next());
        Assert.assertEquals(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#value2"), iterator.next());
        Assert.assertEquals(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#" + CompactiblePropertiesImpl.ANY_VALUE), iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void propertiesShouldBehaveProperlyInTreeSets() {
        Set<CompactibleProperties> properties = new TreeSet<>();

        properties.add(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#" + CompactiblePropertiesImpl.ANY_VALUE));
        properties.add(compactible("key1#value1#key2#value2"));
        properties.add(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#value2"));
        properties.add(compactible("key2#" + CompactiblePropertiesImpl.ANY_VALUE + "#key1#value2"));
        properties.add(compactible("key2#value1#key1#" + CompactiblePropertiesImpl.ANY_VALUE));
        properties.add(compactible(""));

        assertTrue(properties.contains(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#" + CompactiblePropertiesImpl.ANY_VALUE)));
        assertTrue(properties.contains(compactible("key1#value1#key2#value2")));
        assertTrue(properties.contains(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#value2")));
        assertTrue(properties.contains(compactible("key1#value2#key2#" + CompactiblePropertiesImpl.ANY_VALUE + "#")));
        assertTrue(properties.contains(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#value1")));
        assertTrue(properties.contains(compactible("")));
        assertFalse(properties.contains(compactible("key1#value1#key2#" + CompactiblePropertiesImpl.ANY_VALUE + "#")));
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

        assertFalse(compactible("key1#value1").isMutuallyExclusive(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE)));
        assertFalse(compactible("key1#value1").isMutuallyExclusive(literal("key1#" + CompactiblePropertiesImpl.ANY_VALUE)));
        assertFalse(compactible("key1#value1").isMutuallyExclusive(wildcard("key1#" + CompactiblePropertiesImpl.ANY_VALUE)));

        assertFalse(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE).isMutuallyExclusive(compactible("key1#value1")));
        assertFalse(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE).isMutuallyExclusive(literal("key1#value1")));
        assertFalse(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE).isMutuallyExclusive(wildcard("key1#value1")));

        assertFalse(compactible("").isMutuallyExclusive(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE)));
        assertFalse(compactible("").isMutuallyExclusive(literal("key1#" + CompactiblePropertiesImpl.ANY_VALUE)));
        assertFalse(compactible("").isMutuallyExclusive(wildcard("key1#" + CompactiblePropertiesImpl.ANY_VALUE)));

        assertFalse(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE).isMutuallyExclusive(compactible("")));
        assertFalse(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE).isMutuallyExclusive(literal("")));
        assertFalse(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE).isMutuallyExclusive(wildcard("")));

        assertFalse(compactible("key1#value1#key2#value2").isMutuallyExclusive(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#value2")));
        assertFalse(compactible("key1#value1#key2#value2").isMutuallyExclusive(literal("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#value2")));
        assertFalse(compactible("key1#value1#key2#value2").isMutuallyExclusive(wildcard("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#value2")));

        assertTrue(compactible("key1#value1#key2#value3").isMutuallyExclusive(compactible("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#value2")));
        assertTrue(compactible("key1#value1#key2#value3").isMutuallyExclusive(literal("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#value2")));
        assertTrue(compactible("key1#value1#key2#value3").isMutuallyExclusive(wildcard("key1#" + CompactiblePropertiesImpl.ANY_VALUE + "#key2#value2")));
    }


    // just for readability

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
