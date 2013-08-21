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

package com.graphaware.relcount.full.internal.dto.relationship;

import com.graphaware.relcount.full.internal.dto.property.CacheablePropertiesDescriptionImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link CacheableRelationshipDescriptionImpl}.
 */
public class CacheableRelationshipDescriptionImplTest {

    @Test
    public void relationshipShouldBeCorrectlyConstructed() {
        CacheableRelationshipDescription relationship = compactible("test#INCOMING#key1#value1#key2#value2");

        assertTrue(relationship.getProperties().containsKey("key1"));
        assertTrue(relationship.getProperties().containsKey("key2"));
        Assert.assertEquals(2, relationship.getProperties().size());
    }

    @Test
    public void shouldCorrectlyJudgeMoreGeneral() {
        assertTrue(compactible("test#INCOMING#key1#value1#key2#value2").isMoreGeneralThan(compactible("test#INCOMING#key1#value1#key2#value2")));
        assertTrue(compactible("test#INCOMING#key1#value1#key2#value2").isMoreGeneralThan(literal("test#INCOMING#key1#value1#key2#value2")));
        assertTrue(compactible("test#INCOMING#key1#value1#key2#value2").isMoreGeneralThan(wildcard("test#INCOMING#key1#value1#key2#value2")));

        assertFalse(compactible("test#INCOMING#key1#value1#key2#value2").isMoreGeneralThan(compactible("test#OUTGOING#key1#value1#key2#value2")));
        assertFalse(compactible("test#INCOMING#key1#value1#key2#value2").isMoreGeneralThan(literal("test#OUTGOING#key1#value1#key2#value2")));
        assertFalse(compactible("test#INCOMING#key1#value1#key2#value2").isMoreGeneralThan(wildcard("test#OUTGOING#key1#value1#key2#value2")));

        assertFalse(compactible("test#INCOMING#key1#value1#key2#value2").isMoreGeneralThan(compactible("test#INCOMING#key2#value2")));
        assertFalse(compactible("test#INCOMING#key1#value1#key2#value2").isMoreGeneralThan(literal("test#INCOMING#key2#value2")));
        assertFalse(compactible("test#INCOMING#key1#value1#key2#value2").isMoreGeneralThan(wildcard("test#INCOMING#key2#value2")));
    }

    @Test
    public void shouldCorrectlyJudgeMoreSpecific() {
        assertTrue(compactible("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(compactible("test#INCOMING#key1#value1#key2#value2")));
        assertTrue(compactible("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(literal("test#INCOMING#key1#value1#key2#value2")));
        assertTrue(compactible("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(wildcard("test#INCOMING#key1#value1#key2#value2")));

        assertFalse(compactible("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(compactible("test#OUTGOING#key1#value1#key2#value2")));
        assertFalse(compactible("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(literal("test#OUTGOING#key1#value1#key2#value2")));
        assertFalse(compactible("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(wildcard("test#OUTGOING#key1#value1#key2#value2")));

        assertFalse(compactible("test#INCOMING#key2#value2").isMoreSpecificThan(compactible("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(compactible("test#INCOMING#key2#value2").isMoreSpecificThan(literal("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(compactible("test#INCOMING#key2#value2").isMoreSpecificThan(wildcard("test#INCOMING#key1#value1#key2#value2")));
    }

    @Test
    public void shouldGenerateAllMoreGeneral() {
        Set<CacheableRelationshipDescription> result = compactible("test#INCOMING#key1#value1#key2#value2").generateAllMoreGeneral(Collections.<String>emptySet());

        assertEquals(3, result.size());
        Iterator<CacheableRelationshipDescription> iterator = result.iterator();
        Assert.assertEquals(iterator.next(), compactible("test#INCOMING#key1#" + CacheablePropertiesDescriptionImpl.ANY_VALUE + "#key2#value2"));
        Assert.assertEquals(iterator.next(), compactible("test#INCOMING#key1#value1#key2#" + CacheablePropertiesDescriptionImpl.ANY_VALUE));
        Assert.assertEquals(iterator.next(), compactible("test#INCOMING#key1#" + CacheablePropertiesDescriptionImpl.ANY_VALUE + "#key2#" + CacheablePropertiesDescriptionImpl.ANY_VALUE));
    }

    @Test
    public void shouldGenerateAllMoreGeneralWithUnknownKeys() {
        Set<CacheableRelationshipDescription> result = compactible("test#INCOMING#key1#value1").generateAllMoreGeneral(Collections.singleton("key2"));

        assertEquals(3, result.size());
        Iterator<CacheableRelationshipDescription> iterator = result.iterator();
        Assert.assertEquals(iterator.next(), compactible("test#INCOMING#key1#" + CacheablePropertiesDescriptionImpl.ANY_VALUE));
        Assert.assertEquals(iterator.next(), compactible("test#INCOMING#key1#value1#key2#" + CacheablePropertiesDescriptionImpl.ANY_VALUE));
        Assert.assertEquals(iterator.next(), compactible("test#INCOMING#key1#" + CacheablePropertiesDescriptionImpl.ANY_VALUE + "#key2#" + CacheablePropertiesDescriptionImpl.ANY_VALUE));
    }

    @Test
    public void shouldGenerateAllMoreGeneralWithRedundantUnknownKeys() {
        Set<CacheableRelationshipDescription> result = compactible("test#INCOMING#key1#value1").generateAllMoreGeneral(new HashSet<>(Arrays.asList("key1", "key2")));

        assertEquals(3, result.size());
        Iterator<CacheableRelationshipDescription> iterator = result.iterator();
        Assert.assertEquals(iterator.next(), compactible("test#INCOMING#key1#" + CacheablePropertiesDescriptionImpl.ANY_VALUE));
        Assert.assertEquals(iterator.next(), compactible("test#INCOMING#key1#value1#key2#" + CacheablePropertiesDescriptionImpl.ANY_VALUE));
        Assert.assertEquals(iterator.next(), compactible("test#INCOMING#key1#" + CacheablePropertiesDescriptionImpl.ANY_VALUE + "#key2#" + CacheablePropertiesDescriptionImpl.ANY_VALUE));
    }

    @Test
    public void shouldAchieveSpecificToGeneralOrderingForRelationships() {
        Set<CacheableRelationshipDescription> properties = new TreeSet<>();

        properties.add(compactible("test#INCOMING#key1#" + CacheablePropertiesDescriptionImpl.ANY_VALUE + "#key2#" + CacheablePropertiesDescriptionImpl.ANY_VALUE));
        properties.add(compactible("test#INCOMING#key1#value1#key2#value2"));
        properties.add(compactible("test#INCOMING#key1#" + CacheablePropertiesDescriptionImpl.ANY_VALUE + "#key2#value2"));
        properties.add(compactible("test#INCOMING#key1#" + CacheablePropertiesDescriptionImpl.ANY_VALUE + "#key2#value2"));
        properties.add(compactible("test#INCOMING#key1#" + CacheablePropertiesDescriptionImpl.ANY_VALUE + "#key2#value2"));
        properties.add(compactible("xx#INCOMING#key1#" + CacheablePropertiesDescriptionImpl.ANY_VALUE + "#key2#value2"));
        properties.add(compactible("test#OUTGOING#key1#" + CacheablePropertiesDescriptionImpl.ANY_VALUE + "#key2#value2"));

        Iterator<CacheableRelationshipDescription> iterator = properties.iterator();
        Assert.assertEquals(compactible("test#INCOMING#key1#value1#key2#value2"), iterator.next());
        Assert.assertEquals(compactible("test#INCOMING#key1#" + CacheablePropertiesDescriptionImpl.ANY_VALUE + "#key2#value2"), iterator.next());
        Assert.assertEquals(compactible("test#INCOMING#key1#" + CacheablePropertiesDescriptionImpl.ANY_VALUE + "#key2#" + CacheablePropertiesDescriptionImpl.ANY_VALUE), iterator.next());
        Assert.assertEquals(compactible("test#OUTGOING#key1#" + CacheablePropertiesDescriptionImpl.ANY_VALUE + "#key2#value2"), iterator.next());
        Assert.assertEquals(compactible("xx#INCOMING#key1#" + CacheablePropertiesDescriptionImpl.ANY_VALUE + "#key2#value2"), iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void relationshipsShouldBehaveProperlyInTreeSets() {
        Set<CacheableRelationshipDescription> properties = new TreeSet<>();

        properties.add(compactible("test#INCOMING#key1#" + CacheablePropertiesDescriptionImpl.ANY_VALUE + "#key2#" + CacheablePropertiesDescriptionImpl.ANY_VALUE));
        properties.add(compactible("test#INCOMING#key1#value1#key2#value2"));
        properties.add(compactible("test#INCOMING#key1#" + CacheablePropertiesDescriptionImpl.ANY_VALUE + "#key2#value2"));
        properties.add(compactible("test#INCOMING#key2#" + CacheablePropertiesDescriptionImpl.ANY_VALUE + "#key1#value2"));
        properties.add(compactible("test#INCOMING#key1#" + CacheablePropertiesDescriptionImpl.ANY_VALUE + "#key2#value1"));

        assertTrue(properties.contains(compactible("test#INCOMING#key1#" + CacheablePropertiesDescriptionImpl.ANY_VALUE + "#key2#" + CacheablePropertiesDescriptionImpl.ANY_VALUE)));
        assertTrue(properties.contains(compactible("test#INCOMING#key1#value1#key2#value2")));
        assertTrue(properties.contains(compactible("test#INCOMING#key1#" + CacheablePropertiesDescriptionImpl.ANY_VALUE + "#key2#value2")));
        assertTrue(properties.contains(compactible("test#INCOMING#key1#value2#key2#" + CacheablePropertiesDescriptionImpl.ANY_VALUE)));
        assertTrue(properties.contains(compactible("test#INCOMING#key1#" + CacheablePropertiesDescriptionImpl.ANY_VALUE + "#key2#value1")));
        assertFalse(properties.contains(compactible("test#INCOMING#key1#value1")));
    }

    @Test
    public void shouldCorrectlyConvertToString() {
        Assert.assertEquals("PREFIXtest#OUTGOING#key1#value1#key2#value2", new CacheableRelationshipDescriptionImpl("PREFIXtest#OUTGOING#key1#value1#key2#value2", "PREFIX", "#").toString("PREFIX", "#"));
        Assert.assertEquals("_PREFIX_" + "test#INCOMING", new CacheableRelationshipDescriptionImpl("_PREFIX_test#INCOMING", "_PREFIX_", "#").toString("_PREFIX_", "#"));
    }

    @Test
    public void sameRelationshipsShouldBeEqual() {
        assertTrue(new CacheableRelationshipDescriptionImpl("PREFIX" + "test_INCOMING", "PREFIX", "_").equals(new CacheableRelationshipDescriptionImpl("bla" + "test#INCOMING", "bla", "#")));
        assertTrue(new CacheableRelationshipDescriptionImpl("test#OUTGOING#key1#value1#key2#value2", "", "#").equals(new CacheableRelationshipDescriptionImpl("test#OUTGOING#key1#value1#key2#value2", null, "#")));
        assertTrue(new CacheableRelationshipDescriptionImpl("test#OUTGOING#key1#value1#key2#" + CacheablePropertiesDescriptionImpl.ANY_VALUE + "", "", "#").equals(new CacheableRelationshipDescriptionImpl("test#OUTGOING#key1#value1#key2#" + CacheablePropertiesDescriptionImpl.ANY_VALUE + "", null, "#")));
        assertTrue(new CacheableRelationshipDescriptionImpl("test#INCOMING#key1#value1#key2#", "", "#").equals(new CacheableRelationshipDescriptionImpl("test#INCOMING#key1#value1#key2", null, "#")));
    }

    @Test
    public void differentRelationshipsShouldNotBeEqual() {
        assertFalse(new CacheableRelationshipDescriptionImpl("test#OUTGOING", null, "#").equals(new CacheableRelationshipDescriptionImpl("test#INCOMING", null, "#")));
        assertFalse(new CacheableRelationshipDescriptionImpl("test2#OUTGOING#key1#value1#key2#value2", null, "#").equals(new CacheableRelationshipDescriptionImpl("test#OUTGOING#key1#value1#key2#value2", null, "#")));
        assertFalse(new CacheableRelationshipDescriptionImpl("test2#OUTGOING#key1#value1#key2#" + CacheablePropertiesDescriptionImpl.ANY_VALUE + "", null, "#").equals(new CacheableRelationshipDescriptionImpl("test2#OUTGOING#key1#value1#key2#value2", null, "#")));
        assertFalse(new CacheableRelationshipDescriptionImpl("test#OUTGOING#key3#value1#key2#value2", null, "#").equals(new CacheableRelationshipDescriptionImpl("test#OUTGOING#key1#value1#key2#value2#", null, "#")));
    }

    @Test
    public void verifyMutuallyExclusive() {
        assertFalse(compactible("test#INCOMING#key1#value1#key2#value2").isMutuallyExclusive(compactible("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(compactible("test#INCOMING#key1#value1#key2#value2").isMutuallyExclusive(literal("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(compactible("test#INCOMING#key1#value1#key2#value2").isMutuallyExclusive(wildcard("test#INCOMING#key1#value1#key2#value2")));

        assertTrue(compactible("test#OUTGOING#key1#value1#key2#value2").isMutuallyExclusive(compactible("test#INCOMING#key1#value1#key2#value2")));
        assertTrue(compactible("test#OUTGOING#key1#value1#key2#value2").isMutuallyExclusive(literal("test#INCOMING#key1#value1#key2#value2")));
        assertTrue(compactible("test#OUTGOING#key1#value1#key2#value2").isMutuallyExclusive(wildcard("test#INCOMING#key1#value1#key2#value2")));

        assertTrue(compactible("test2#INCOMING#key1#value1#key2#value2").isMutuallyExclusive(compactible("test#INCOMING#key1#value1#key2#value2")));
        assertTrue(compactible("test2#INCOMING#key1#value1#key2#value2").isMutuallyExclusive(literal("test#INCOMING#key1#value1#key2#value2")));
        assertTrue(compactible("test2#INCOMING#key1#value1#key2#value2").isMutuallyExclusive(wildcard("test#INCOMING#key1#value1#key2#value2")));

        assertTrue(compactible("test#INCOMING#key1#value1#key2#value2").isMutuallyExclusive(compactible("test2#INCOMING#key1#value1#key2#value2")));
        assertTrue(compactible("test#INCOMING#key1#value1#key2#value2").isMutuallyExclusive(literal("test2#INCOMING#key1#value1#key2#value2")));
        assertTrue(compactible("test#INCOMING#key1#value1#key2#value2").isMutuallyExclusive(wildcard("test2#INCOMING#key1#value1#key2#value2")));

        assertFalse(compactible("test#INCOMING#key1#value1#key2#value2").isMutuallyExclusive(compactible("test#INCOMING#key1#value1#key2#" + CacheablePropertiesDescriptionImpl.ANY_VALUE)));
        assertFalse(compactible("test#INCOMING#key1#value1#key2#value2").isMutuallyExclusive(literal("test#INCOMING#key1#value1#key2#" + CacheablePropertiesDescriptionImpl.ANY_VALUE)));
        assertFalse(compactible("test#INCOMING#key1#value1#key2#value2").isMutuallyExclusive(wildcard("test#INCOMING#key1#value1#key2#" + CacheablePropertiesDescriptionImpl.ANY_VALUE)));
    }


    private CacheableRelationshipDescription compactible(String s) {
        return new CacheableRelationshipDescriptionImpl(s, null, "#");
    }

    private RelationshipDescription literal(String s) {
        return new LiteralRelationshipDescription(s, null, "#");
    }

    private RelationshipDescription wildcard(String s) {
        return new WildcardRelationshipDescription(s, null, "#");
    }
}
