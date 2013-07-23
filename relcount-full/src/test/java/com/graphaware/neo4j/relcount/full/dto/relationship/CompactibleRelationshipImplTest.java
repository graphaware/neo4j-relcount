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

package com.graphaware.neo4j.relcount.full.dto.relationship;

import org.junit.Test;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import static com.graphaware.neo4j.relcount.full.dto.property.CompactiblePropertiesImpl.ANY_VALUE;
import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link }.
 */
public class CompactibleRelationshipImplTest {

    @Test
    public void relationshipShouldBeCorrectlyConstructed() {
        CompactibleRelationship relationship = compactible("test#INCOMING#key1#value1#key2#value2");

        assertTrue(relationship.getProperties().containsKey("key1"));
        assertTrue(relationship.getProperties().containsKey("key2"));
        assertEquals(2, relationship.getProperties().size());
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
        Set<CompactibleRelationship> result = compactible("test#INCOMING#key1#value1#key2#value2").generateAllMoreGeneral();

        assertEquals(4, result.size());
        Iterator<CompactibleRelationship> iterator = result.iterator();
        assertEquals(iterator.next(), compactible("test#INCOMING#key1#value1#key2#value2"));
        assertEquals(iterator.next(), compactible("test#INCOMING#key1#" + ANY_VALUE + "#key2#value2"));
        assertEquals(iterator.next(), compactible("test#INCOMING#key1#value1#key2#" + ANY_VALUE));
        assertEquals(iterator.next(), compactible("test#INCOMING#key1#" + ANY_VALUE + "#key2#" + ANY_VALUE));
    }

    @Test
    public void shouldGenerateOneMoreGeneral() {
        Set<CompactibleRelationship> result = compactible("test#INCOMING#key1#value1#key2#value2").generateOneMoreGeneral();

        assertEquals(3, result.size());
        Iterator<CompactibleRelationship> iterator = result.iterator();
        assertEquals(iterator.next(), compactible("test#INCOMING#key1#value1#key2#value2"));
        assertEquals(iterator.next(), compactible("test#INCOMING#key1#" + ANY_VALUE + "#key2#value2"));
        assertEquals(iterator.next(), compactible("test#INCOMING#key1#value1#key2#" + ANY_VALUE));
    }

    @Test
    public void shouldAchieveSpecificToGeneralOrderingForRelationships() {
        Set<CompactibleRelationship> properties = new TreeSet<>();

        properties.add(compactible("test#INCOMING#key1#" + ANY_VALUE + "#key2#" + ANY_VALUE));
        properties.add(compactible("test#INCOMING#key1#value1#key2#value2"));
        properties.add(compactible("test#INCOMING#key1#" + ANY_VALUE + "#key2#value2"));
        properties.add(compactible("test#INCOMING#key1#" + ANY_VALUE + "#key2#value2"));
        properties.add(compactible("test#INCOMING#key1#" + ANY_VALUE + "#key2#value2"));
        properties.add(compactible("xx#INCOMING#key1#" + ANY_VALUE + "#key2#value2"));
        properties.add(compactible("test#OUTGOING#key1#" + ANY_VALUE + "#key2#value2"));

        Iterator<CompactibleRelationship> iterator = properties.iterator();
        assertEquals(compactible("test#INCOMING#key1#value1#key2#value2"), iterator.next());
        assertEquals(compactible("test#INCOMING#key1#" + ANY_VALUE + "#key2#value2"), iterator.next());
        assertEquals(compactible("test#INCOMING#key1#" + ANY_VALUE + "#key2#" + ANY_VALUE), iterator.next());
        assertEquals(compactible("test#OUTGOING#key1#" + ANY_VALUE + "#key2#value2"), iterator.next());
        assertEquals(compactible("xx#INCOMING#key1#" + ANY_VALUE + "#key2#value2"), iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void relationshipsShouldBehaveProperlyInTreeSets() {
        Set<CompactibleRelationship> properties = new TreeSet<>();

        properties.add(compactible("test#INCOMING#key1#" + ANY_VALUE + "#key2#" + ANY_VALUE));
        properties.add(compactible("test#INCOMING#key1#value1#key2#value2"));
        properties.add(compactible("test#INCOMING#key1#" + ANY_VALUE + "#key2#value2"));
        properties.add(compactible("test#INCOMING#key2#" + ANY_VALUE + "#key1#value2"));
        properties.add(compactible("test#INCOMING#key1#" + ANY_VALUE + "#key2#value1"));

        assertTrue(properties.contains(compactible("test#INCOMING#key1#" + ANY_VALUE + "#key2#" + ANY_VALUE)));
        assertTrue(properties.contains(compactible("test#INCOMING#key1#value1#key2#value2")));
        assertTrue(properties.contains(compactible("test#INCOMING#key1#" + ANY_VALUE + "#key2#value2")));
        assertTrue(properties.contains(compactible("test#INCOMING#key1#value2#key2#" + ANY_VALUE)));
        assertTrue(properties.contains(compactible("test#INCOMING#key1#" + ANY_VALUE + "#key2#value1")));
        assertFalse(properties.contains(compactible("test#INCOMING#key1#value1")));
    }

    @Test
    public void shouldCorrectlyConvertToString() {
        assertEquals("PREFIXtest#OUTGOING#key1#value1#key2#value2", new CompactibleRelationshipImpl("PREFIXtest#OUTGOING#key1#value1#key2#value2", "PREFIX", "#").toString("PREFIX", "#"));
        assertEquals("_PREFIX_" + "test#INCOMING", new CompactibleRelationshipImpl("_PREFIX_test#INCOMING", "_PREFIX_", "#").toString("_PREFIX_", "#"));
        assertEquals("_PREFIX_" + "test#INCOMING", new CompactibleRelationshipImpl("_PREFIX_test#INCOMING#", "_PREFIX_", "#").toString("_PREFIX_", "#"));
    }

    @Test
    public void sameRelationshipsShouldBeEqual() {
        assertTrue(new CompactibleRelationshipImpl("PREFIX" + "test_INCOMING", "PREFIX", "_").equals(new CompactibleRelationshipImpl("bla" + "test#INCOMING", "bla", "#")));
        assertTrue(new CompactibleRelationshipImpl("test#OUTGOING#key1#value1#key2#value2", "", "#").equals(new CompactibleRelationshipImpl("test#OUTGOING#key1#value1#key2#value2", null, "#")));
        assertTrue(new CompactibleRelationshipImpl("test#OUTGOING#key1#value1#key2#" + ANY_VALUE + "", "", "#").equals(new CompactibleRelationshipImpl("test#OUTGOING#key1#value1#key2#" + ANY_VALUE + "", null, "#")));
        assertTrue(new CompactibleRelationshipImpl("test#INCOMING#key1#value1#key2#", "", "#").equals(new CompactibleRelationshipImpl("test#INCOMING#key1#value1#key2", null, "#")));
    }

    @Test
    public void differentRelationshipsShouldNotBeEqual() {
        assertFalse(new CompactibleRelationshipImpl("test#OUTGOING", null, "#").equals(new CompactibleRelationshipImpl("test#INCOMING", null, "#")));
        assertFalse(new CompactibleRelationshipImpl("test2#OUTGOING#key1#value1#key2#value2", null, "#").equals(new CompactibleRelationshipImpl("test#OUTGOING#key1#value1#key2#value2", null, "#")));
        assertFalse(new CompactibleRelationshipImpl("test2#OUTGOING#key1#value1#key2#" + ANY_VALUE + "", null, "#").equals(new CompactibleRelationshipImpl("test2#OUTGOING#key1#value1#key2#value2", null, "#")));
        assertFalse(new CompactibleRelationshipImpl("test#OUTGOING#key3#value1#key2#value2", null, "#").equals(new CompactibleRelationshipImpl("test#OUTGOING#key1#value1#key2#value2#", null, "#")));
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

        assertFalse(compactible("test#INCOMING#key1#value1#key2#value2").isMutuallyExclusive(compactible("test#INCOMING#key1#value1#key2#" + ANY_VALUE)));
        assertFalse(compactible("test#INCOMING#key1#value1#key2#value2").isMutuallyExclusive(literal("test#INCOMING#key1#value1#key2#" + ANY_VALUE)));
        assertFalse(compactible("test#INCOMING#key1#value1#key2#value2").isMutuallyExclusive(wildcard("test#INCOMING#key1#value1#key2#" + ANY_VALUE)));
    }


    private CompactibleRelationship compactible(String s) {
        return new CompactibleRelationshipImpl(s, null, "#");
    }

    private RelationshipDescription literal(String s) {
        return new LiteralRelationshipDescription(s, null, "#");
    }

    private RelationshipDescription wildcard(String s) {
        return new WildcardRelationshipDescription(s, null, "#");
    }
}
