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

import com.graphaware.neo4j.representation.relationship.Relationship;
import com.graphaware.neo4j.representation.relationship.SimpleRelationship;
import org.junit.Test;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import static com.graphaware.neo4j.relcount.representation.LiteralComparableProperties.LITERAL;
import static com.graphaware.neo4j.utils.Constants.GA_REL_PREFIX;
import static junit.framework.Assert.*;

/**
 * Unit test for {@link com.graphaware.neo4j.relcount.representation.ComparableRelationship}.
 */
public class ComparableRelationshipTest {

    @Test
    public void propertiesShouldBeCorrectlyConstructed() {
        ComparableRelationship relationship = crel("test#INCOMING#" + LITERAL + "key1#value1#key2#value2");

        assertTrue(relationship.getProperties() instanceof LiteralComparableProperties);
        assertTrue(relationship.getProperties().containsKey("key1"));
        assertTrue(relationship.getProperties().containsKey("key2"));
        assertEquals(2, relationship.getProperties().size());

        relationship = crel("test#INCOMING#key1#value1#key2#value2");

        assertFalse(relationship.getProperties() instanceof LiteralComparableProperties);
        assertTrue(relationship.getProperties().containsKey("key1"));
        assertTrue(relationship.getProperties().containsKey("key2"));
        assertEquals(2, relationship.getProperties().size());

        relationship = crel("test#INCOMING#" + LITERAL);

        assertTrue(relationship.getProperties() instanceof LiteralComparableProperties);
        assertEquals(0, relationship.getProperties().size());
    }

    @Test
    public void shouldCorrectlyJudgeMoreGeneral() {
        assertTrue(crel("test#INCOMING#key1#value1#key2#value2").isMoreGeneralThan(srel("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(crel("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreGeneralThan(srel("test#INCOMING#key1#value1#key2#value2")));

        assertTrue(lrel("test#INCOMING#","key1#value1#key2#value2").isMoreGeneralThan(srel("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#", "key1#value1#key2#value2").isStrictlyMoreGeneralThan(srel("test#INCOMING#key1#value1#key2#value2")));

        assertTrue(crel("test#INCOMING#key2#value2").isMoreGeneralThan(srel("test#INCOMING#key1#value1#key2#value2")));
        assertTrue(crel("test#INCOMING#key2#value2").isStrictlyMoreGeneralThan(srel("test#INCOMING#key1#value1#key2#value2")));

        assertFalse(lrel("test#INCOMING#", "key2#value2").isMoreGeneralThan(srel("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#", "key2#value2").isStrictlyMoreGeneralThan(srel("test#INCOMING#key1#value1#key2#value2")));

        assertTrue(crel("test#INCOMING").isMoreGeneralThan(crel("test#INCOMING#key1#value1#key2#value2")));
        assertTrue(crel("test#INCOMING").isStrictlyMoreGeneralThan(crel("test#INCOMING#key1#value1#key2#value2")));

        assertFalse(lrel("test#INCOMING#","").isMoreGeneralThan(crel("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#","").isStrictlyMoreGeneralThan(crel("test#INCOMING#key1#value1#key2#value2")));

        assertFalse(crel("test#INCOMING#key2#value3").isMoreGeneralThan(srel("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(crel("test#INCOMING#key2#value3").isStrictlyMoreGeneralThan(srel("test#INCOMING#key1#value1#key2#value2")));

        assertFalse(lrel("test#INCOMING#","key2#value3").isMoreGeneralThan(srel("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#","key2#value3").isStrictlyMoreGeneralThan(srel("test#INCOMING#key1#value1#key2#value2")));

        assertFalse(crel("test#INCOMING#key1#value1#key2#value2").isMoreGeneralThan(srel("test#OUTGOING#key1#value1#key2#value2")));
        assertFalse(crel("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreGeneralThan(srel("test#OUTGOING#key1#value1#key2#value2")));

        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isMoreGeneralThan(srel("test#OUTGOING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isStrictlyMoreGeneralThan(srel("test#OUTGOING#key1#value1#key2#value2")));

        assertFalse(crel("test#INCOMING#key1#value1#key2#value2").isMoreGeneralThan(srel("test2#INCOMING#key1#value1#key2#value2")));
        assertFalse(crel("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreGeneralThan(srel("test2#INCOMING#key1#value1#key2#value2")));

        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isMoreGeneralThan(srel("test2#INCOMING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isStrictlyMoreGeneralThan(srel("test2#INCOMING#key1#value1#key2#value2")));
    }

    @Test
    public void shouldCorrectlyJudgeMoreSpecific() {
        assertTrue(crel("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(srel("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(crel("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreSpecificThan(srel("test#INCOMING#key1#value1#key2#value2")));

        assertTrue(lrel("test#INCOMING#","key1#value1#key2#value2").isMoreSpecificThan(srel("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isStrictlyMoreSpecificThan(srel("test#INCOMING#key1#value1#key2#value2")));

        assertTrue(crel("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(srel("test#INCOMING#key2#value2")));
        assertTrue(crel("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreSpecificThan(srel("test#INCOMING#key2#value2")));

        assertTrue(lrel("test#INCOMING#","key1#value1#key2#value2").isMoreSpecificThan(srel("test#INCOMING#key2#value2")));
        assertTrue(lrel("test#INCOMING#","key1#value1#key2#value2").isStrictlyMoreSpecificThan(srel("test#INCOMING#key2#value2")));

        assertTrue(crel("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(crel("test#INCOMING")));
        assertTrue(crel("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreSpecificThan(crel("test#INCOMING")));

        assertTrue(lrel("test#INCOMING#","key1#value1#key2#value2").isMoreSpecificThan(crel("test#INCOMING")));
        assertTrue(lrel("test#INCOMING#","key1#value1#key2#value2").isStrictlyMoreSpecificThan(crel("test#INCOMING")));

        assertFalse(crel("test#INCOMING#key2#value2").isMoreSpecificThan(srel("test#INCOMING#key2#value3")));
        assertFalse(crel("test#INCOMING#key2#value2").isStrictlyMoreSpecificThan(srel("test#INCOMING#key2#value3")));

        assertFalse(lrel("test#INCOMING#","key2#value2").isMoreSpecificThan(srel("test#INCOMING#key2#value3")));
        assertFalse(lrel("test#INCOMING#","key2#value2").isStrictlyMoreSpecificThan(srel("test#INCOMING#key2#value3")));

        assertFalse(crel("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(srel("test#OUTGOING#key1#value1#key2#value2")));
        assertFalse(crel("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreSpecificThan(srel("test#OUTGOING#key1#value1#key2#value2")));

        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isMoreSpecificThan(srel("test#OUTGOING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isStrictlyMoreSpecificThan(srel("test#OUTGOING#key1#value1#key2#value2")));

        assertFalse(crel("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(srel("test2#INCOMING#key1#value1#key2#value2")));
        assertFalse(crel("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreSpecificThan(srel("test2#INCOMING#key1#value1#key2#value2")));

        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isMoreSpecificThan(srel("test2#INCOMING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isStrictlyMoreSpecificThan(srel("test2#INCOMING#key1#value1#key2#value2")));
    }

    @Test
    public void shouldGenerateAllMoreGeneral() {
        Set<ComparableRelationship> result = crel("test#INCOMING#key1#value1#key2#value2").generateAllMoreGeneral();

        assertEquals(4, result.size());
        Iterator<ComparableRelationship> iterator = result.iterator();
        assertEquals(iterator.next(), crel("test#INCOMING#key1#value1#key2#value2"));
        assertEquals(iterator.next(), crel("test#INCOMING#key1#value1"));
        assertEquals(iterator.next(), crel("test#INCOMING#key2#value2"));
        assertEquals(iterator.next(), crel("test#INCOMING"));
    }

    @Test
    public void shouldGenerateOneMoreGeneral() {
        Set<ComparableRelationship> result = crel("test#INCOMING#key1#value1#key2#value2").generateOneMoreGeneral();

        assertEquals(3, result.size());
        Iterator<ComparableRelationship> iterator = result.iterator();
        assertEquals(iterator.next(), crel("test#INCOMING#key1#value1#key2#value2"));
        assertEquals(iterator.next(), crel("test#INCOMING#key1#value1"));
        assertEquals(iterator.next(), crel("test#INCOMING#key2#value2"));
    }

    @Test
    public void shouldAchieveSpecificToGeneralOrderingForRelationships() {
        Set<ComparableRelationship> properties = new TreeSet<ComparableRelationship>();

        properties.add(crel("test#INCOMING"));
        properties.add(crel("test#INCOMING#key1#value1#key2#value2"));
        properties.add(crel("test#INCOMING#key2#value2"));
        properties.add(crel("test#INCOMING#key2#value2"));
        properties.add(crel("test#INCOMING#key2#value2"));
        properties.add(crel("xx#INCOMING#key2#value2"));
        properties.add(crel("test#OUTGOING#key2#value2"));

        Iterator<ComparableRelationship> iterator = properties.iterator();
        assertEquals(crel("test#INCOMING#key1#value1#key2#value2"), iterator.next());
        assertEquals(crel("test#INCOMING#key2#value2"), iterator.next());
        assertEquals(crel("test#INCOMING"), iterator.next());
        assertEquals(crel("test#OUTGOING#key2#value2"), iterator.next());
        assertEquals(crel("xx#INCOMING#key2#value2"), iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void relationshipsShouldBehaveProperlyInTreeSets() {
        Set<ComparableRelationship> properties = new TreeSet<ComparableRelationship>();

        properties.add(crel("test#INCOMING"));
        properties.add(crel("test#INCOMING#key1#value1#key2#value2"));
        properties.add(crel("test#INCOMING#key2#value2"));
        properties.add(crel("test#INCOMING#key1#value2"));
        properties.add(crel("test#INCOMING#key2#value1"));

        assertTrue(properties.contains(crel("test#INCOMING")));
        assertTrue(properties.contains(crel("test#INCOMING#key1#value1#key2#value2")));
        assertTrue(properties.contains(crel("test#INCOMING#key2#value2")));
        assertTrue(properties.contains(crel("test#INCOMING#key1#value2")));
        assertTrue(properties.contains(crel("test#INCOMING#key2#value1")));
        assertFalse(properties.contains(crel("test#INCOMING#key1#value1")));
    }

    private ComparableRelationship crel(String s) {
        return new ComparableRelationship(GA_REL_PREFIX + s);
    }

    private Relationship srel(String s) {
        return new SimpleRelationship(GA_REL_PREFIX + s);
    }
    private ComparableRelationship lrel(String rel, String props) {
        return new ComparableRelationship(GA_REL_PREFIX + rel + LITERAL +props);
    }

}
