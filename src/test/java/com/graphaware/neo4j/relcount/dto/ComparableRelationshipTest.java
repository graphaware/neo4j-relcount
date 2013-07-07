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

package com.graphaware.neo4j.relcount.dto;

import com.graphaware.neo4j.dto.relationship.immutable.DirectedRelationship;
import com.graphaware.neo4j.dto.relationship.immutable.SerializableDirectedRelationship;
import com.graphaware.neo4j.relcount.full.dto.ComparableRelationship;
import com.graphaware.neo4j.relcount.full.dto.LiteralComparableProperties;
import org.junit.Test;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import static com.graphaware.neo4j.common.Constants.GA_REL_PREFIX;
import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link com.graphaware.neo4j.relcount.full.dto.ComparableRelationship}.
 */
public class ComparableRelationshipTest {

    @Test
    public void propertiesShouldBeCorrectlyConstructed() {
        ComparableRelationship relationship = crel("test#INCOMING#_LITERAL_#true#key1#value1#key2#value2");

        assertTrue(relationship.getProperties() instanceof LiteralComparableProperties);
        assertTrue(relationship.getProperties().containsKey("key1"));
        assertTrue(relationship.getProperties().containsKey("key2"));
        assertEquals(2, relationship.getProperties().size());

        relationship = crel("test#INCOMING#key1#value1#key2#value2");

        assertFalse(relationship.getProperties() instanceof LiteralComparableProperties);
        assertTrue(relationship.getProperties().containsKey("key1"));
        assertTrue(relationship.getProperties().containsKey("key2"));
        assertEquals(2, relationship.getProperties().size());

        relationship = crel("test#INCOMING#_LITERAL_#true");

        assertTrue(relationship.getProperties() instanceof LiteralComparableProperties);
        assertEquals(0, relationship.getProperties().size());
    }

    @Test
    public void shouldCorrectlyJudgeMoreGeneral() {
        assertTrue(crel("test#INCOMING#key1#value1#key2#value2").isMoreGeneralThan(drel("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(crel("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreGeneralThan(drel("test#INCOMING#key1#value1#key2#value2")));

        assertTrue(lrel("test#INCOMING#","key1#value1#key2#value2").isMoreGeneralThan(drel("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#", "key1#value1#key2#value2").isStrictlyMoreGeneralThan(drel("test#INCOMING#key1#value1#key2#value2")));

        assertTrue(crel("test#INCOMING#key2#value2").isMoreGeneralThan(drel("test#INCOMING#key1#value1#key2#value2")));
        assertTrue(crel("test#INCOMING#key2#value2").isStrictlyMoreGeneralThan(drel("test#INCOMING#key1#value1#key2#value2")));

        assertFalse(lrel("test#INCOMING#", "key2#value2").isMoreGeneralThan(drel("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#", "key2#value2").isStrictlyMoreGeneralThan(drel("test#INCOMING#key1#value1#key2#value2")));

        assertTrue(crel("test#INCOMING").isMoreGeneralThan(crel("test#INCOMING#key1#value1#key2#value2")));
        assertTrue(crel("test#INCOMING").isStrictlyMoreGeneralThan(crel("test#INCOMING#key1#value1#key2#value2")));

        assertFalse(lrel("test#INCOMING#","").isMoreGeneralThan(crel("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#","").isStrictlyMoreGeneralThan(crel("test#INCOMING#key1#value1#key2#value2")));

        assertFalse(crel("test#INCOMING#key2#value3").isMoreGeneralThan(drel("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(crel("test#INCOMING#key2#value3").isStrictlyMoreGeneralThan(drel("test#INCOMING#key1#value1#key2#value2")));

        assertFalse(lrel("test#INCOMING#","key2#value3").isMoreGeneralThan(drel("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#","key2#value3").isStrictlyMoreGeneralThan(drel("test#INCOMING#key1#value1#key2#value2")));

        assertFalse(crel("test#INCOMING#key1#value1#key2#value2").isMoreGeneralThan(drel("test#OUTGOING#key1#value1#key2#value2")));
        assertFalse(crel("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreGeneralThan(drel("test#OUTGOING#key1#value1#key2#value2")));

        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isMoreGeneralThan(drel("test#OUTGOING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isStrictlyMoreGeneralThan(drel("test#OUTGOING#key1#value1#key2#value2")));

        assertFalse(crel("test#INCOMING#key1#value1#key2#value2").isMoreGeneralThan(drel("test2#INCOMING#key1#value1#key2#value2")));
        assertFalse(crel("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreGeneralThan(drel("test2#INCOMING#key1#value1#key2#value2")));

        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isMoreGeneralThan(drel("test2#INCOMING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isStrictlyMoreGeneralThan(drel("test2#INCOMING#key1#value1#key2#value2")));
    }

    @Test
    public void shouldCorrectlyJudgeMoreSpecific() {
        assertTrue(crel("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(drel("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(crel("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreSpecificThan(drel("test#INCOMING#key1#value1#key2#value2")));

        assertTrue(lrel("test#INCOMING#","key1#value1#key2#value2").isMoreSpecificThan(drel("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isStrictlyMoreSpecificThan(drel("test#INCOMING#key1#value1#key2#value2")));

        assertTrue(crel("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(drel("test#INCOMING#key2#value2")));
        assertTrue(crel("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreSpecificThan(drel("test#INCOMING#key2#value2")));

        assertTrue(lrel("test#INCOMING#","key1#value1#key2#value2").isMoreSpecificThan(drel("test#INCOMING#key2#value2")));
        assertTrue(lrel("test#INCOMING#","key1#value1#key2#value2").isStrictlyMoreSpecificThan(drel("test#INCOMING#key2#value2")));

        assertTrue(crel("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(crel("test#INCOMING")));
        assertTrue(crel("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreSpecificThan(crel("test#INCOMING")));

        assertTrue(lrel("test#INCOMING#","key1#value1#key2#value2").isMoreSpecificThan(crel("test#INCOMING")));
        assertTrue(lrel("test#INCOMING#","key1#value1#key2#value2").isStrictlyMoreSpecificThan(crel("test#INCOMING")));

        assertFalse(crel("test#INCOMING#key2#value2").isMoreSpecificThan(drel("test#INCOMING#key2#value3")));
        assertFalse(crel("test#INCOMING#key2#value2").isStrictlyMoreSpecificThan(drel("test#INCOMING#key2#value3")));

        assertFalse(lrel("test#INCOMING#","key2#value2").isMoreSpecificThan(drel("test#INCOMING#key2#value3")));
        assertFalse(lrel("test#INCOMING#","key2#value2").isStrictlyMoreSpecificThan(drel("test#INCOMING#key2#value3")));

        assertFalse(crel("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(drel("test#OUTGOING#key1#value1#key2#value2")));
        assertFalse(crel("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreSpecificThan(drel("test#OUTGOING#key1#value1#key2#value2")));

        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isMoreSpecificThan(drel("test#OUTGOING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isStrictlyMoreSpecificThan(drel("test#OUTGOING#key1#value1#key2#value2")));

        assertFalse(crel("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(drel("test2#INCOMING#key1#value1#key2#value2")));
        assertFalse(crel("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreSpecificThan(drel("test2#INCOMING#key1#value1#key2#value2")));

        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isMoreSpecificThan(drel("test2#INCOMING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isStrictlyMoreSpecificThan(drel("test2#INCOMING#key1#value1#key2#value2")));
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

    @Test
    public void shouldCorrectlyConvertToString() {
        assertEquals(GA_REL_PREFIX + "test#OUTGOING#_LITERAL_#true#key1#value1#key2#value2", new ComparableRelationship(GA_REL_PREFIX + "test#OUTGOING#_LITERAL_#true#key1#value1#key2#value2").toString());
        assertEquals(GA_REL_PREFIX + "test#INCOMING#_LITERAL_#true", new ComparableRelationship(GA_REL_PREFIX + "test#INCOMING#_LITERAL_#true").toString());
        assertEquals(GA_REL_PREFIX + "test#INCOMING#_LITERAL_#true", new ComparableRelationship(GA_REL_PREFIX + "test#INCOMING#_LITERAL_#anything#").toString());
    }

    @Test
    public void sameRelationshipsShouldBeEqual() {
        assertTrue(new ComparableRelationship(GA_REL_PREFIX + "test#INCOMING#_LITERAL_#true").equals(new ComparableRelationship(GA_REL_PREFIX + "test#INCOMING#_LITERAL_#true")));
        assertTrue(new ComparableRelationship(GA_REL_PREFIX + "test#OUTGOING#_LITERAL_#true#key1#value1#key2#value2").equals(new ComparableRelationship(GA_REL_PREFIX + "test#OUTGOING#_LITERAL_#true#key1#value1#key2#value2")));
        assertTrue(new ComparableRelationship(GA_REL_PREFIX + "test#OUTGOING#_LITERAL_#true#key1#value1#key2#value2").equals(new ComparableRelationship(GA_REL_PREFIX + "test#OUTGOING#_LITERAL_#anything#key1#value1#key2#value2#")));
        assertTrue(new ComparableRelationship(GA_REL_PREFIX + "test#INCOMING#_LITERAL_#true#key1#value1#key2#").equals(new ComparableRelationship(GA_REL_PREFIX + "test#INCOMING#_LITERAL_#true#key1#value1#key2")));
    }

    @Test
    public void differentRelationshipsShouldNotBeEqual() {
        assertFalse(new ComparableRelationship(GA_REL_PREFIX + "test#OUTGOING#_LITERAL_#true").equals(new ComparableRelationship(GA_REL_PREFIX + "test#INCOMING#_LITERAL_#true")));
        assertFalse(new ComparableRelationship(GA_REL_PREFIX + "test2#OUTGOING#_LITERAL_#true#key1#value1#key2#value2").equals(new ComparableRelationship(GA_REL_PREFIX + "test#OUTGOING#_LITERAL_#true#key1#value1#key2#value2")));
        assertFalse(new ComparableRelationship(GA_REL_PREFIX + "test#OUTGOING#_LITERAL_#true#key3#value1#key2#value2").equals(new ComparableRelationship(GA_REL_PREFIX + "test#OUTGOING#_LITERAL_#true#key1#value1#key2#value2#")));
    }

    private ComparableRelationship crel(String s) {
        return new ComparableRelationship(GA_REL_PREFIX + s);
    }

    private DirectedRelationship drel(String s) {
        return new SerializableDirectedRelationship(GA_REL_PREFIX + s);
    }

    private ComparableRelationship lrel(String rel, String props) {
        return new ComparableRelationship(GA_REL_PREFIX + rel + "_LITERAL_#true#" +props);
    }

}
