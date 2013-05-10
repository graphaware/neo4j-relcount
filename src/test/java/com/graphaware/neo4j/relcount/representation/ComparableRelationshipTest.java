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

import com.graphaware.neo4j.relcount.representation.ComparableRelationship;
import com.graphaware.neo4j.representation.relationship.Relationship;
import com.graphaware.neo4j.representation.relationship.SimpleRelationship;
import org.junit.Test;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import static com.graphaware.neo4j.utils.Constants.GA_REL_PREFIX;
import static junit.framework.Assert.*;

/**
 * Unit test for {@link com.graphaware.neo4j.relcount.representation.ComparableRelationship}.
 */
public class ComparableRelationshipTest {

    @Test
    public void shouldCorrectlyJudgeMoreGeneral() {
        assertTrue(rel("test#INCOMING#key1#value1#key2#value2").isMoreGeneralThan(rel2("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(rel("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreGeneralThan(rel2("test#INCOMING#key1#value1#key2#value2")));

        assertTrue(rel("test#INCOMING#key2#value2").isMoreGeneralThan(rel2("test#INCOMING#key1#value1#key2#value2")));
        assertTrue(rel("test#INCOMING#key2#value2").isStrictlyMoreGeneralThan(rel2("test#INCOMING#key1#value1#key2#value2")));

        assertTrue(rel("test#INCOMING").isMoreGeneralThan(rel("test#INCOMING#key1#value1#key2#value2")));
        assertTrue(rel("test#INCOMING").isStrictlyMoreGeneralThan(rel("test#INCOMING#key1#value1#key2#value2")));

        assertFalse(rel("test#INCOMING#key2#value3").isMoreGeneralThan(rel2("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(rel("test#INCOMING#key2#value3").isStrictlyMoreGeneralThan(rel2("test#INCOMING#key1#value1#key2#value2")));

        assertFalse(rel("test#INCOMING#key1#value1#key2#value2").isMoreGeneralThan(rel2("test#OUTGOING#key1#value1#key2#value2")));
        assertFalse(rel("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreGeneralThan(rel2("test#OUTGOING#key1#value1#key2#value2")));

        assertFalse(rel("test#INCOMING#key1#value1#key2#value2").isMoreGeneralThan(rel2("test2#INCOMING#key1#value1#key2#value2")));
        assertFalse(rel("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreGeneralThan(rel2("test2#INCOMING#key1#value1#key2#value2")));
    }

    @Test
    public void shouldCorrectlyJudgeMoreSpecific() {
        assertTrue(rel("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(rel2("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(rel("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreSpecificThan(rel2("test#INCOMING#key1#value1#key2#value2")));

        assertTrue(rel("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(rel2("test#INCOMING#key2#value2")));
        assertTrue(rel("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreSpecificThan(rel2("test#INCOMING#key2#value2")));

        assertTrue(rel("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(rel("test#INCOMING")));
        assertTrue(rel("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreSpecificThan(rel("test#INCOMING")));

        assertFalse(rel("test#INCOMING#key2#value2").isMoreSpecificThan(rel2("test#INCOMING#key2#value3")));
        assertFalse(rel("test#INCOMING#key2#value2").isStrictlyMoreSpecificThan(rel2("test#INCOMING#key2#value3")));

        assertFalse(rel("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(rel2("test#OUTGOING#key1#value1#key2#value2")));
        assertFalse(rel("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreSpecificThan(rel2("test#OUTGOING#key1#value1#key2#value2")));

        assertFalse(rel("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(rel2("test2#INCOMING#key1#value1#key2#value2")));
        assertFalse(rel("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreSpecificThan(rel2("test2#INCOMING#key1#value1#key2#value2")));
    }

    @Test
    public void shouldGenerateAllMoreGeneral() {
        Set<ComparableRelationship> result = rel("test#INCOMING#key1#value1#key2#value2").generateAllMoreGeneral();

        assertEquals(4, result.size());
        Iterator<ComparableRelationship> iterator = result.iterator();
        assertEquals(iterator.next(), rel("test#INCOMING#key1#value1#key2#value2"));
        assertEquals(iterator.next(), rel("test#INCOMING#key1#value1"));
        assertEquals(iterator.next(), rel("test#INCOMING#key2#value2"));
        assertEquals(iterator.next(), rel("test#INCOMING"));
    }

    @Test
    public void shouldGenerateOneMoreGeneral() {
        Set<ComparableRelationship> result = rel("test#INCOMING#key1#value1#key2#value2").generateOneMoreGeneral();

        assertEquals(3, result.size());
        Iterator<ComparableRelationship> iterator = result.iterator();
        assertEquals(iterator.next(), rel("test#INCOMING#key1#value1#key2#value2"));
        assertEquals(iterator.next(), rel("test#INCOMING#key1#value1"));
        assertEquals(iterator.next(), rel("test#INCOMING#key2#value2"));
    }

    @Test
    public void shouldAchieveSpecificToGeneralOrderingForRelationships() {
        Set<ComparableRelationship> properties = new TreeSet<ComparableRelationship>();

        properties.add(rel("test#INCOMING"));
        properties.add(rel("test#INCOMING#key1#value1#key2#value2"));
        properties.add(rel("test#INCOMING#key2#value2"));
        properties.add(rel("test#INCOMING#key2#value2"));
        properties.add(rel("test#INCOMING#key2#value2"));
        properties.add(rel("xx#INCOMING#key2#value2"));
        properties.add(rel("test#OUTGOING#key2#value2"));

        Iterator<ComparableRelationship> iterator = properties.iterator();
        assertEquals(rel("test#INCOMING#key1#value1#key2#value2"), iterator.next());
        assertEquals(rel("test#INCOMING#key2#value2"), iterator.next());
        assertEquals(rel("test#INCOMING"), iterator.next());
        assertEquals(rel("test#OUTGOING#key2#value2"), iterator.next());
        assertEquals(rel("xx#INCOMING#key2#value2"), iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void relationshipsShouldBehaveProperlyInTreeSets() {
        Set<ComparableRelationship> properties = new TreeSet<ComparableRelationship>();

        properties.add(rel("test#INCOMING"));
        properties.add(rel("test#INCOMING#key1#value1#key2#value2"));
        properties.add(rel("test#INCOMING#key2#value2"));
        properties.add(rel("test#INCOMING#key1#value2"));
        properties.add(rel("test#INCOMING#key2#value1"));

        assertTrue(properties.contains(rel("test#INCOMING")));
        assertTrue(properties.contains(rel("test#INCOMING#key1#value1#key2#value2")));
        assertTrue(properties.contains(rel("test#INCOMING#key2#value2")));
        assertTrue(properties.contains(rel("test#INCOMING#key1#value2")));
        assertTrue(properties.contains(rel("test#INCOMING#key2#value1")));
        assertFalse(properties.contains(rel("test#INCOMING#key1#value1")));
    }

    /**
     * just for readability
     */
    private ComparableRelationship rel(String s) {
        return new ComparableRelationship(GA_REL_PREFIX + s);
    }

    private Relationship rel2(String s) {
        return new SimpleRelationship(GA_REL_PREFIX + s);
    }
}
