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

import com.graphaware.neo4j.dto.common.relationship.ImmutableDirectedRelationship;
import com.graphaware.neo4j.dto.string.relationship.SerializableDirectedRelationshipImpl;
import com.graphaware.neo4j.relcount.full.dto.property.LiterallyCountableProperties;
import org.junit.Test;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import static com.graphaware.neo4j.common.Constants.GA_REL_PREFIX;
import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link }.
 */
public class LiterallyComparableRelationshipTest {

    @Test
    public void propertiesShouldBeCorrectlyConstructed() {
        CountableRelationship relationship = lrel("test#INCOMING#_LITERAL_#true#key1#value1#key2#value2");

        assertTrue(relationship.getProperties() instanceof LiterallyCountableProperties);
        assertTrue(relationship.getProperties().containsKey("key1"));
        assertTrue(relationship.getProperties().containsKey("key2"));
        assertEquals(2, relationship.getProperties().size());

        relationship = crel("test#INCOMING#key1#value1#key2#value2");

        assertFalse(relationship.getProperties() instanceof LiterallyCountableProperties);
        assertTrue(relationship.getProperties().containsKey("key1"));
        assertTrue(relationship.getProperties().containsKey("key2"));
        assertEquals(2, relationship.getProperties().size());

        relationship = lrel("test#INCOMING#_LITERAL_#true");

        assertTrue(relationship.getProperties() instanceof LiterallyCountableProperties);
        assertEquals(0, relationship.getProperties().size());
    }

    @Test
    public void shouldCorrectlyJudgeMoreGeneral() {
        assertTrue(lrel("test#INCOMING#","key1#value1#key2#value2").isMoreGeneralThan(drel("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#", "key1#value1#key2#value2").isStrictlyMoreGeneralThan(drel("test#INCOMING#key1#value1#key2#value2")));

        assertTrue(lrel("test#INCOMING#","key1#value1#key2#value2").isMoreGeneralThan(crel("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#", "key1#value1#key2#value2").isStrictlyMoreGeneralThan(crel("test#INCOMING#key1#value1#key2#value2")));

        assertFalse(lrel("test#INCOMING#", "key2#value2").isMoreGeneralThan(drel("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#", "key2#value2").isStrictlyMoreGeneralThan(drel("test#INCOMING#key1#value1#key2#value2")));

        assertFalse(lrel("test#INCOMING#", "key2#value2").isMoreGeneralThan(crel("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#", "key2#value2").isStrictlyMoreGeneralThan(crel("test#INCOMING#key1#value1#key2#value2")));

        assertFalse(lrel("test#INCOMING#","").isMoreGeneralThan(drel("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#","").isStrictlyMoreGeneralThan(drel("test#INCOMING#key1#value1#key2#value2")));

        assertFalse(lrel("test#INCOMING#","").isMoreGeneralThan(crel("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#","").isStrictlyMoreGeneralThan(crel("test#INCOMING#key1#value1#key2#value2")));

        assertFalse(lrel("test#INCOMING#","key2#value3").isMoreGeneralThan(drel("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#","key2#value3").isStrictlyMoreGeneralThan(drel("test#INCOMING#key1#value1#key2#value2")));

        assertFalse(lrel("test#INCOMING#","key2#value3").isMoreGeneralThan(crel("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#","key2#value3").isStrictlyMoreGeneralThan(crel("test#INCOMING#key1#value1#key2#value2")));

        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isMoreGeneralThan(drel("test#OUTGOING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isStrictlyMoreGeneralThan(drel("test#OUTGOING#key1#value1#key2#value2")));

        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isMoreGeneralThan(crel("test#OUTGOING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isStrictlyMoreGeneralThan(crel("test#OUTGOING#key1#value1#key2#value2")));

        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isMoreGeneralThan(drel("test2#INCOMING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isStrictlyMoreGeneralThan(drel("test2#INCOMING#key1#value1#key2#value2")));

        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isMoreGeneralThan(crel("test2#INCOMING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isStrictlyMoreGeneralThan(crel("test2#INCOMING#key1#value1#key2#value2")));
    }

    @Test
    public void shouldCorrectlyJudgeMoreSpecific() {
        assertTrue(lrel("test#INCOMING#","key1#value1#key2#value2").isMoreSpecificThan(drel("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isStrictlyMoreSpecificThan(drel("test#INCOMING#key1#value1#key2#value2")));

        assertTrue(lrel("test#INCOMING#","key1#value1#key2#value2").isMoreSpecificThan(drel("test#INCOMING#key2#value2")));
        assertTrue(lrel("test#INCOMING#","key1#value1#key2#value2").isStrictlyMoreSpecificThan(drel("test#INCOMING#key2#value2")));

        assertTrue(lrel("test#INCOMING#","key1#value1#key2#value2").isMoreSpecificThan(drel("test#INCOMING")));
        assertTrue(lrel("test#INCOMING#","key1#value1#key2#value2").isStrictlyMoreSpecificThan(drel("test#INCOMING")));

        assertFalse(lrel("test#INCOMING#","key2#value2").isMoreSpecificThan(drel("test#INCOMING#key2#value3")));
        assertFalse(lrel("test#INCOMING#","key2#value2").isStrictlyMoreSpecificThan(drel("test#INCOMING#key2#value3")));

        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isMoreSpecificThan(drel("test#OUTGOING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isStrictlyMoreSpecificThan(drel("test#OUTGOING#key1#value1#key2#value2")));

        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isMoreSpecificThan(drel("test2#INCOMING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isStrictlyMoreSpecificThan(drel("test2#INCOMING#key1#value1#key2#value2")));

        assertTrue(lrel("test#INCOMING#","key1#value1#key2#value2").isMoreSpecificThan(crel("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isStrictlyMoreSpecificThan(crel("test#INCOMING#key1#value1#key2#value2")));

        assertTrue(lrel("test#INCOMING#","key1#value1#key2#value2").isMoreSpecificThan(crel("test#INCOMING#key2#value2")));
        assertTrue(lrel("test#INCOMING#","key1#value1#key2#value2").isStrictlyMoreSpecificThan(crel("test#INCOMING#key2#value2")));

        assertTrue(lrel("test#INCOMING#","key1#value1#key2#value2").isMoreSpecificThan(crel("test#INCOMING")));
        assertTrue(lrel("test#INCOMING#","key1#value1#key2#value2").isStrictlyMoreSpecificThan(crel("test#INCOMING")));

        assertFalse(lrel("test#INCOMING#","key2#value2").isMoreSpecificThan(crel("test#INCOMING#key2#value3")));
        assertFalse(lrel("test#INCOMING#","key2#value2").isStrictlyMoreSpecificThan(crel("test#INCOMING#key2#value3")));

        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isMoreSpecificThan(crel("test#OUTGOING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isStrictlyMoreSpecificThan(crel("test#OUTGOING#key1#value1#key2#value2")));

        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isMoreSpecificThan(crel("test2#INCOMING#key1#value1#key2#value2")));
        assertFalse(lrel("test#INCOMING#","key1#value1#key2#value2").isStrictlyMoreSpecificThan(crel("test2#INCOMING#key1#value1#key2#value2")));
    }

    @Test
    public void shouldGenerateAllMoreGeneral() {
        Set<CountableRelationship> result = lrel("test#INCOMING#key1#value1#key2#value2").generateAllMoreGeneral();

        assertEquals(5, result.size());
        Iterator<CountableRelationship> iterator = result.iterator();
        assertEquals(iterator.next(), lrel("test#INCOMING#key1#value1#key2#value2"));
        assertEquals(iterator.next(), crel("test#INCOMING#key1#value1#key2#value2"));
        assertEquals(iterator.next(), crel("test#INCOMING#key1#value1"));
        assertEquals(iterator.next(), crel("test#INCOMING#key2#value2"));
        assertEquals(iterator.next(), crel("test#INCOMING"));
    }

    @Test
    public void shouldGenerateOneMoreGeneral() {
        Set<CountableRelationship> result = lrel("test#INCOMING#key1#value1#key2#value2").generateOneMoreGeneral();

        assertEquals(1, result.size());
        Iterator<CountableRelationship> iterator = result.iterator();
        assertEquals(iterator.next(), crel("test#INCOMING#key1#value1#key2#value2"));
    }

    @Test
    public void shouldAchieveSpecificToGeneralOrderingForRelationships() {
        Set<CountableRelationship> properties = new TreeSet<>();

        properties.add(lrel("test#INCOMING"));
        properties.add(lrel("test#INCOMING#key1#value1#key2#value2"));
        properties.add(lrel("test#INCOMING#key2#value2"));
        properties.add(lrel("test#INCOMING#key2#value2"));
        properties.add(lrel("test#INCOMING#key2#value2"));
        properties.add(lrel("xx#INCOMING#key2#value2"));
        properties.add(lrel("test#OUTGOING#key2#value2"));

        Iterator<CountableRelationship> iterator = properties.iterator();
        assertEquals(lrel("test#INCOMING"), iterator.next());
        assertEquals(lrel("test#INCOMING#key1#value1#key2#value2"), iterator.next());
        assertEquals(lrel("test#INCOMING#key2#value2"), iterator.next());
        assertEquals(lrel("test#OUTGOING#key2#value2"), iterator.next());
        assertEquals(lrel("xx#INCOMING#key2#value2"), iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void relationshipsShouldBehaveProperlyInTreeSets() {
        Set<CountableRelationship> properties = new TreeSet<>();

        properties.add(lrel("test#INCOMING"));
        properties.add(lrel("test#INCOMING#key1#value1#key2#value2"));
        properties.add(lrel("test#INCOMING#key2#value2"));
        properties.add(lrel("test#INCOMING#key1#value2"));
        properties.add(lrel("test#INCOMING#key2#value1"));

        assertTrue(properties.contains(lrel("test#INCOMING")));
        assertTrue(properties.contains(lrel("test#INCOMING#key1#value1#key2#value2")));
        assertTrue(properties.contains(lrel("test#INCOMING#key2#value2")));
        assertTrue(properties.contains(lrel("test#INCOMING#key1#value2")));
        assertTrue(properties.contains(lrel("test#INCOMING#key2#value1")));
        assertFalse(properties.contains(lrel("test#INCOMING#key1#value1")));
    }

    @Test
    public void shouldCorrectlyConvertToString() {
        assertEquals(GA_REL_PREFIX + "test#OUTGOING#_LITERAL_#true#key1#value1#key2#value2", new LiterallyCountableRelationship(GA_REL_PREFIX + "test#OUTGOING#_LITERAL_#true#key1#value1#key2#value2").toString());
        assertEquals(GA_REL_PREFIX + "test#INCOMING#_LITERAL_#true", new LiterallyCountableRelationship(GA_REL_PREFIX + "test#INCOMING#").toString());
        assertEquals(GA_REL_PREFIX + "test#INCOMING#_LITERAL_#true", new LiterallyCountableRelationship(GA_REL_PREFIX + "test#INCOMING#_LITERAL_#anything#").toString());
    }

    @Test
    public void sameRelationshipsShouldBeEqual() {
        assertTrue(new LiterallyCountableRelationship(GA_REL_PREFIX + "test#INCOMING#").equals(new LiterallyCountableRelationship(GA_REL_PREFIX + "test#INCOMING#_LITERAL_#true")));
        assertTrue(new LiterallyCountableRelationship(GA_REL_PREFIX + "test#OUTGOING#_LITERAL_#true#key1#value1#key2#value2").equals(new LiterallyCountableRelationship(GA_REL_PREFIX + "test#OUTGOING#_LITERAL_#true#key1#value1#key2#value2")));
        assertTrue(new LiterallyCountableRelationship(GA_REL_PREFIX + "test#OUTGOING#key1#value1#key2#value2").equals(new LiterallyCountableRelationship(GA_REL_PREFIX + "test#OUTGOING#_LITERAL_#anything#key1#value1#key2#value2#")));
        assertTrue(new LiterallyCountableRelationship(GA_REL_PREFIX + "test#INCOMING#_LITERAL_#true#key1#value1#key2#").equals(new LiterallyCountableRelationship(GA_REL_PREFIX + "test#INCOMING#_LITERAL_#true#key1#value1#key2")));
    }

    @Test
    public void differentRelationshipsShouldNotBeEqual() {
        assertFalse(new LiterallyCountableRelationship(GA_REL_PREFIX + "test#OUTGOING#_LITERAL_#true").equals(new LiterallyCountableRelationship(GA_REL_PREFIX + "test#INCOMING#_LITERAL_#true")));
        assertFalse(new LiterallyCountableRelationship(GA_REL_PREFIX + "test2#OUTGOING#_LITERAL_#true#key1#value1#key2#value2").equals(new LiterallyCountableRelationship(GA_REL_PREFIX + "test#OUTGOING#_LITERAL_#true#key1#value1#key2#value2")));
        assertFalse(new LiterallyCountableRelationship(GA_REL_PREFIX + "test#OUTGOING#_LITERAL_#true#key3#value1#key2#value2").equals(new LiterallyCountableRelationship(GA_REL_PREFIX + "test#OUTGOING#_LITERAL_#true#key1#value1#key2#value2#")));
    }

    private CountableRelationship crel(String s) {
        return new GenerallyCountableRelationship(GA_REL_PREFIX + s);
    }

    private ImmutableDirectedRelationship<String, ?> drel(String s) {
        return new SerializableDirectedRelationshipImpl(GA_REL_PREFIX + s);
    }

    private CountableRelationship lrel(String rel, String props) {
        return new LiterallyCountableRelationship(GA_REL_PREFIX + rel + "_LITERAL_#true#" +props);
    }

    private CountableRelationship lrel(String rel) {
        return new LiterallyCountableRelationship(GA_REL_PREFIX + rel);
    }

}
