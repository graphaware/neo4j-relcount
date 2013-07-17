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

import com.graphaware.neo4j.relcount.full.dto.property.GeneralPropertiesDescription;
import org.junit.Test;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link }.
 */
public class GeneralRelationshipDescriptionTest {

    @Test
    public void relationshipShouldBeCorrectlyConstructed() {
        RelationshipDescription relationship = gen("test#INCOMING#_LITERAL_#true#key1#value1#key2#value2");

        assertTrue(relationship.getProperties() instanceof GeneralPropertiesDescription);
        assertTrue(relationship.getProperties().containsKey("_LITERAL_"));
        assertTrue(relationship.getProperties().containsKey("key1"));
        assertTrue(relationship.getProperties().containsKey("key2"));
        assertEquals(3, relationship.getProperties().size());

        relationship = gen("test#INCOMING#key1#value1#key2#value2");

        assertTrue(relationship.getProperties().containsKey("key1"));
        assertTrue(relationship.getProperties().containsKey("key2"));
        assertEquals(2, relationship.getProperties().size());
    }

    @Test
    public void shouldCorrectlyJudgeMoreGeneral() {
        assertTrue(gen("test#INCOMING#key1#value1#key2#value2").isMoreGeneralThan(gen("test#INCOMING#key1#value1#key2#value2")));
        assertTrue(gen("test#INCOMING#key1#value1#key2#value2").isMoreGeneralThan(lit("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(gen("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreGeneralThan(gen("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(gen("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreGeneralThan(lit("test#INCOMING#key1#value1#key2#value2")));

        assertTrue(gen("test#INCOMING#key2#value2").isMoreGeneralThan(gen("test#INCOMING#key1#value1#key2#value2")));
        assertTrue(gen("test#INCOMING#key2#value2").isMoreGeneralThan(lit("test#INCOMING#key1#value1#key2#value2")));
        assertTrue(gen("test#INCOMING#key2#value2").isStrictlyMoreGeneralThan(gen("test#INCOMING#key1#value1#key2#value2")));
        assertTrue(gen("test#INCOMING#key2#value2").isStrictlyMoreGeneralThan(lit("test#INCOMING#key1#value1#key2#value2")));

        assertTrue(gen("test#INCOMING").isMoreGeneralThan(gen("test#INCOMING#key1#value1#key2#value2")));
        assertTrue(gen("test#INCOMING").isMoreGeneralThan(lit("test#INCOMING#key1#value1#key2#value2")));
        assertTrue(gen("test#INCOMING").isStrictlyMoreGeneralThan(gen("test#INCOMING#key1#value1#key2#value2")));
        assertTrue(gen("test#INCOMING").isStrictlyMoreGeneralThan(lit("test#INCOMING#key1#value1#key2#value2")));

        assertFalse(gen("test#INCOMING#key2#value3").isMoreGeneralThan(gen("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(gen("test#INCOMING#key2#value3").isMoreGeneralThan(lit("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(gen("test#INCOMING#key2#value3").isStrictlyMoreGeneralThan(gen("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(gen("test#INCOMING#key2#value3").isStrictlyMoreGeneralThan(lit("test#INCOMING#key1#value1#key2#value2")));

        assertFalse(gen("test#INCOMING#key1#value1#key2#value2").isMoreGeneralThan(gen("test#OUTGOING#key1#value1#key2#value2")));
        assertFalse(gen("test#INCOMING#key1#value1#key2#value2").isMoreGeneralThan(lit("test#OUTGOING#key1#value1#key2#value2")));
        assertFalse(gen("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreGeneralThan(gen("test#OUTGOING#key1#value1#key2#value2")));
        assertFalse(gen("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreGeneralThan(lit("test#OUTGOING#key1#value1#key2#value2")));

        assertFalse(gen("test#INCOMING#key1#value1#key2#value2").isMoreGeneralThan(gen("test2#INCOMING#key1#value1#key2#value2")));
        assertFalse(gen("test#INCOMING#key1#value1#key2#value2").isMoreGeneralThan(lit("test2#INCOMING#key1#value1#key2#value2")));
        assertFalse(gen("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreGeneralThan(gen("test2#INCOMING#key1#value1#key2#value2")));
        assertFalse(gen("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreGeneralThan(lit("test2#INCOMING#key1#value1#key2#value2")));
    }

    @Test
    public void shouldCorrectlyJudgeMoreSpecific() {
        assertTrue(gen("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(gen("test#INCOMING#key1#value1#key2#value2")));
        assertTrue(gen("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(lit("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(gen("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreSpecificThan(gen("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(gen("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreSpecificThan(lit("test#INCOMING#key1#value1#key2#value2")));

        assertTrue(gen("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(gen("test#INCOMING#key2#value2")));
        assertFalse(gen("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(lit("test#INCOMING#key2#value2")));
        assertTrue(gen("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreSpecificThan(gen("test#INCOMING#key2#value2")));
        assertFalse(gen("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreSpecificThan(lit("test#INCOMING#key2#value2")));

        assertTrue(gen("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(gen("test#INCOMING")));
        assertFalse(gen("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(lit("test#INCOMING")));
        assertTrue(gen("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreSpecificThan(gen("test#INCOMING")));
        assertFalse(gen("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreSpecificThan(lit("test#INCOMING")));

        assertFalse(gen("test#INCOMING#key2#value2").isMoreSpecificThan(gen("test#INCOMING#key2#value3")));
        assertFalse(gen("test#INCOMING#key2#value2").isMoreSpecificThan(lit("test#INCOMING#key2#value3")));
        assertFalse(gen("test#INCOMING#key2#value2").isStrictlyMoreSpecificThan(gen("test#INCOMING#key2#value3")));
        assertFalse(gen("test#INCOMING#key2#value2").isStrictlyMoreSpecificThan(lit("test#INCOMING#key2#value3")));

        assertFalse(gen("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(gen("test#OUTGOING#key1#value1#key2#value2")));
        assertFalse(gen("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(lit("test#OUTGOING#key1#value1#key2#value2")));
        assertFalse(gen("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreSpecificThan(gen("test#OUTGOING#key1#value1#key2#value2")));
        assertFalse(gen("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreSpecificThan(lit("test#OUTGOING#key1#value1#key2#value2")));

        assertFalse(gen("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(gen("test2#INCOMING#key1#value1#key2#value2")));
        assertFalse(gen("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(lit("test2#INCOMING#key1#value1#key2#value2")));
        assertFalse(gen("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreSpecificThan(gen("test2#INCOMING#key1#value1#key2#value2")));
        assertFalse(gen("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreSpecificThan(lit("test2#INCOMING#key1#value1#key2#value2")));
    }

    @Test
    public void shouldGenerateAllMoreGeneral() {
        Set<RelationshipDescription> result = gen("test#INCOMING#key1#value1#key2#value2").generateAllMoreGeneral();

        assertEquals(4, result.size());
        Iterator<RelationshipDescription> iterator = result.iterator();
        assertEquals(iterator.next(), gen("test#INCOMING#key1#value1#key2#value2"));
        assertEquals(iterator.next(), gen("test#INCOMING#key1#value1"));
        assertEquals(iterator.next(), gen("test#INCOMING#key2#value2"));
        assertEquals(iterator.next(), gen("test#INCOMING"));
    }

    @Test
    public void shouldGenerateOneMoreGeneral() {
        Set<RelationshipDescription> result = gen("test#INCOMING#key1#value1#key2#value2").generateOneMoreGeneral();

        assertEquals(3, result.size());
        Iterator<RelationshipDescription> iterator = result.iterator();
        assertEquals(iterator.next(), gen("test#INCOMING#key1#value1#key2#value2"));
        assertEquals(iterator.next(), gen("test#INCOMING#key1#value1"));
        assertEquals(iterator.next(), gen("test#INCOMING#key2#value2"));
    }

    @Test
    public void shouldAchieveSpecificToGeneralOrderingForRelationships() {
        Set<RelationshipDescription> properties = new TreeSet<>();

        properties.add(gen("test#INCOMING"));
        properties.add(gen("test#INCOMING#key1#value1#key2#value2"));
        properties.add(gen("test#INCOMING#key2#value2"));
        properties.add(gen("test#INCOMING#key2#value2"));
        properties.add(gen("test#INCOMING#key2#value2"));
        properties.add(gen("xx#INCOMING#key2#value2"));
        properties.add(gen("test#OUTGOING#key2#value2"));

        Iterator<RelationshipDescription> iterator = properties.iterator();
        assertEquals(gen("test#INCOMING#key1#value1#key2#value2"), iterator.next());
        assertEquals(gen("test#INCOMING#key2#value2"), iterator.next());
        assertEquals(gen("test#INCOMING"), iterator.next());
        assertEquals(gen("test#OUTGOING#key2#value2"), iterator.next());
        assertEquals(gen("xx#INCOMING#key2#value2"), iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void relationshipsShouldBehaveProperlyInTreeSets() {
        Set<RelationshipDescription> properties = new TreeSet<>();

        properties.add(gen("test#INCOMING"));
        properties.add(gen("test#INCOMING#key1#value1#key2#value2"));
        properties.add(gen("test#INCOMING#key2#value2"));
        properties.add(gen("test#INCOMING#key1#value2"));
        properties.add(gen("test#INCOMING#key2#value1"));

        assertTrue(properties.contains(gen("test#INCOMING")));
        assertTrue(properties.contains(gen("test#INCOMING#key1#value1#key2#value2")));
        assertTrue(properties.contains(gen("test#INCOMING#key2#value2")));
        assertTrue(properties.contains(gen("test#INCOMING#key1#value2")));
        assertTrue(properties.contains(gen("test#INCOMING#key2#value1")));
        assertFalse(properties.contains(gen("test#INCOMING#key1#value1")));
    }

    @Test
    public void shouldCorrectlyConvertToString() {
        assertEquals("PREFIXtest#OUTGOING#key1#value1#key2#value2", new GeneralRelationshipDescription("PREFIXtest#OUTGOING#key1#value1#key2#value2", "PREFIX").toString("PREFIX"));
        assertEquals("_PREFIX_" + "test#INCOMING", new GeneralRelationshipDescription("_PREFIX_test#INCOMING", "_PREFIX_").toString("_PREFIX_"));
        assertEquals("_PREFIX_" + "test#INCOMING", new GeneralRelationshipDescription("_PREFIX_test#INCOMING#", "_PREFIX_").toString("_PREFIX_"));
    }

    @Test
    public void sameRelationshipsShouldBeEqual() {
        assertTrue(new GeneralRelationshipDescription("PREFIX" + "test#INCOMING", "PREFIX").equals(new GeneralRelationshipDescription("bla" + "test#INCOMING", "bla")));
        assertTrue(new GeneralRelationshipDescription("test#OUTGOING#key1#value1#key2#value2", "").equals(new GeneralRelationshipDescription("test#OUTGOING#key1#value1#key2#value2", null)));
        assertTrue(new GeneralRelationshipDescription("test#INCOMING#key1#value1#key2#", "").equals(new GeneralRelationshipDescription("test#INCOMING#key1#value1#key2", null)));
    }

    @Test
    public void differentRelationshipsShouldNotBeEqual() {
        assertFalse(new GeneralRelationshipDescription("test#OUTGOING", null).equals(new GeneralRelationshipDescription("test#INCOMING", null)));
        assertFalse(new GeneralRelationshipDescription("test2#OUTGOING#key1#value1#key2#value2", null).equals(new GeneralRelationshipDescription("test#OUTGOING#key1#value1#key2#value2", null)));
        assertFalse(new GeneralRelationshipDescription("test#OUTGOING#key3#value1#key2#value2", null).equals(new GeneralRelationshipDescription("test#OUTGOING#key1#value1#key2#value2#", null)));
    }

    private RelationshipDescription gen(String s) {
        return new GeneralRelationshipDescription(s, null);
    }

    private RelationshipDescription lit(String s) {
        return new LiteralRelationshipDescription(s, null);
    }
}
