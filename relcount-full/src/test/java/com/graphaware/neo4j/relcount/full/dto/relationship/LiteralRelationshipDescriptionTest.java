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

import com.graphaware.neo4j.relcount.full.dto.property.LiteralPropertiesDescription;
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
public class LiteralRelationshipDescriptionTest {

    @Test
    public void propertiesShouldBeCorrectlyConstructed() {
        RelationshipDescription relationship = lit("test#INCOMING#_LITERAL_#true#key1#value1#key2#value2");

        assertTrue(relationship.getProperties() instanceof LiteralPropertiesDescription);
        assertTrue(relationship.getProperties().containsKey("key1"));
        assertTrue(relationship.getProperties().containsKey("key2"));
        assertEquals(2, relationship.getProperties().size());

        relationship = gen("test#INCOMING#key1#value1#key2#value2");

        assertFalse(relationship.getProperties() instanceof LiteralPropertiesDescription);
        assertTrue(relationship.getProperties().containsKey("key1"));
        assertTrue(relationship.getProperties().containsKey("key2"));
        assertEquals(2, relationship.getProperties().size());

        relationship = lit("test#INCOMING#_LITERAL_#true");

        assertTrue(relationship.getProperties() instanceof LiteralPropertiesDescription);
        assertEquals(0, relationship.getProperties().size());
    }

    @Test
    public void shouldCorrectlyJudgeMoreGeneral() {
        assertTrue(lit("test#INCOMING#", "key1#value1#key2#value2").isMoreGeneralThan(gen("test#INCOMING#key1#value1#key2#value2")));
        assertTrue(lit("test#INCOMING#", "key1#value1#key2#value2").isMoreGeneralThan(lit("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(lit("test#INCOMING#", "key1#value1#key2#value2").isStrictlyMoreGeneralThan(gen("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(lit("test#INCOMING#", "key1#value1#key2#value2").isStrictlyMoreGeneralThan(lit("test#INCOMING#key1#value1#key2#value2")));

        assertFalse(lit("test#INCOMING#", "key2#value2").isMoreGeneralThan(gen("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(lit("test#INCOMING#", "key2#value2").isMoreGeneralThan(lit("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(lit("test#INCOMING#", "key2#value2").isStrictlyMoreGeneralThan(gen("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(lit("test#INCOMING#", "key2#value2").isStrictlyMoreGeneralThan(lit("test#INCOMING#key1#value1#key2#value2")));

        assertFalse(lit("test#INCOMING#", "").isMoreGeneralThan(gen("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(lit("test#INCOMING#", "").isMoreGeneralThan(lit("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(lit("test#INCOMING#", "").isStrictlyMoreGeneralThan(gen("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(lit("test#INCOMING#", "").isStrictlyMoreGeneralThan(lit("test#INCOMING#key1#value1#key2#value2")));

        assertFalse(lit("test#INCOMING#", "key2#value3").isMoreGeneralThan(gen("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(lit("test#INCOMING#", "key2#value3").isMoreGeneralThan(lit("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(lit("test#INCOMING#", "key2#value3").isStrictlyMoreGeneralThan(gen("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(lit("test#INCOMING#", "key2#value3").isStrictlyMoreGeneralThan(lit("test#INCOMING#key1#value1#key2#value2")));

        assertFalse(lit("test#INCOMING#", "key1#value1#key2#value2").isMoreGeneralThan(gen("test#OUTGOING#key1#value1#key2#value2")));
        assertFalse(lit("test#INCOMING#", "key1#value1#key2#value2").isMoreGeneralThan(lit("test#OUTGOING#key1#value1#key2#value2")));
        assertFalse(lit("test#INCOMING#", "key1#value1#key2#value2").isStrictlyMoreGeneralThan(gen("test#OUTGOING#key1#value1#key2#value2")));
        assertFalse(lit("test#INCOMING#", "key1#value1#key2#value2").isStrictlyMoreGeneralThan(lit("test#OUTGOING#key1#value1#key2#value2")));

        assertFalse(lit("test#INCOMING#", "key1#value1#key2#value2").isMoreGeneralThan(gen("test2#INCOMING#key1#value1#key2#value2")));
        assertFalse(lit("test#INCOMING#", "key1#value1#key2#value2").isMoreGeneralThan(lit("test2#INCOMING#key1#value1#key2#value2")));
        assertFalse(lit("test#INCOMING#", "key1#value1#key2#value2").isStrictlyMoreGeneralThan(gen("test2#INCOMING#key1#value1#key2#value2")));
        assertFalse(lit("test#INCOMING#", "key1#value1#key2#value2").isStrictlyMoreGeneralThan(lit("test2#INCOMING#key1#value1#key2#value2")));
    }

    @Test
    public void shouldCorrectlyJudgeMoreSpecific() {
        assertTrue(lit("test#INCOMING#", "key1#value1#key2#value2").isMoreSpecificThan(gen("test#INCOMING#key1#value1#key2#value2")));
        assertTrue(lit("test#INCOMING#", "key1#value1#key2#value2").isMoreSpecificThan(lit("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(lit("test#INCOMING#", "key1#value1#key2#value2").isStrictlyMoreSpecificThan(gen("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(lit("test#INCOMING#", "key1#value1#key2#value2").isStrictlyMoreSpecificThan(lit("test#INCOMING#key1#value1#key2#value2")));

        assertTrue(lit("test#INCOMING#", "key1#value1#key2#value2").isMoreSpecificThan(gen("test#INCOMING#key2#value2")));
        assertFalse(lit("test#INCOMING#", "key1#value1#key2#value2").isMoreSpecificThan(lit("test#INCOMING#key2#value2")));
        assertTrue(lit("test#INCOMING#", "key1#value1#key2#value2").isStrictlyMoreSpecificThan(gen("test#INCOMING#key2#value2")));
        assertFalse(lit("test#INCOMING#", "key1#value1#key2#value2").isStrictlyMoreSpecificThan(lit("test#INCOMING#key2#value2")));

        assertTrue(lit("test#INCOMING#", "key1#value1#key2#value2").isMoreSpecificThan(gen("test#INCOMING")));
        assertFalse(lit("test#INCOMING#", "key1#value1#key2#value2").isMoreSpecificThan(lit("test#INCOMING")));
        assertTrue(lit("test#INCOMING#", "key1#value1#key2#value2").isStrictlyMoreSpecificThan(gen("test#INCOMING")));
        assertFalse(lit("test#INCOMING#", "key1#value1#key2#value2").isStrictlyMoreSpecificThan(lit("test#INCOMING")));

        assertFalse(lit("test#INCOMING#", "key2#value2").isMoreSpecificThan(gen("test#INCOMING#key2#value3")));
        assertFalse(lit("test#INCOMING#", "key2#value2").isMoreSpecificThan(lit("test#INCOMING#key2#value3")));
        assertFalse(lit("test#INCOMING#", "key2#value2").isStrictlyMoreSpecificThan(gen("test#INCOMING#key2#value3")));
        assertFalse(lit("test#INCOMING#", "key2#value2").isStrictlyMoreSpecificThan(lit("test#INCOMING#key2#value3")));

        assertFalse(lit("test#INCOMING#", "key1#value1#key2#value2").isMoreSpecificThan(gen("test#OUTGOING#key1#value1#key2#value2")));
        assertFalse(lit("test#INCOMING#", "key1#value1#key2#value2").isMoreSpecificThan(lit("test#OUTGOING#key1#value1#key2#value2")));
        assertFalse(lit("test#INCOMING#", "key1#value1#key2#value2").isStrictlyMoreSpecificThan(gen("test#OUTGOING#key1#value1#key2#value2")));
        assertFalse(lit("test#INCOMING#", "key1#value1#key2#value2").isStrictlyMoreSpecificThan(lit("test#OUTGOING#key1#value1#key2#value2")));

        assertFalse(lit("test#INCOMING#", "key1#value1#key2#value2").isMoreSpecificThan(gen("test2#INCOMING#key1#value1#key2#value2")));
        assertFalse(lit("test#INCOMING#", "key1#value1#key2#value2").isMoreSpecificThan(lit("test2#INCOMING#key1#value1#key2#value2")));
        assertFalse(lit("test#INCOMING#", "key1#value1#key2#value2").isStrictlyMoreSpecificThan(gen("test2#INCOMING#key1#value1#key2#value2")));
        assertFalse(lit("test#INCOMING#", "key1#value1#key2#value2").isStrictlyMoreSpecificThan(lit("test2#INCOMING#key1#value1#key2#value2")));
    }

    @Test
    public void shouldGenerateAllMoreGeneral() {
        Set<RelationshipDescription> result = lit("test#INCOMING#key1#value1#key2#value2").generateAllMoreGeneral();

        assertEquals(5, result.size());
        Iterator<RelationshipDescription> iterator = result.iterator();
        assertEquals(iterator.next(), lit("test#INCOMING#key1#value1#key2#value2"));
        assertEquals(iterator.next(), gen("test#INCOMING#key1#value1#key2#value2"));
        assertEquals(iterator.next(), gen("test#INCOMING#key1#value1"));
        assertEquals(iterator.next(), gen("test#INCOMING#key2#value2"));
        assertEquals(iterator.next(), gen("test#INCOMING"));
    }

    @Test
    public void shouldGenerateOneMoreGeneral() {
        Set<RelationshipDescription> result = lit("test#INCOMING#key1#value1#key2#value2").generateOneMoreGeneral();

        assertEquals(1, result.size());
        Iterator<RelationshipDescription> iterator = result.iterator();
        assertEquals(iterator.next(), gen("test#INCOMING#key1#value1#key2#value2"));
    }

    @Test
    public void shouldAchieveSpecificToGeneralOrderingForRelationships() {
        Set<RelationshipDescription> properties = new TreeSet<>();

        properties.add(lit("test#INCOMING"));
        properties.add(lit("test#INCOMING#key1#value1#key2#value2"));
        properties.add(lit("test#INCOMING#key2#value2"));
        properties.add(lit("test#INCOMING#key2#value2"));
        properties.add(lit("test#INCOMING#key2#value2"));
        properties.add(lit("xx#INCOMING#key2#value2"));
        properties.add(lit("test#OUTGOING#key2#value2"));

        Iterator<RelationshipDescription> iterator = properties.iterator();
        assertEquals(lit("test#INCOMING"), iterator.next());
        assertEquals(lit("test#INCOMING#key1#value1#key2#value2"), iterator.next());
        assertEquals(lit("test#INCOMING#key2#value2"), iterator.next());
        assertEquals(lit("test#OUTGOING#key2#value2"), iterator.next());
        assertEquals(lit("xx#INCOMING#key2#value2"), iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void relationshipsShouldBehaveProperlyInTreeSets() {
        Set<RelationshipDescription> properties = new TreeSet<>();

        properties.add(lit("test#INCOMING"));
        properties.add(lit("test#INCOMING#key1#value1#key2#value2"));
        properties.add(lit("test#INCOMING#key2#value2"));
        properties.add(lit("test#INCOMING#key1#value2"));
        properties.add(lit("test#INCOMING#key2#value1"));

        assertTrue(properties.contains(lit("test#INCOMING")));
        assertTrue(properties.contains(lit("test#INCOMING#key1#value1#key2#value2")));
        assertTrue(properties.contains(lit("test#INCOMING#key2#value2")));
        assertTrue(properties.contains(lit("test#INCOMING#key1#value2")));
        assertTrue(properties.contains(lit("test#INCOMING#key2#value1")));
        assertFalse(properties.contains(lit("test#INCOMING#key1#value1")));
    }

    @Test
    public void shouldCorrectlyConvertToString() {
        assertEquals("_PRE_" + "test#OUTGOING#_LITERAL_#true#key1#value1#key2#value2", new LiteralRelationshipDescription("_PRE_test#OUTGOING#_LITERAL_#true#key1#value1#key2#value2", "_PRE_").toString("_PRE_"));
        assertEquals("test#INCOMING#_LITERAL_#true", new LiteralRelationshipDescription("_PRE_test#INCOMING#", "_PRE_").toString());
        assertEquals("test#INCOMING#_LITERAL_#true", new LiteralRelationshipDescription("test#INCOMING#_LITERAL_#anything#", "").toString());
    }

    @Test
    public void sameRelationshipsShouldBeEqual() {
        assertTrue(new LiteralRelationshipDescription("test#INCOMING#", "").equals(new LiteralRelationshipDescription("_PRE_test#INCOMING#_LITERAL_#true", "_PRE_")));
        assertTrue(new LiteralRelationshipDescription("test#OUTGOING#_LITERAL_#true#key1#value1#key2#value2", "").equals(new LiteralRelationshipDescription("test#OUTGOING#_LITERAL_#true#key1#value1#key2#value2", null)));
        assertTrue(new LiteralRelationshipDescription("test#OUTGOING#key1#value1#key2#value2", "").equals(new LiteralRelationshipDescription("test#OUTGOING#_LITERAL_#anything#key1#value1#key2#value2#", null)));
        assertTrue(new LiteralRelationshipDescription("test#INCOMING#_LITERAL_#true#key1#value1#key2#", "").equals(new LiteralRelationshipDescription("test#INCOMING#_LITERAL_#true#key1#value1#key2", null)));
    }

    @Test
    public void differentRelationshipsShouldNotBeEqual() {
        assertFalse(new LiteralRelationshipDescription("test#OUTGOING#_LITERAL_#true", null).equals(new LiteralRelationshipDescription("test#INCOMING#_LITERAL_#true", null)));
        assertFalse(new LiteralRelationshipDescription("test2#OUTGOING#_LITERAL_#true#key1#value1#key2#value2", null).equals(new LiteralRelationshipDescription("test#OUTGOING#_LITERAL_#true#key1#value1#key2#value2", null)));
        assertFalse(new LiteralRelationshipDescription("test#OUTGOING#_LITERAL_#true#key3#value1#key2#value2", null).equals(new LiteralRelationshipDescription("test#OUTGOING#_LITERAL_#true#key1#value1#key2#value2#", null)));
    }

    private RelationshipDescription gen(String s) {
        return new GeneralRelationshipDescription(s, null);
    }

    private RelationshipDescription lit(String rel, String props) {
        return new LiteralRelationshipDescription(rel + "_LITERAL_#true#" + props, null);
    }

    private RelationshipDescription lit(String rel) {
        return new LiteralRelationshipDescription("_PRE_" + rel, "_PRE_");
    }

}
