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
import com.graphaware.neo4j.relcount.full.dto.property.CandidateGeneralizedProperties;
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
public class ComparableRelationshipTest {

    @Test
    public void propertiesShouldBeCorrectlyConstructed() {
        CandidateRelationship relationship = crel("test#INCOMING#_LITERAL_#true#key1#value1#key2#value2");

        assertTrue(relationship.getProperties() instanceof CandidateGeneralizedProperties);
        assertTrue(relationship.getProperties().containsKey("_LITERAL_"));
        assertTrue(relationship.getProperties().containsKey("key1"));
        assertTrue(relationship.getProperties().containsKey("key2"));
        assertEquals(3, relationship.getProperties().size());

        relationship = crel("test#INCOMING#key1#value1#key2#value2");

        assertTrue(relationship.getProperties().containsKey("key1"));
        assertTrue(relationship.getProperties().containsKey("key2"));
        assertEquals(2, relationship.getProperties().size());
    }

    @Test
    public void shouldCorrectlyJudgeMoreGeneral() {
        assertTrue(crel("test#INCOMING#key1#value1#key2#value2").isMoreGeneralThan(drel("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(crel("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreGeneralThan(drel("test#INCOMING#key1#value1#key2#value2")));

        assertTrue(crel("test#INCOMING#key2#value2").isMoreGeneralThan(drel("test#INCOMING#key1#value1#key2#value2")));
        assertTrue(crel("test#INCOMING#key2#value2").isStrictlyMoreGeneralThan(drel("test#INCOMING#key1#value1#key2#value2")));

        assertTrue(crel("test#INCOMING").isMoreGeneralThan(crel("test#INCOMING#key1#value1#key2#value2")));
        assertTrue(crel("test#INCOMING").isStrictlyMoreGeneralThan(crel("test#INCOMING#key1#value1#key2#value2")));

        assertFalse(crel("test#INCOMING#key2#value3").isMoreGeneralThan(drel("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(crel("test#INCOMING#key2#value3").isStrictlyMoreGeneralThan(drel("test#INCOMING#key1#value1#key2#value2")));

        assertFalse(crel("test#INCOMING#key1#value1#key2#value2").isMoreGeneralThan(drel("test#OUTGOING#key1#value1#key2#value2")));
        assertFalse(crel("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreGeneralThan(drel("test#OUTGOING#key1#value1#key2#value2")));

        assertFalse(crel("test#INCOMING#key1#value1#key2#value2").isMoreGeneralThan(drel("test2#INCOMING#key1#value1#key2#value2")));
        assertFalse(crel("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreGeneralThan(drel("test2#INCOMING#key1#value1#key2#value2")));
    }

    @Test
    public void shouldCorrectlyJudgeMoreSpecific() {
        assertTrue(crel("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(drel("test#INCOMING#key1#value1#key2#value2")));
        assertFalse(crel("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreSpecificThan(drel("test#INCOMING#key1#value1#key2#value2")));

        assertTrue(crel("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(drel("test#INCOMING#key2#value2")));
        assertTrue(crel("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreSpecificThan(drel("test#INCOMING#key2#value2")));

        assertTrue(crel("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(crel("test#INCOMING")));
        assertTrue(crel("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreSpecificThan(crel("test#INCOMING")));

        assertFalse(crel("test#INCOMING#key2#value2").isMoreSpecificThan(drel("test#INCOMING#key2#value3")));
        assertFalse(crel("test#INCOMING#key2#value2").isStrictlyMoreSpecificThan(drel("test#INCOMING#key2#value3")));

        assertFalse(crel("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(drel("test#OUTGOING#key1#value1#key2#value2")));
        assertFalse(crel("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreSpecificThan(drel("test#OUTGOING#key1#value1#key2#value2")));

        assertFalse(crel("test#INCOMING#key1#value1#key2#value2").isMoreSpecificThan(drel("test2#INCOMING#key1#value1#key2#value2")));
        assertFalse(crel("test#INCOMING#key1#value1#key2#value2").isStrictlyMoreSpecificThan(drel("test2#INCOMING#key1#value1#key2#value2")));
    }

    @Test
    public void shouldGenerateAllMoreGeneral() {
        Set<CandidateRelationship> result = crel("test#INCOMING#key1#value1#key2#value2").generateAllMoreGeneral();

        assertEquals(4, result.size());
        Iterator<CandidateRelationship> iterator = result.iterator();
        assertEquals(iterator.next(), crel("test#INCOMING#key1#value1#key2#value2"));
        assertEquals(iterator.next(), crel("test#INCOMING#key1#value1"));
        assertEquals(iterator.next(), crel("test#INCOMING#key2#value2"));
        assertEquals(iterator.next(), crel("test#INCOMING"));
    }

    @Test
    public void shouldGenerateOneMoreGeneral() {
        Set<CandidateRelationship> result = crel("test#INCOMING#key1#value1#key2#value2").generateOneMoreGeneral();

        assertEquals(3, result.size());
        Iterator<CandidateRelationship> iterator = result.iterator();
        assertEquals(iterator.next(), crel("test#INCOMING#key1#value1#key2#value2"));
        assertEquals(iterator.next(), crel("test#INCOMING#key1#value1"));
        assertEquals(iterator.next(), crel("test#INCOMING#key2#value2"));
    }

    @Test
    public void shouldAchieveSpecificToGeneralOrderingForRelationships() {
        Set<CandidateRelationship> properties = new TreeSet<>();

        properties.add(crel("test#INCOMING"));
        properties.add(crel("test#INCOMING#key1#value1#key2#value2"));
        properties.add(crel("test#INCOMING#key2#value2"));
        properties.add(crel("test#INCOMING#key2#value2"));
        properties.add(crel("test#INCOMING#key2#value2"));
        properties.add(crel("xx#INCOMING#key2#value2"));
        properties.add(crel("test#OUTGOING#key2#value2"));

        Iterator<CandidateRelationship> iterator = properties.iterator();
        assertEquals(crel("test#INCOMING#key1#value1#key2#value2"), iterator.next());
        assertEquals(crel("test#INCOMING#key2#value2"), iterator.next());
        assertEquals(crel("test#INCOMING"), iterator.next());
        assertEquals(crel("test#OUTGOING#key2#value2"), iterator.next());
        assertEquals(crel("xx#INCOMING#key2#value2"), iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void relationshipsShouldBehaveProperlyInTreeSets() {
        Set<CandidateRelationship> properties = new TreeSet<>();

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
        assertEquals(GA_REL_PREFIX + "test#OUTGOING#key1#value1#key2#value2", new CandidateGeneralizedRelationship(GA_REL_PREFIX + "test#OUTGOING#key1#value1#key2#value2").toString());
        assertEquals(GA_REL_PREFIX + "test#INCOMING", new CandidateGeneralizedRelationship(GA_REL_PREFIX + "test#INCOMING").toString());
        assertEquals(GA_REL_PREFIX + "test#INCOMING", new CandidateGeneralizedRelationship(GA_REL_PREFIX + "test#INCOMING#").toString());
    }

    @Test
    public void sameRelationshipsShouldBeEqual() {
        assertTrue(new CandidateGeneralizedRelationship(GA_REL_PREFIX + "test#INCOMING").equals(new CandidateGeneralizedRelationship(GA_REL_PREFIX + "test#INCOMING")));
        assertTrue(new CandidateGeneralizedRelationship(GA_REL_PREFIX + "test#OUTGOING#key1#value1#key2#value2").equals(new CandidateGeneralizedRelationship(GA_REL_PREFIX + "test#OUTGOING#key1#value1#key2#value2")));
        assertTrue(new CandidateGeneralizedRelationship(GA_REL_PREFIX + "test#INCOMING#key1#value1#key2#").equals(new CandidateGeneralizedRelationship(GA_REL_PREFIX + "test#INCOMING#key1#value1#key2")));
    }

    @Test
    public void differentRelationshipsShouldNotBeEqual() {
        assertFalse(new CandidateGeneralizedRelationship(GA_REL_PREFIX + "test#OUTGOING").equals(new CandidateGeneralizedRelationship(GA_REL_PREFIX + "test#INCOMING")));
        assertFalse(new CandidateGeneralizedRelationship(GA_REL_PREFIX + "test2#OUTGOING#key1#value1#key2#value2").equals(new CandidateGeneralizedRelationship(GA_REL_PREFIX + "test#OUTGOING#key1#value1#key2#value2")));
        assertFalse(new CandidateGeneralizedRelationship(GA_REL_PREFIX + "test#OUTGOING#key3#value1#key2#value2").equals(new CandidateGeneralizedRelationship(GA_REL_PREFIX + "test#OUTGOING#key1#value1#key2#value2#")));
    }

    private CandidateRelationship crel(String s) {
        return new CandidateGeneralizedRelationship(GA_REL_PREFIX + s);
    }

    private ImmutableDirectedRelationship<String, ?> drel(String s) {
        return new SerializableDirectedRelationshipImpl(GA_REL_PREFIX + s);
    }
}
