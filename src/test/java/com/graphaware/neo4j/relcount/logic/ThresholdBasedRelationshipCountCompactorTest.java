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

package com.graphaware.neo4j.relcount.logic;

import com.graphaware.neo4j.relcount.representation.ComparableRelationship;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Node;

import java.util.Map;
import java.util.TreeMap;

import static com.graphaware.neo4j.utils.Constants.GA_REL_PREFIX;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link com.graphaware.neo4j.relcount.logic.ThresholdBasedRelationshipCountCompactor}
 */
public class ThresholdBasedRelationshipCountCompactorTest {

    private RelationshipCountCompactor compactor;
    private RelationshipCountManager mockManager;
    private Node mockNode;

    @Before
    public void setUp() {
        mockManager = mock(RelationshipCountManager.class);

        mockNode = mock(Node.class);
    }

    private Map<ComparableRelationship, Integer> generateCachedCounts() {
        Map<ComparableRelationship, Integer> cachedCounts = new TreeMap<ComparableRelationship, Integer>();
        cachedCounts.put(rel("test#OUTGOING#k1#v1#k2#v1"), 1);
        cachedCounts.put(rel("test#OUTGOING#k1#v1#k2#v2"), 1);
        cachedCounts.put(rel("test#OUTGOING#k1#v1#k2#v3"), 2);
        cachedCounts.put(rel("test#OUTGOING#k1#v1#k2#v4"), 1);
        cachedCounts.put(rel("test#OUTGOING#k1#v1#k2#v5"), 1);
        cachedCounts.put(rel("test#OUTGOING#k1#v1#k2#v6"), 1);
        cachedCounts.put(rel("test#OUTGOING#k1#v1#k2#v7"), 1);
        cachedCounts.put(rel("test#OUTGOING#k1#v1#k2#v8"), 1);
        cachedCounts.put(rel("test#OUTGOING#k1#v1"), 5);

        cachedCounts.put(rel("test#OUTGOING#k2#v1"), 1);
        cachedCounts.put(rel("test#OUTGOING#k2#v2"), 2);
        cachedCounts.put(rel("test#OUTGOING#k2#v3"), 3);
        cachedCounts.put(rel("test#OUTGOING#k2#v4"), 1);
        cachedCounts.put(rel("test#OUTGOING#k2#v5"), 1);

        cachedCounts.put(rel("test#OUTGOING#k3#v3"), 100);
        return cachedCounts;
    }

    private Map<ComparableRelationship, Integer> generateCompactedCachedCounts() {
        Map<ComparableRelationship, Integer> cachedCounts = new TreeMap<ComparableRelationship, Integer>();
        cachedCounts.put(rel("test#OUTGOING#k1#v1"), 14);

        cachedCounts.put(rel("test#OUTGOING#k2#v1"), 1);
        cachedCounts.put(rel("test#OUTGOING#k2#v2"), 2);
        cachedCounts.put(rel("test#OUTGOING#k2#v3"), 3);
        cachedCounts.put(rel("test#OUTGOING#k2#v4"), 1);
        cachedCounts.put(rel("test#OUTGOING#k2#v5"), 1);

        cachedCounts.put(rel("test#OUTGOING#k3#v3"), 100);
        return cachedCounts;
    }

    private Map<ComparableRelationship, Integer> generateMoreCompactedCachedCounts() {
        Map<ComparableRelationship, Integer> cachedCounts = new TreeMap<ComparableRelationship, Integer>();
        cachedCounts.put(rel("test#OUTGOING"), 122);
        return cachedCounts;
    }

    @Test
    public void nothingShouldBeCompactedBeforeThresholdIsReached() {
        when(mockManager.getRelationshipCounts(mockNode)).thenReturn(generateCachedCounts());

        compactor = new ThresholdBasedRelationshipCountCompactor(8, mockManager);

        compactor.compactRelationshipCounts(rel("test#OUTGOING#k1#v1#k2#v6"), mockNode);

        verify(mockManager).getRelationshipCounts(mockNode);
        verifyNoMoreInteractions(mockManager, mockNode);
    }

    @Test
    public void nothingShouldBeCompactedBeforeThresholdIsReached2() {
        when(mockManager.getRelationshipCounts(mockNode))
                .thenReturn(generateCachedCounts())
                .thenThrow(new RuntimeException("Too many invocations")); //to prevent infinite loop

        compactor = new ThresholdBasedRelationshipCountCompactor(8, mockManager);

        compactor.compactRelationshipCounts(rel("test#OUTGOING#k1#v1"), mockNode);

        verify(mockManager).getRelationshipCounts(mockNode);
        verifyNoMoreInteractions(mockManager, mockNode);
    }

    @Test
    public void verifyCompaction() {
        when(mockManager.getRelationshipCounts(mockNode))
                .thenReturn(generateCachedCounts())
                .thenReturn(generateCompactedCachedCounts());

        compactor = new ThresholdBasedRelationshipCountCompactor(7, mockManager);

        compactor.compactRelationshipCounts(rel("test#OUTGOING#k1#v1#k2#v6"), mockNode);

        verify(mockManager, times(2)).getRelationshipCounts(mockNode);

        verify(mockManager).deleteCount(rel("test#OUTGOING#k1#v1#k2#v1"), mockNode);
        verify(mockManager).deleteCount(rel("test#OUTGOING#k1#v1#k2#v2"), mockNode);
        verify(mockManager).deleteCount(rel("test#OUTGOING#k1#v1#k2#v3"), mockNode);
        verify(mockManager).deleteCount(rel("test#OUTGOING#k1#v1#k2#v4"), mockNode);
        verify(mockManager).deleteCount(rel("test#OUTGOING#k1#v1#k2#v5"), mockNode);
        verify(mockManager).deleteCount(rel("test#OUTGOING#k1#v1#k2#v6"), mockNode);
        verify(mockManager).deleteCount(rel("test#OUTGOING#k1#v1#k2#v7"), mockNode);
        verify(mockManager).deleteCount(rel("test#OUTGOING#k1#v1#k2#v8"), mockNode);

        verify(mockManager, times(7)).incrementCount(rel("test#OUTGOING#k1#v1"), mockNode, 1);
        verify(mockManager, times(1)).incrementCount(rel("test#OUTGOING#k1#v1"), mockNode, 2);

        verifyNoMoreInteractions(mockManager, mockNode);
    }

    @Test
    public void verifyCompaction2() {
        when(mockManager.getRelationshipCounts(mockNode))
                .thenReturn(generateCachedCounts())
                .thenReturn(generateCompactedCachedCounts())
                .thenReturn(generateMoreCompactedCachedCounts());

        compactor = new ThresholdBasedRelationshipCountCompactor(6, mockManager);

        compactor.compactRelationshipCounts(rel("test#OUTGOING#k1#v1#k2#v6"), mockNode);

        verify(mockManager, times(3)).getRelationshipCounts(mockNode);

        verify(mockManager).deleteCount(rel("test#OUTGOING#k1#v1#k2#v1"), mockNode);
        verify(mockManager).deleteCount(rel("test#OUTGOING#k1#v1#k2#v2"), mockNode);
        verify(mockManager).deleteCount(rel("test#OUTGOING#k1#v1#k2#v3"), mockNode);
        verify(mockManager).deleteCount(rel("test#OUTGOING#k1#v1#k2#v4"), mockNode);
        verify(mockManager).deleteCount(rel("test#OUTGOING#k1#v1#k2#v5"), mockNode);
        verify(mockManager).deleteCount(rel("test#OUTGOING#k1#v1#k2#v6"), mockNode);
        verify(mockManager).deleteCount(rel("test#OUTGOING#k1#v1#k2#v7"), mockNode);
        verify(mockManager).deleteCount(rel("test#OUTGOING#k1#v1#k2#v8"), mockNode);

        verify(mockManager, times(7)).incrementCount(rel("test#OUTGOING#k1#v1"), mockNode, 1);
        verify(mockManager, times(1)).incrementCount(rel("test#OUTGOING#k1#v1"), mockNode, 2);

        verify(mockManager).deleteCount(rel("test#OUTGOING#k1#v1"), mockNode);
        verify(mockManager).deleteCount(rel("test#OUTGOING#k2#v1"), mockNode);
        verify(mockManager).deleteCount(rel("test#OUTGOING#k2#v2"), mockNode);
        verify(mockManager).deleteCount(rel("test#OUTGOING#k2#v3"), mockNode);
        verify(mockManager).deleteCount(rel("test#OUTGOING#k2#v4"), mockNode);
        verify(mockManager).deleteCount(rel("test#OUTGOING#k2#v5"), mockNode);
        verify(mockManager).deleteCount(rel("test#OUTGOING#k3#v3"), mockNode);

        verify(mockManager).incrementCount(rel("test#OUTGOING"), mockNode, 14);
        verify(mockManager).incrementCount(rel("test#OUTGOING"), mockNode, 2);
        verify(mockManager).incrementCount(rel("test#OUTGOING"), mockNode, 3);
        verify(mockManager).incrementCount(rel("test#OUTGOING"), mockNode, 100);
        verify(mockManager, times(3)).incrementCount(rel("test#OUTGOING"), mockNode, 1);

        verifyNoMoreInteractions(mockManager, mockNode);
    }

    /**
     * just for readability
     */
    private ComparableRelationship rel(String s) {
        return new ComparableRelationship(GA_REL_PREFIX + s);
    }
}
