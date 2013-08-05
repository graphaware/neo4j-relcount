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

package com.graphaware.relcount.full.module;

import com.graphaware.relcount.common.module.RelationshipCountModule;
import com.graphaware.relcount.full.strategy.RelationshipCountStrategiesImpl;
import com.graphaware.tx.event.improved.strategy.IncludeNoNodes;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

/**
 * Unit test for {@link com.graphaware.relcount.full.module.FullRelationshipCountModule}. These miscellaneous tests, most of the core logic tests are in
 * {@link FullRelationshipCountIntegrationTest}.
 */
public class FullRelationshipCountModuleTest {

    @Test
    public void sameConfigShouldHaveSameHashCode() {
        RelationshipCountStrategiesImpl strategies = RelationshipCountStrategiesImpl.defaultStrategies();
        RelationshipCountModule module1 = new FullRelationshipCountModule(strategies.with(5).with(IncludeNoNodes.getInstance()));
        RelationshipCountModule module2 = new FullRelationshipCountModule(strategies.with(5).with(IncludeNoNodes.getInstance()));

        assertEquals(module1.hashCode(), module2.hashCode());
    }

    @Test
    public void differentConfigShouldHaveADifferentHashCode() {
        RelationshipCountStrategiesImpl strategies = RelationshipCountStrategiesImpl.defaultStrategies();
        RelationshipCountModule module1 = new FullRelationshipCountModule(strategies.with(5));
        RelationshipCountModule module2 = new FullRelationshipCountModule(strategies.with(6));

        assertNotSame(module1.hashCode(), module2.hashCode());
    }
}
