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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.graphaware.relcount.common.module.RelationshipCountModule;
import com.graphaware.relcount.full.strategy.RelationshipCountStrategiesImpl;
import com.graphaware.tx.event.improved.strategy.IncludeNoNodes;
import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Unit test for {@link com.graphaware.relcount.full.module.FullRelationshipCountModule}. These miscellaneous tests, most of the core logic tests are in
 * {@link FullRelationshipCountIntegrationTest}.
 */
public class FullRelationshipCountModuleTest {

    @Test
    public void sameConfigShouldProduceSameString() {
        RelationshipCountStrategiesImpl strategies = RelationshipCountStrategiesImpl.defaultStrategies();
        RelationshipCountModule module1 = new FullRelationshipCountModule(strategies.with(5).with(IncludeNoNodes.getInstance()));
        RelationshipCountModule module2 = new FullRelationshipCountModule(strategies.with(5).with(IncludeNoNodes.getInstance()));

        Assert.assertEquals(module1.asString(), module2.asString());
    }

    @Test
    public void differentConfigShouldProduceDifferentString() {
        RelationshipCountStrategiesImpl strategies = RelationshipCountStrategiesImpl.defaultStrategies();
        RelationshipCountModule module1 = new FullRelationshipCountModule(strategies.with(5));
        RelationshipCountModule module2 = new FullRelationshipCountModule(strategies.with(6));

        assertNotSame(module1.asString(), module2.asString());
    }

    @Test
    public void ser() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("test", 123L);
        map.put("test2", new int[]{1, 2, 3, 4});
        Kryo kryo = new Kryo();
        kryo.register(HashMap.class, 90);
        kryo.register(Map.class, 91);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Output output = new Output(stream);
        kryo.writeObject(output, map);
        output.flush();
        output.close();

        byte[] b = stream.toByteArray();
        String s = Base64.encode(b);
        System.out.println(s);

        Map<String, Object> map2 = kryo.readObject(new Input(Base64.decode(s)), HashMap.class);

        assertEquals(2, map2.size());
        assertEquals(123L, map2.get("test"));
        assertTrue(Arrays.equals(new int[]{1, 2, 3, 4}, (int[]) map2.get("test2")));
    }
}
