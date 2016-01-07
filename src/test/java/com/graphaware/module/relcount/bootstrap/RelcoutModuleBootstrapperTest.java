/*
 * Copyright (c) 2013-2016 GraphAware
 *
 * This file is part of the GraphAware Framework.
 *
 * GraphAware Framework is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details. You should have received a copy of
 * the GNU General Public License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

package com.graphaware.module.relcount.bootstrap;

import com.graphaware.module.relcount.count.*;
import com.graphaware.tx.executor.single.SimpleTransactionExecutor;
import com.graphaware.tx.executor.single.VoidReturningCallback;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.test.TestGraphDatabaseFactory;

import static com.graphaware.common.description.relationship.RelationshipDescriptionFactory.wildcard;
import static com.graphaware.runtime.bootstrap.RuntimeKernelExtension.RUNTIME_ENABLED;
import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.helpers.Settings.STRING;
import static org.neo4j.helpers.Settings.setting;

/**
 * Integration test for {@link RelcountModuleBootstrapper}.
 */
public class RelcoutModuleBootstrapperTest {
    private static final Setting<String> MODULE_ENABLED = setting("com.graphaware.module.relcount.1", STRING, RelcountModuleBootstrapper.class.getCanonicalName());

    protected GraphDatabaseService database;

    @Before
    public void setUp() {
        database = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig(RUNTIME_ENABLED, "true")
                .setConfig(MODULE_ENABLED, MODULE_ENABLED.getDefaultValue())
                .newGraphDatabase();
    }

    @After
    public void tearDown() {
        database.shutdown();
    }

    @Test
    public void defaultRuntimeOnExistingDatabase() {
        simulateUsage();

        verifyCounts(new NaiveRelationshipCounter());
        verifyCounts(new NaiveRelationshipCounter(database, "relcount"));
        verifyCounts(new CachedRelationshipCounter(database, "relcount"));
        verifyCounts(new LegacyFallbackRelationshipCounter(database, "relcount"));
        verifyCounts(new FallbackRelationshipCounter(database, "relcount"));
    }

    private void verifyCounts(RelationshipCounter counter) {
        try (Transaction tx = database.beginTx()) {
            Node one = database.getNodeById(0);

            assertEquals(1, counter.count(one, wildcard(withName("ONE"), OUTGOING)));

            tx.success();
        }
    }

    private void simulateUsage() {
        new SimpleTransactionExecutor(database).executeInTransaction(new VoidReturningCallback() {
            @Override
            protected void doInTx(GraphDatabaseService database) {
                database.createNode();
                database.createNode();

                Node one = database.getNodeById(0);
                Node two = database.getNodeById(1);

                one.createRelationshipTo(two, withName("ONE"));
            }
        });
    }
}
