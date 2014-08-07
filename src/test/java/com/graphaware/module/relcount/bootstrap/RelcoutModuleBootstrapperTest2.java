package com.graphaware.module.relcount.bootstrap;

import com.graphaware.module.relcount.count.*;
import com.graphaware.tx.executor.single.SimpleTransactionExecutor;
import com.graphaware.tx.executor.single.VoidReturningCallback;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static com.graphaware.common.description.relationship.RelationshipDescriptionFactory.wildcard;
import static com.graphaware.runtime.bootstrap.RuntimeKernelExtension.RUNTIME_ENABLED;
import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.helpers.Settings.STRING;
import static org.neo4j.helpers.Settings.setting;

/**
 * Integration test for {@link com.graphaware.module.relcount.bootstrap.RelcountModuleBootstrapper}.
 */
public class RelcoutModuleBootstrapperTest2 {
    private static final Setting<String> MODULE_ENABLED = setting("com.graphaware.module.relcount.1", STRING, RelcountModuleBootstrapper.class.getCanonicalName());

    protected GraphDatabaseService database;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @After
    public void tearDown() {
        database.shutdown();
    }

    @Test
    public void defaultRuntimeOnExistingDatabase() {
        database = new GraphDatabaseFactory()
                .newEmbeddedDatabase(temporaryFolder.getRoot().getAbsolutePath());

        simulateUsage();

        database.shutdown();

        database = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(temporaryFolder.getRoot().getAbsolutePath())
                .setConfig(RUNTIME_ENABLED, "true")
                .setConfig(MODULE_ENABLED, MODULE_ENABLED.getDefaultValue())
                .newGraphDatabase();

        verifyCounts(new LegacyNaiveRelationshipCounter(database));
        verifyCounts(new NaiveRelationshipCounter(database));
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
