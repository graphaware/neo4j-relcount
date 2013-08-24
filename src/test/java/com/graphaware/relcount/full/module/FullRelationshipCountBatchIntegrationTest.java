package com.graphaware.relcount.full.module;

import com.graphaware.framework.BatchGraphAwareFramework;
import com.graphaware.relcount.common.BatchIntegrationTest;
import com.graphaware.relcount.common.counter.UnableToCountException;
import com.graphaware.relcount.full.counter.FullCachedRelationshipCounter;
import com.graphaware.relcount.full.counter.FullFallingBackRelationshipCounter;
import com.graphaware.relcount.full.counter.FullNaiveRelationshipCounter;
import com.graphaware.relcount.full.counter.FullRelationshipCounter;
import com.graphaware.relcount.full.strategy.RelationshipCountStrategiesImpl;
import com.graphaware.relcount.full.strategy.RelationshipPropertiesExtractionStrategy;
import com.graphaware.relcount.full.strategy.RelationshipWeighingStrategy;
import com.graphaware.tx.event.improved.strategy.IncludeAllNodeProperties;
import com.graphaware.tx.event.improved.strategy.IncludeAllNodes;
import com.graphaware.tx.event.improved.strategy.RelationshipInclusionStrategy;
import com.graphaware.tx.event.improved.strategy.RelationshipPropertyInclusionStrategy;
import org.junit.Assert;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.TransactionSimulatingBatchInserterImpl;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.fail;
import static org.neo4j.graphdb.Direction.*;

/**
 * Integration test for full relationship counting with batch inserter.
 */
@SuppressWarnings("PointlessArithmeticExpression")
public class FullRelationshipCountBatchIntegrationTest extends BatchIntegrationTest {

    @Test
    public void noFramework() {
        setUpTwoNodes();
        simulateInserts();
        startDatabase();

        verifyCounts(1, defaultNaiveCounterCreator());
        verifyCounts(0, defaultCachedCounterCreator());
        verifyCounts(0, defaultFallbackCounterCreator());
    }

    @Test
    public void noFramework2() {
        setUpTwoNodes();
        simulateInserts();
        simulateInserts();
        startDatabase();

        verifyCounts(2, defaultNaiveCounterCreator());
        verifyCounts(0, defaultCachedCounterCreator());
        verifyCounts(0, defaultFallbackCounterCreator());
    }

    @Test
    public void cachedCountsCanBeRebuilt() {
        BatchGraphAwareFramework framework = new BatchGraphAwareFramework(batchInserter);
        FullRelationshipCountModule module = new FullRelationshipCountModule();
        framework.registerModule(module);
        framework.start();

        setUpTwoNodes();
        simulateInserts();

        batchInserter.shutdown();

        batchInserter = new TransactionSimulatingBatchInserterImpl(temporaryFolder.getRoot().getAbsolutePath());
        framework = new BatchGraphAwareFramework(batchInserter);
        module = new FullRelationshipCountModule();
        framework.registerModule(module);
        framework.start();

        module.reinitialize(batchInserter);

        startDatabase();

        verifyCounts(1, naiveCounterCreator(module));
        verifyCounts(1, cachedCounterCreator(module));
        verifyCounts(1, fallbackCounterCreator(module));
    }

    @Test
    public void defaultFrameworkOnNewDatabase() {
        BatchGraphAwareFramework framework = new BatchGraphAwareFramework(batchInserter);
        final FullRelationshipCountModule module = new FullRelationshipCountModule();
        framework.registerModule(module);
        framework.start();

        setUpTwoNodes();
        simulateInserts();
        startDatabase();

        verifyCounts(1, naiveCounterCreator(module));
        verifyCounts(1, cachedCounterCreator(module));
        verifyCounts(1, fallbackCounterCreator(module));
    }

    @Test
    public void defaultFrameworkWithChangedModule() throws IOException {
        BatchGraphAwareFramework framework = new BatchGraphAwareFramework(batchInserter);
        FullRelationshipCountModule module = new FullRelationshipCountModule();
        framework.registerModule(module);
        framework.start();

        setUpTwoNodes();
        simulateInserts();
        startDatabase();

        verifyCounts(1, naiveCounterCreator(module));
        verifyCounts(1, cachedCounterCreator(module));
        verifyCounts(1, fallbackCounterCreator(module));

        database.shutdown();

        batchInserter = new TransactionSimulatingBatchInserterImpl(temporaryFolder.getRoot().getAbsolutePath());

        framework = new BatchGraphAwareFramework(batchInserter);
        module = new FullRelationshipCountModule(RelationshipCountStrategiesImpl.defaultStrategies().with(4));
        framework.registerModule(module);
        framework.start();

        startDatabase();

        verifyCounts(1, naiveCounterCreator(module));
        verifyCompactedCounts(1, cachedCounterCreator(module));
        verifyCounts(1, fallbackCounterCreator(module));

        database.shutdown();

        batchInserter = new TransactionSimulatingBatchInserterImpl(temporaryFolder.getRoot().getAbsolutePath());

        framework = new BatchGraphAwareFramework(batchInserter);
        module = new FullRelationshipCountModule(RelationshipCountStrategiesImpl.defaultStrategies().with(20));
        framework.registerModule(module);
        framework.start();

        startDatabase();

        verifyCounts(1, naiveCounterCreator(module));
        verifyCounts(1, cachedCounterCreator(module));
        verifyCounts(1, fallbackCounterCreator(module));
    }

    @Test
    public void defaultFrameworkOnExistingDatabase() {
        setUpTwoNodes();
        simulateInserts();

        BatchGraphAwareFramework framework = new BatchGraphAwareFramework(batchInserter);
        final FullRelationshipCountModule module = new FullRelationshipCountModule();
        framework.registerModule(module);
        framework.start();

        startDatabase();

        verifyCounts(1, naiveCounterCreator(module));
        verifyCounts(1, cachedCounterCreator(module));
        verifyCounts(1, fallbackCounterCreator(module));
    }

    @Test
    public void customFrameworkOnNewDatabase() {
        BatchGraphAwareFramework framework = new BatchGraphAwareFramework(batchInserter, new CustomConfig());
        final FullRelationshipCountModule module = new FullRelationshipCountModule();
        framework.registerModule(module);
        framework.start();

        setUpTwoNodes();
        simulateInserts();
        startDatabase();

        verifyCounts(1, naiveCounterCreator(module));
        verifyCounts(1, cachedCounterCreator(module));
        verifyCounts(1, fallbackCounterCreator(module));
    }

    @Test
    public void customFrameworkOnExistingDatabase() {
        setUpTwoNodes();
        simulateInserts();

        BatchGraphAwareFramework framework = new BatchGraphAwareFramework(batchInserter, new CustomConfig());
        final FullRelationshipCountModule module = new FullRelationshipCountModule();
        framework.registerModule(module);
        framework.start();

        startDatabase();

        verifyCounts(1, naiveCounterCreator(module));
        verifyCounts(1, cachedCounterCreator(module));
        verifyCounts(1, fallbackCounterCreator(module));
    }

    @Test
    public void weightedRelationships() {
        BatchGraphAwareFramework framework = new BatchGraphAwareFramework(batchInserter, new CustomConfig());
        final FullRelationshipCountModule module = new FullRelationshipCountModule(
                RelationshipCountStrategiesImpl.defaultStrategies()
                        .with(new RelationshipWeighingStrategy() {
                            @Override
                            public int getRelationshipWeight(Relationship relationship, Node pointOfView) {
                                return (int) relationship.getProperty(BatchIntegrationTest.WEIGHT, 1);
                            }

                            @Override
                            public String asString() {
                                return "custom";
                            }
                        }));

        framework.registerModule(module);
        framework.start();

        setUpTwoNodes();
        simulateInserts();
        startDatabase();

        verifyWeightedCounts(1, naiveCounterCreator(module));
        verifyWeightedCounts(1, cachedCounterCreator(module));
        verifyWeightedCounts(1, fallbackCounterCreator(module));
    }


    @Test
    public void defaultStrategiesWithLowerThreshold() {
        BatchGraphAwareFramework framework = new BatchGraphAwareFramework(batchInserter, new CustomConfig());
        final FullRelationshipCountModule module = new FullRelationshipCountModule(
                RelationshipCountStrategiesImpl.defaultStrategies().with(4)
        );
        framework.registerModule(module);
        framework.start();

        setUpTwoNodes();
        simulateInserts();
        startDatabase();

        verifyCounts(1, naiveCounterCreator(module));
        verifyCompactedCounts(1, cachedCounterCreator(module));
        verifyCounts(1, fallbackCounterCreator(module));
    }

    @Test
    public void defaultStrategiesWithLowerThreshold2() {
        BatchGraphAwareFramework framework = new BatchGraphAwareFramework(batchInserter);
        final FullRelationshipCountModule module = new FullRelationshipCountModule(
                RelationshipCountStrategiesImpl.defaultStrategies().with(4)
        );
        framework.registerModule(module);
        framework.start();

        setUpTwoNodes();
        simulateInserts();
        simulateInserts();
        startDatabase();

        verifyCounts(2, naiveCounterCreator(module));
        verifyCompactedCounts(2, cachedCounterCreator(module));
        verifyCounts(2, fallbackCounterCreator(module));
    }

    @Test
    public void defaultStrategiesWithLowerThreshold3() {
        BatchGraphAwareFramework framework = new BatchGraphAwareFramework(batchInserter, new CustomConfig());
        final FullRelationshipCountModule module = new FullRelationshipCountModule(
                RelationshipCountStrategiesImpl.defaultStrategies().with(3)
        );
        framework.registerModule(module);
        framework.start();

        setUpTwoNodes();
        simulateInserts();
        startDatabase();

        try {
            cachedCounterCreator(module).createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.TIMESTAMP, "123").with(BatchIntegrationTest.K1, "V1").count(database.getNodeById(1));
            fail();
        } catch (UnableToCountException e) {
            //OK
        }
    }

    @Test
    public void weightedRelationshipsWithCompaction() {
        BatchGraphAwareFramework framework = new BatchGraphAwareFramework(batchInserter, new CustomConfig());
        final FullRelationshipCountModule module = new FullRelationshipCountModule(
                RelationshipCountStrategiesImpl.defaultStrategies()
                        .with(new RelationshipWeighingStrategy() {
                            @Override
                            public int getRelationshipWeight(Relationship relationship, Node pointOfView) {
                                return (int) relationship.getProperty(BatchIntegrationTest.WEIGHT, 1);
                            }

                            @Override
                            public String asString() {
                                return "custom";
                            }
                        })
                        .with(10));

        framework.registerModule(module);
        framework.start();

        setUpTwoNodes();
        simulateInserts();
        simulateInserts();
        simulateInserts();
        simulateInserts();
        startDatabase();

        verifyWeightedCounts(4, fallbackCounterCreator(module));
        verifyWeightedCounts(4, naiveCounterCreator(module));
    }

    @Test
    public void twoSimultaneousModules() {
        BatchGraphAwareFramework framework = new BatchGraphAwareFramework(batchInserter);
        final FullRelationshipCountModule module1 = new FullRelationshipCountModule("M1", RelationshipCountStrategiesImpl.defaultStrategies());
        final FullRelationshipCountModule module2 = new FullRelationshipCountModule("M2",
                RelationshipCountStrategiesImpl.defaultStrategies()
                        .with(new RelationshipWeighingStrategy() {
                            @Override
                            public int getRelationshipWeight(Relationship relationship, Node pointOfView) {
                                return (int) relationship.getProperty(BatchIntegrationTest.WEIGHT, 1);
                            }

                            @Override
                            public String asString() {
                                return "custom";
                            }
                        }));

        framework.registerModule(module1);
        framework.registerModule(module2);
        framework.start();

        setUpTwoNodes();
        simulateInserts();
        simulateInserts();
        startDatabase();

        verifyCounts(2, naiveCounterCreator(module1));
        verifyCounts(2, cachedCounterCreator(module1));
        verifyCounts(2, fallbackCounterCreator(module1));

        verifyWeightedCounts(2, naiveCounterCreator(module2));
        verifyWeightedCounts(2, cachedCounterCreator(module2));
        verifyWeightedCounts(2, fallbackCounterCreator(module2));
    }

    @Test
    public void customRelationshipPropertiesExtractionStrategy() {
        BatchGraphAwareFramework framework = new BatchGraphAwareFramework(batchInserter, new CustomConfig());
        final FullRelationshipCountModule module = new FullRelationshipCountModule(
                RelationshipCountStrategiesImpl.defaultStrategies()
                        .with(new RelationshipPropertiesExtractionStrategy.OtherNodeIncludingAdapter() {
                            @Override
                            protected Map<String, String> extractProperties(Map<String, String> properties, Node otherNode) {
                                properties.put("otherNodeName", otherNode.getProperty(BatchIntegrationTest.NAME, "UNKNOWN").toString());
                                return properties;
                            }

                            @Override
                            public String asString() {
                                return "custom";
                            }
                        })
                        .with(IncludeAllNodes.getInstance())
                        .with(IncludeAllNodeProperties.getInstance()));

        framework.registerModule(module);
        framework.start();

        setUpTwoNodes();
        simulateInserts();
        simulateInserts();
        startDatabase();

        Assert.assertEquals(4, naiveCounterCreator(module).createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V1").with("otherNodeName", "Two").count(database.getNodeById(1)));
        Assert.assertEquals(4, fallbackCounterCreator(module).createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V1").with("otherNodeName", "Two").count(database.getNodeById(1)));
        Assert.assertEquals(4, cachedCounterCreator(module).createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V1").with("otherNodeName", "Two").count(database.getNodeById(1)));
    }

    @Test
    public void customRelationshipInclusionStrategy() {
        BatchGraphAwareFramework framework = new BatchGraphAwareFramework(batchInserter, new CustomConfig());
        final FullRelationshipCountModule module = new FullRelationshipCountModule(
                RelationshipCountStrategiesImpl.defaultStrategies()
                        .with(new RelationshipInclusionStrategy() {
                            @Override
                            public boolean include(Relationship relationship) {
                                return !relationship.isType(RelationshipTypes.TWO);
                            }

                            @Override
                            public String asString() {
                                return "custom";
                            }
                        }));

        framework.registerModule(module);
        framework.start();

        setUpTwoNodes();
        simulateInserts();
        simulateInserts();
        startDatabase();

        //naive doesn't care about this strategy
        Assert.assertEquals(2, naiveCounterCreator(module).createCounter(RelationshipTypes.TWO, OUTGOING).count(database.getNodeById(1)));
        Assert.assertEquals(0, fallbackCounterCreator(module).createCounter(RelationshipTypes.TWO, OUTGOING).count(database.getNodeById(1)));
        Assert.assertEquals(0, cachedCounterCreator(module).createCounter(RelationshipTypes.TWO, OUTGOING).count(database.getNodeById(1)));
    }

    @Test
    public void customRelationshipPropertiesInclusionStrategy() {
        BatchGraphAwareFramework framework = new BatchGraphAwareFramework(batchInserter, new CustomConfig());
        final FullRelationshipCountModule module = new FullRelationshipCountModule(
                RelationshipCountStrategiesImpl.defaultStrategies()
                        .with(new RelationshipPropertyInclusionStrategy() {
                            @Override
                            public boolean include(String key, Relationship propertyContainer) {
                                return !BatchIntegrationTest.WEIGHT.equals(key);
                            }

                            @Override
                            public String asString() {
                                return "custom";
                            }
                        }));

        framework.registerModule(module);
        framework.start();

        setUpTwoNodes();
        simulateInserts();
        simulateInserts();
        startDatabase();

        //naive doesn't care about this strategy
        Assert.assertEquals(2, naiveCounterCreator(module).createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 7).count(database.getNodeById(1)));
        Assert.assertEquals(2, naiveCounterCreator(module).createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 7).countLiterally(database.getNodeById(1)));
        Assert.assertEquals(0, cachedCounterCreator(module).createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 7).count(database.getNodeById(1)));
        Assert.assertEquals(0, cachedCounterCreator(module).createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 7).countLiterally(database.getNodeById(1)));
        Assert.assertEquals(0, fallbackCounterCreator(module).createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 7).count(database.getNodeById(1)));
        Assert.assertEquals(0, fallbackCounterCreator(module).createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 7).countLiterally(database.getNodeById(1)));
    }

    @Test
    public void batchTest() {
        BatchGraphAwareFramework framework = new BatchGraphAwareFramework(batchInserter, new CustomConfig());
        final FullRelationshipCountModule module = new FullRelationshipCountModule();
        framework.registerModule(module);
        framework.start();

        setUpTwoNodes();

        for (int i = 0; i < 100; i++) {
            simulateInserts();
        }

        startDatabase();

        verifyCounts(100, naiveCounterCreator(module));
        verifyCounts(100, cachedCounterCreator(module));
        verifyCounts(100, fallbackCounterCreator(module));
    }

    @Test
    public void batchTestWithMultipleModulesAndLowerThreshold() {
        BatchGraphAwareFramework framework = new BatchGraphAwareFramework(batchInserter, new CustomConfig());
        final FullRelationshipCountModule module1 = new FullRelationshipCountModule("M1", RelationshipCountStrategiesImpl.defaultStrategies().with(4));
        final FullRelationshipCountModule module2 = new FullRelationshipCountModule("M2", RelationshipCountStrategiesImpl.defaultStrategies().with(4));
        framework.registerModule(module1);
        framework.registerModule(module2);
        framework.start();

        setUpTwoNodes();

        for (int i = 0; i < 20; i++) {
            simulateInserts();
        }

        startDatabase();

        verifyCounts(20, naiveCounterCreator(module1));
        verifyCompactedCounts(20, cachedCounterCreator(module1));
        verifyCounts(20, fallbackCounterCreator(module1));

        verifyCounts(20, naiveCounterCreator(module2));
        verifyCompactedCounts(20, cachedCounterCreator(module2));
        verifyCounts(20, fallbackCounterCreator(module2));
    }

    //helpers

    private CounterCreator defaultNaiveCounterCreator() {
        return new CounterCreator() {
            @Override
            public FullRelationshipCounter createCounter(RelationshipType type, Direction direction) {
                return new FullNaiveRelationshipCounter(type, direction);
            }
        };
    }

    private CounterCreator defaultFallbackCounterCreator() {
        return new CounterCreator() {
            @Override
            public FullRelationshipCounter createCounter(RelationshipType type, Direction direction) {
                return new FullFallingBackRelationshipCounter(type, direction);
            }
        };
    }

    private CounterCreator defaultCachedCounterCreator() {
        return new CounterCreator() {
            @Override
            public FullRelationshipCounter createCounter(RelationshipType type, Direction direction) {
                return new FullCachedRelationshipCounter(type, direction);
            }
        };
    }

    private CounterCreator naiveCounterCreator(final FullRelationshipCountModule module) {
        return new CounterCreator() {
            @Override
            public FullRelationshipCounter createCounter(RelationshipType type, Direction direction) {
                return module.naiveCounter(type, direction);
            }
        };
    }

    private CounterCreator cachedCounterCreator(final FullRelationshipCountModule module) {
        return new CounterCreator() {
            @Override
            public FullRelationshipCounter createCounter(RelationshipType type, Direction direction) {
                return module.cachedCounter(type, direction);
            }
        };
    }

    private CounterCreator fallbackCounterCreator(final FullRelationshipCountModule module) {
        return new CounterCreator() {
            @Override
            public FullRelationshipCounter createCounter(RelationshipType type, Direction direction) {
                return module.fallingBackCounter(type, direction);
            }
        };
    }

    private void verifyCounts(int factor, CounterCreator counterCreator) {
        Node one = database.getNodeById(1);
        Node two = database.getNodeById(2);

        //Node one incoming

        Assert.assertEquals(3 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).countLiterally(one));

        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 1).count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 1).countLiterally(one));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 2).count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 2).countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 3).count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 3).countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 4).count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 4).countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 5).count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 5).countLiterally(one));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 7).count(one));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 7).countLiterally(one));

        Assert.assertEquals(2 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V1").count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V1").countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V2").count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V2").countLiterally(one));

        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K2, "V1").count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K2, "V1").countLiterally(one));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K2, "V2").count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K2, "V2").countLiterally(one));

        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V2").count(one));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V2").countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V1").count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V1").countLiterally(one));

        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K1, "V1").count(one));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K1, "V1").countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K2, "V2").count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K2, "V2").countLiterally(one));

        //Node one outgoing

        Assert.assertEquals(7 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).countLiterally(one));

        Assert.assertEquals(2 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 1).count(one));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 1).countLiterally(one));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 2).count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 2).countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 3).count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 3).countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 4).count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 4).countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 5).count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 5).countLiterally(one));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 7).count(one));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 7).countLiterally(one));

        Assert.assertEquals(5 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K1, "V1").count(one));
        Assert.assertEquals(3 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K1, "V1").countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K1, "V2").count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K1, "V2").countLiterally(one));

        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K2, "V1").count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K2, "V1").countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K2, "V2").count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K2, "V2").countLiterally(one));

        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V2").count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V2").countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V1").count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V1").countLiterally(one));

        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K1, "V1").count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K1, "V1").countLiterally(one));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 1).with(BatchIntegrationTest.K1, "V1").count(one));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 1).with(BatchIntegrationTest.K1, "V1").countLiterally(one));

        //Node one both

        Assert.assertEquals(10 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).countLiterally(one));

        Assert.assertEquals(2 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 1).count(one));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 1).countLiterally(one));
        Assert.assertEquals(2 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 2).count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 2).countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 3).count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 3).countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 4).count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 4).countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 5).count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 5).countLiterally(one));
        Assert.assertEquals(2 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 7).count(one));
        Assert.assertEquals(2 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 7).countLiterally(one));

        Assert.assertEquals(7 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K1, "V1").count(one));
        Assert.assertEquals(3 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K1, "V1").countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K1, "V2").count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K1, "V2").countLiterally(one));

        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K2, "V1").count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K2, "V1").countLiterally(one));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K2, "V2").count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K2, "V2").countLiterally(one));

        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V2").count(one));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V2").countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V1").count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V1").countLiterally(one));

        Assert.assertEquals(2 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K1, "V1").count(one));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K1, "V1").countLiterally(one));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 1).with(BatchIntegrationTest.K1, "V1").count(one));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 1).with(BatchIntegrationTest.K1, "V1").countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K2, "V2").count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K2, "V2").countLiterally(one));

        //Node two outgoing

        Assert.assertEquals(2 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).countLiterally(two));

        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 1).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 1).countLiterally(two));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 2).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 2).countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 3).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 3).countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 4).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 4).countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 5).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 5).countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 7).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 7).countLiterally(two));

        Assert.assertEquals(2 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K1, "V1").count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K1, "V1").countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K1, "V2").count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K1, "V2").countLiterally(two));

        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K2, "V1").count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K2, "V1").countLiterally(two));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K2, "V2").count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K2, "V2").countLiterally(two));

        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V2").count(two));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V2").countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V1").count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V1").countLiterally(two));

        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K1, "V1").count(two));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K1, "V1").countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K2, "V2").count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K2, "V2").countLiterally(two));

        //Node two incoming

        Assert.assertEquals(6 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).countLiterally(two));

        Assert.assertEquals(2 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 1).count(two));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 1).countLiterally(two));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 2).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 2).countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 3).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 3).countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 4).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 4).countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 5).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 5).countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 7).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 7).countLiterally(two));

        Assert.assertEquals(5 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V1").count(two));
        Assert.assertEquals(3 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V1").countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V2").count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V2").countLiterally(two));

        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K2, "V1").count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K2, "V1").countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K2, "V2").count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K2, "V2").countLiterally(two));

        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V2").count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V2").countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V1").count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V1").countLiterally(two));

        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K1, "V1").count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K1, "V1").countLiterally(two));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 1).with(BatchIntegrationTest.K1, "V1").count(two));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 1).with(BatchIntegrationTest.K1, "V1").countLiterally(two));

        //Node two both

        Assert.assertEquals(8 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).countLiterally(two));

        Assert.assertEquals(2 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 1).count(two));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 1).countLiterally(two));
        Assert.assertEquals(2 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 2).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 2).countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 3).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 3).countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 4).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 4).countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 5).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 5).countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 7).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 7).countLiterally(two));

        Assert.assertEquals(7 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K1, "V1").count(two));
        Assert.assertEquals(3 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K1, "V1").countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K1, "V2").count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K1, "V2").countLiterally(two));

        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K2, "V1").count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K2, "V1").countLiterally(two));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K2, "V2").count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K2, "V2").countLiterally(two));

        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V2").count(two));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V2").countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V1").count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V1").countLiterally(two));

        Assert.assertEquals(2 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K1, "V1").count(two));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K1, "V1").countLiterally(two));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 1).with(BatchIntegrationTest.K1, "V1").count(two));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 1).with(BatchIntegrationTest.K1, "V1").countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K2, "V2").count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K2, "V2").countLiterally(two));
    }

    private void verifyWeightedCounts(int factor, CounterCreator counterCreator) {
        Node one = database.getNodeById(1);
        Node two = database.getNodeById(2);

        //Node one incoming

        Assert.assertEquals(10 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).countLiterally(one));

        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 1).count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 1).countLiterally(one));
        Assert.assertEquals(2 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 2).count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 2).countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 3).count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 3).countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 4).count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 4).countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 5).count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 5).countLiterally(one));
        Assert.assertEquals(7 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 7).count(one));
        Assert.assertEquals(7 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 7).countLiterally(one));

        Assert.assertEquals(3 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V1").count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V1").countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V2").count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V2").countLiterally(one));

        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K2, "V1").count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K2, "V1").countLiterally(one));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K2, "V2").count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K2, "V2").countLiterally(one));

        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V2").count(one));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V2").countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V1").count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V1").countLiterally(one));

        Assert.assertEquals(2 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K1, "V1").count(one));
        Assert.assertEquals(2 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K1, "V1").countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K2, "V2").count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K2, "V2").countLiterally(one));

        //Node one outgoing

        Assert.assertEquals(14 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).countLiterally(one));

        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.TWO, OUTGOING).count(one));

        Assert.assertEquals(2 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 1).count(one));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 1).countLiterally(one));
        Assert.assertEquals(2 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 2).count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 2).countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 3).count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 3).countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 4).count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 4).countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 5).count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 5).countLiterally(one));
        Assert.assertEquals(7 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 7).count(one));
        Assert.assertEquals(7 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 7).countLiterally(one));

        Assert.assertEquals(6 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K1, "V1").count(one));
        Assert.assertEquals(3 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K1, "V1").countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K1, "V2").count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K1, "V2").countLiterally(one));

        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K2, "V1").count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K2, "V1").countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K2, "V2").count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K2, "V2").countLiterally(one));

        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V2").count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V2").countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V1").count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V1").countLiterally(one));

        Assert.assertEquals(2 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K1, "V1").count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K1, "V1").countLiterally(one));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 1).with(BatchIntegrationTest.K1, "V1").count(one));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 1).with(BatchIntegrationTest.K1, "V1").countLiterally(one));

        //Node one both

        Assert.assertEquals(24 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).countLiterally(one));

        Assert.assertEquals(2 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 1).count(one));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 1).countLiterally(one));
        Assert.assertEquals(4 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 2).count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 2).countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 3).count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 3).countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 4).count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 4).countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 5).count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 5).countLiterally(one));
        Assert.assertEquals(14 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 7).count(one));
        Assert.assertEquals(14 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 7).countLiterally(one));

        Assert.assertEquals(9 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K1, "V1").count(one));
        Assert.assertEquals(3 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K1, "V1").countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K1, "V2").count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K1, "V2").countLiterally(one));

        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K2, "V1").count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K2, "V1").countLiterally(one));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K2, "V2").count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K2, "V2").countLiterally(one));

        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V2").count(one));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V2").countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V1").count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V1").countLiterally(one));

        Assert.assertEquals(4 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K1, "V1").count(one));
        Assert.assertEquals(2 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K1, "V1").countLiterally(one));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 1).with(BatchIntegrationTest.K1, "V1").count(one));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 1).with(BatchIntegrationTest.K1, "V1").countLiterally(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K2, "V2").count(one));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K2, "V2").countLiterally(one));

        //Node two outgoing

        Assert.assertEquals(3 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).countLiterally(two));

        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 1).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 1).countLiterally(two));
        Assert.assertEquals(2 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 2).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 2).countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 3).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 3).countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 4).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 4).countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 5).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 5).countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 7).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 7).countLiterally(two));

        Assert.assertEquals(3 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K1, "V1").count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K1, "V1").countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K1, "V2").count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K1, "V2").countLiterally(two));

        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K2, "V1").count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K2, "V1").countLiterally(two));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K2, "V2").count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K2, "V2").countLiterally(two));

        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V2").count(two));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V2").countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V1").count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V1").countLiterally(two));

        Assert.assertEquals(2 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K1, "V1").count(two));
        Assert.assertEquals(2 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K1, "V1").countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K2, "V2").count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, OUTGOING).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K2, "V2").countLiterally(two));

        //Node two incoming

        Assert.assertEquals(7 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).countLiterally(two));

        Assert.assertEquals(2 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 1).count(two));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 1).countLiterally(two));
        Assert.assertEquals(2 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 2).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 2).countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 3).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 3).countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 4).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 4).countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 5).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 5).countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 7).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 7).countLiterally(two));

        Assert.assertEquals(6 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V1").count(two));
        Assert.assertEquals(3 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V1").countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V2").count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V2").countLiterally(two));

        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K2, "V1").count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K2, "V1").countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K2, "V2").count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K2, "V2").countLiterally(two));

        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V2").count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V2").countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V1").count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V1").countLiterally(two));

        Assert.assertEquals(2 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K1, "V1").count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K1, "V1").countLiterally(two));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 1).with(BatchIntegrationTest.K1, "V1").count(two));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 1).with(BatchIntegrationTest.K1, "V1").countLiterally(two));

        //Node two both

        Assert.assertEquals(10 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).countLiterally(two));

        Assert.assertEquals(2 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 1).count(two));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 1).countLiterally(two));
        Assert.assertEquals(4 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 2).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 2).countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 3).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 3).countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 4).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 4).countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 5).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 5).countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 7).count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 7).countLiterally(two));

        Assert.assertEquals(9 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K1, "V1").count(two));
        Assert.assertEquals(3 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K1, "V1").countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K1, "V2").count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K1, "V2").countLiterally(two));

        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K2, "V1").count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K2, "V1").countLiterally(two));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K2, "V2").count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K2, "V2").countLiterally(two));

        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V2").count(two));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V2").countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V1").count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K1, "V1").with(BatchIntegrationTest.K2, "V1").countLiterally(two));

        Assert.assertEquals(4 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K1, "V1").count(two));
        Assert.assertEquals(2 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K1, "V1").countLiterally(two));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 1).with(BatchIntegrationTest.K1, "V1").count(two));
        Assert.assertEquals(1 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 1).with(BatchIntegrationTest.K1, "V1").countLiterally(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K2, "V2").count(two));
        Assert.assertEquals(0 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K2, "V2").countLiterally(two));
    }

    private void verifyCompactedCounts(int factor, CounterCreator counterCreator) {
        Node one = database.getNodeById(1);
        Node two = database.getNodeById(2);

        //Node one incoming

        Assert.assertEquals(3 * factor, counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).count(one));

        try {
            counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).countLiterally(one);
            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 1).count(one);
            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 1).countLiterally(one);
            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 2).count(one);

            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 2).countLiterally(one);

            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 7).count(one);

            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 7).countLiterally(one);
            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V1").count(one);
            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K1, "V1").countLiterally(one);
            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K2, "V1").count(one);
            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.K2, "V1").countLiterally(one);
            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K1, "V1").count(one);
            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K1, "V1").countLiterally(one);
            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K2, "V2").count(one);
            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(RelationshipTypes.ONE, INCOMING).with(BatchIntegrationTest.WEIGHT, 2).with(BatchIntegrationTest.K2, "V2").countLiterally(one);
            fail();
        } catch (UnableToCountException e) {
        }

        //Node one both

        Assert.assertEquals(10 * factor, counterCreator.createCounter(RelationshipTypes.ONE, BOTH).count(one));

        try {
            counterCreator.createCounter(RelationshipTypes.ONE, BOTH).countLiterally(one);
            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 1).count(one);
            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 1).countLiterally(one);
            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 7).count(one);
            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.WEIGHT, 7).countLiterally(one);
            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K1, "V1").count(one);
            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(RelationshipTypes.ONE, BOTH).with(BatchIntegrationTest.K1, "V1").countLiterally(one);
            fail();
        } catch (UnableToCountException e) {
        }
    }

    interface CounterCreator {
        FullRelationshipCounter createCounter(RelationshipType type, Direction direction);
    }
}
