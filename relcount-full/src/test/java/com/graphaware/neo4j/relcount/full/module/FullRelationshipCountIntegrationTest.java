package com.graphaware.neo4j.relcount.full.module;

import com.graphaware.neo4j.framework.GraphAwareFramework;
import com.graphaware.neo4j.framework.strategy.IncludeAllNodeProperties;
import com.graphaware.neo4j.framework.strategy.IncludeAllNodes;
import com.graphaware.neo4j.relcount.common.IntegrationTest;
import com.graphaware.neo4j.relcount.common.counter.UnableToCountException;
import com.graphaware.neo4j.relcount.full.counter.FullCachedRelationshipCounter;
import com.graphaware.neo4j.relcount.full.counter.FullFallingBackRelationshipCounter;
import com.graphaware.neo4j.relcount.full.counter.FullNaiveRelationshipCounter;
import com.graphaware.neo4j.relcount.full.counter.FullRelationshipCounter;
import com.graphaware.neo4j.relcount.full.strategy.RelationshipCountStrategiesImpl;
import com.graphaware.neo4j.relcount.full.strategy.RelationshipPropertiesExtractionStrategy;
import com.graphaware.neo4j.relcount.full.strategy.RelationshipWeighingStrategy;
import com.graphaware.neo4j.tx.event.strategy.RelationshipInclusionStrategy;
import com.graphaware.neo4j.tx.event.strategy.RelationshipPropertyInclusionStrategy;
import com.graphaware.neo4j.tx.single.SimpleTransactionExecutor;
import com.graphaware.neo4j.tx.single.VoidReturningCallback;
import org.junit.Test;
import org.neo4j.graphdb.*;

import java.util.Map;

import static com.graphaware.neo4j.relcount.common.IntegrationTest.RelationshipTypes.ONE;
import static com.graphaware.neo4j.relcount.common.IntegrationTest.RelationshipTypes.TWO;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.Direction.*;

/**
 * Integration test for full relationship counting.
 */
@SuppressWarnings("PointlessArithmeticExpression")
public class FullRelationshipCountIntegrationTest extends IntegrationTest {

    @Test
    public void noFramework() {
        setUpTwoNodes();
        simulateUsage();

        verifyCounts(1, defaultNaiveCounterCreator());
        verifyCounts(0, defaultCachedCounterCreator());
        verifyCounts(0, defaultFallbackCounterCreator());
    }

    @Test
    public void noFramework2() {
        setUpTwoNodes();
        simulateUsage();
        simulateUsage();

        verifyCounts(2, defaultNaiveCounterCreator());
        verifyCounts(0, defaultCachedCounterCreator());
        verifyCounts(0, defaultFallbackCounterCreator());
    }

    @Test
    public void cachedCountsCanBeRebuilt() {
        GraphAwareFramework framework = new GraphAwareFramework(database);
        final FullRelationshipCountModule module = new FullRelationshipCountModule();
        framework.registerModule(module);
        framework.start();

        setUpTwoNodes();
        simulateUsage();

        module.reinitialize(database);

        verifyCounts(1, naiveCounterCreator(module));
        verifyCounts(1, cachedCounterCreator(module));
        verifyCounts(1, fallbackCounterCreator(module));
    }

    @Test
    public void defaultFrameworkOnNewDatabase() {
        GraphAwareFramework framework = new GraphAwareFramework(database);
        final FullRelationshipCountModule module = new FullRelationshipCountModule();
        framework.registerModule(module);
        framework.start();

        setUpTwoNodes();
        simulateUsage();

        verifyCounts(1, naiveCounterCreator(module));
        verifyCounts(1, cachedCounterCreator(module));
        verifyCounts(1, fallbackCounterCreator(module));
    }

    @Test
    public void defaultFrameworkOnExistingDatabase() {
        setUpTwoNodes();
        simulateUsage();

        GraphAwareFramework framework = new GraphAwareFramework(database);
        final FullRelationshipCountModule module = new FullRelationshipCountModule();
        framework.registerModule(module);
        framework.start();

        verifyCounts(1, naiveCounterCreator(module));
        verifyCounts(1, cachedCounterCreator(module));
        verifyCounts(1, fallbackCounterCreator(module));
    }

    @Test
    public void customFrameworkOnNewDatabase() {
        GraphAwareFramework framework = new GraphAwareFramework(database, new CustomConfig());
        final FullRelationshipCountModule module = new FullRelationshipCountModule();
        framework.registerModule(module);
        framework.start();

        setUpTwoNodes();
        simulateUsage();

        verifyCounts(1, naiveCounterCreator(module));
        verifyCounts(1, cachedCounterCreator(module));
        verifyCounts(1, fallbackCounterCreator(module));
    }

    @Test
    public void customFrameworkOnExistingDatabase() {
        setUpTwoNodes();
        simulateUsage();

        GraphAwareFramework framework = new GraphAwareFramework(database, new CustomConfig());
        final FullRelationshipCountModule module = new FullRelationshipCountModule();
        framework.registerModule(module);
        framework.start();

        verifyCounts(1, naiveCounterCreator(module));
        verifyCounts(1, cachedCounterCreator(module));
        verifyCounts(1, fallbackCounterCreator(module));
    }

    @Test
    public void weightedRelationships() {
        for (int numberOfRounds = 1; numberOfRounds <= 10; numberOfRounds++) {
            setUp();

            GraphAwareFramework framework = new GraphAwareFramework(database, new CustomConfig());
            final FullRelationshipCountModule module = new FullRelationshipCountModule(
                    RelationshipCountStrategiesImpl.defaultStrategies()
                            .with(new RelationshipWeighingStrategy() {
                                @Override
                                public int getRelationshipWeight(Relationship relationship, Node pointOfView) {
                                    return (int) relationship.getProperty(WEIGHT, 1);
                                }
                            }));

            framework.registerModule(module);
            framework.start();

            setUpTwoNodes();

            for (int i = 0; i < numberOfRounds; i++) {
                simulateUsage();
            }

            verifyWeightedCounts(numberOfRounds, naiveCounterCreator(module));
            verifyWeightedCounts(numberOfRounds, cachedCounterCreator(module));
            verifyWeightedCounts(numberOfRounds, fallbackCounterCreator(module));

            tearDown();
        }
    }

    @Test
    public void defaultStrategiesWithLowerThreshold() {
        GraphAwareFramework framework = new GraphAwareFramework(database, new CustomConfig());
        final FullRelationshipCountModule module = new FullRelationshipCountModule(
                RelationshipCountStrategiesImpl.defaultStrategies().with(5)
        );
        framework.registerModule(module);
        framework.start();

        setUpTwoNodes();
        simulateUsage();

        verifyCounts(1, naiveCounterCreator(module));
        verifyCompactedCounts(1, cachedCounterCreator(module));
        verifyCounts(1, fallbackCounterCreator(module));
    }

    @Test
    public void defaultStrategiesWithLowerThreshold2() {
        GraphAwareFramework framework = new GraphAwareFramework(database);
        final FullRelationshipCountModule module = new FullRelationshipCountModule(
                RelationshipCountStrategiesImpl.defaultStrategies().with(5)
        );
        framework.registerModule(module);
        framework.start();

        setUpTwoNodes();
        simulateUsage();
        simulateUsage();

        verifyCounts(2, naiveCounterCreator(module));
        verifyCompactedCounts(2, cachedCounterCreator(module));
        verifyCounts(2, fallbackCounterCreator(module));
    }

    @Test
    public void defaultStrategiesWithLowerThreshold3() {
        GraphAwareFramework framework = new GraphAwareFramework(database, new CustomConfig());
        final FullRelationshipCountModule module = new FullRelationshipCountModule(
                RelationshipCountStrategiesImpl.defaultStrategies().with(4)
        );
        framework.registerModule(module);
        framework.start();

        setUpTwoNodes();
        simulateUsage();

        try {
            cachedCounterCreator(module).createCounter(ONE, OUTGOING).with(WEIGHT, 2).with(TIMESTAMP, "123").with(K1, "V1").count(database.getNodeById(1));
            fail();
        } catch (UnableToCountException e) {
            //OK
        }
    }

    @Test
    public void defaultStrategiesWithLowerThreshold20() {
        for (int threshold = 3; threshold <= 20; threshold++) {
            setUp();

            GraphAwareFramework framework = new GraphAwareFramework(database, new CustomConfig());
            final FullRelationshipCountModule module = new FullRelationshipCountModule(
                    RelationshipCountStrategiesImpl.defaultStrategies().with(threshold)
            );
            framework.registerModule(module);
            framework.start();

            setUpTwoNodes();
            simulateUsage();
            simulateUsage();
            simulateUsage();

            verifyCounts(3, fallbackCounterCreator(module));
            verifyCounts(3, naiveCounterCreator(module));

            tearDown();
        }
    }

    @Test
    public void weightedRelationshipsWithCompaction() {
        GraphAwareFramework framework = new GraphAwareFramework(database, new CustomConfig());
        final FullRelationshipCountModule module = new FullRelationshipCountModule(
                RelationshipCountStrategiesImpl.defaultStrategies()
                        .with(new RelationshipWeighingStrategy() {
                            @Override
                            public int getRelationshipWeight(Relationship relationship, Node pointOfView) {
                                return (int) relationship.getProperty(WEIGHT, 1);
                            }
                        })
                        .with(10));

        framework.registerModule(module);
        framework.start();

        setUpTwoNodes();
        simulateUsage();
        simulateUsage();
        simulateUsage();
        simulateUsage();

        verifyWeightedCounts(4, fallbackCounterCreator(module));
        verifyWeightedCounts(4, naiveCounterCreator(module));
    }

    @Test
    public void twoSimultaneousModules() {
        GraphAwareFramework framework = new GraphAwareFramework(database);
        final FullRelationshipCountModule module1 = new FullRelationshipCountModule("M1", RelationshipCountStrategiesImpl.defaultStrategies());
        final FullRelationshipCountModule module2 = new FullRelationshipCountModule("M2",
                RelationshipCountStrategiesImpl.defaultStrategies()
                        .with(new RelationshipWeighingStrategy() {
                            @Override
                            public int getRelationshipWeight(Relationship relationship, Node pointOfView) {
                                return (int) relationship.getProperty(WEIGHT, 1);
                            }
                        }));

        framework.registerModule(module1);
        framework.registerModule(module2);
        framework.start();

        setUpTwoNodes();
        simulateUsage();
        simulateUsage();

        verifyCounts(2, naiveCounterCreator(module1));
        verifyCounts(2, cachedCounterCreator(module1));
        verifyCounts(2, fallbackCounterCreator(module1));

        verifyWeightedCounts(2, naiveCounterCreator(module2));
        verifyWeightedCounts(2, cachedCounterCreator(module2));
        verifyWeightedCounts(2, fallbackCounterCreator(module2));
    }

    @Test
    public void customRelationshipPropertiesExtractionStrategy() {
        GraphAwareFramework framework = new GraphAwareFramework(database, new CustomConfig());
        final FullRelationshipCountModule module = new FullRelationshipCountModule(
                RelationshipCountStrategiesImpl.defaultStrategies()
                        .with(new RelationshipPropertiesExtractionStrategy.OtherNodeIncludingAdapter() {
                            @Override
                            protected Map<String, String> extractProperties(Map<String, String> properties, Node otherNode) {
                                properties.put("otherNodeName", otherNode.getProperty(NAME, "UNKNOWN").toString());
                                return properties;
                            }
                        })
                        .with(IncludeAllNodes.getInstance())
                        .with(IncludeAllNodeProperties.getInstance()));

        framework.registerModule(module);
        framework.start();

        setUpTwoNodes();
        simulateUsage();
        simulateUsage();

        assertEquals(4, naiveCounterCreator(module).createCounter(ONE, INCOMING).with(K1, "V1").with("otherNodeName", "Two").count(database.getNodeById(1)));
        assertEquals(4, fallbackCounterCreator(module).createCounter(ONE, INCOMING).with(K1, "V1").with("otherNodeName", "Two").count(database.getNodeById(1)));
        assertEquals(4, cachedCounterCreator(module).createCounter(ONE, INCOMING).with(K1, "V1").with("otherNodeName", "Two").count(database.getNodeById(1)));
    }

    @Test
    public void customRelationshipInclusionStrategy() {
        GraphAwareFramework framework = new GraphAwareFramework(database, new CustomConfig());
        final FullRelationshipCountModule module = new FullRelationshipCountModule(
                RelationshipCountStrategiesImpl.defaultStrategies()
                        .with(new RelationshipInclusionStrategy() {
                            @Override
                            public boolean include(Relationship relationship) {
                                return !relationship.isType(TWO);
                            }
                        }));

        framework.registerModule(module);
        framework.start();

        setUpTwoNodes();
        simulateUsage();
        simulateUsage();

        //naive doesn't care about this strategy
        assertEquals(2, naiveCounterCreator(module).createCounter(TWO, OUTGOING).count(database.getNodeById(1)));
        assertEquals(0, fallbackCounterCreator(module).createCounter(TWO, OUTGOING).count(database.getNodeById(1)));
        assertEquals(0, cachedCounterCreator(module).createCounter(TWO, OUTGOING).count(database.getNodeById(1)));
    }

    @Test
    public void customRelationshipPropertiesInclusionStrategy() {
        GraphAwareFramework framework = new GraphAwareFramework(database, new CustomConfig());
        final FullRelationshipCountModule module = new FullRelationshipCountModule(
                RelationshipCountStrategiesImpl.defaultStrategies()
                        .with(new RelationshipPropertyInclusionStrategy() {
                            @Override
                            public boolean include(String key, Relationship propertyContainer) {
                                return !WEIGHT.equals(key);
                            }
                        }));

        framework.registerModule(module);
        framework.start();

        setUpTwoNodes();
        simulateUsage();
        simulateUsage();

        //naive doesn't care about this strategy
        assertEquals(2, naiveCounterCreator(module).createCounter(ONE, INCOMING).with(WEIGHT, 7).count(database.getNodeById(1)));
        assertEquals(2, naiveCounterCreator(module).createCounter(ONE, INCOMING).with(WEIGHT, 7).countLiterally(database.getNodeById(1)));
        assertEquals(0, cachedCounterCreator(module).createCounter(ONE, INCOMING).with(WEIGHT, 7).count(database.getNodeById(1)));
        assertEquals(0, cachedCounterCreator(module).createCounter(ONE, INCOMING).with(WEIGHT, 7).countLiterally(database.getNodeById(1)));
        assertEquals(0, fallbackCounterCreator(module).createCounter(ONE, INCOMING).with(WEIGHT, 7).count(database.getNodeById(1)));
        assertEquals(0, fallbackCounterCreator(module).createCounter(ONE, INCOMING).with(WEIGHT, 7).countLiterally(database.getNodeById(1)));
    }

    @Test
    public void batchTest() {
        GraphAwareFramework framework = new GraphAwareFramework(database, new CustomConfig());
        final FullRelationshipCountModule module = new FullRelationshipCountModule();
        framework.registerModule(module);
        framework.start();

        setUpTwoNodes();

        new SimpleTransactionExecutor(database).executeInTransaction(new VoidReturningCallback() {
            @Override
            protected void doInTx(GraphDatabaseService database) {
                for (int i = 0; i < 100; i++) {
                    simulateUsage();
                }
            }
        });

        verifyCounts(100, naiveCounterCreator(module));
        verifyCounts(100, cachedCounterCreator(module));
        verifyCounts(100, fallbackCounterCreator(module));
    }

    @Test
    public void batchTestWithMultipleModulesAndLowerThreshold() {
        GraphAwareFramework framework = new GraphAwareFramework(database, new CustomConfig());
        final FullRelationshipCountModule module1 = new FullRelationshipCountModule("M1", RelationshipCountStrategiesImpl.defaultStrategies().with(5));
        final FullRelationshipCountModule module2 = new FullRelationshipCountModule("M2", RelationshipCountStrategiesImpl.defaultStrategies().with(5));
        framework.registerModule(module1);
        framework.registerModule(module2);
        framework.start();

        setUpTwoNodes();

        new SimpleTransactionExecutor(database).executeInTransaction(new VoidReturningCallback() {
            @Override
            protected void doInTx(GraphDatabaseService database) {
                for (int i = 0; i < 20; i++) {
                    simulateUsage();
                }
            }
        });

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

        assertEquals(3 * factor, counterCreator.createCounter(ONE, INCOMING).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).countLiterally(one));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 1).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 1).countLiterally(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 3).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 3).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 4).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 4).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 5).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 5).countLiterally(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 7).count(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 7).countLiterally(one));

        assertEquals(2 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V2").countLiterally(one));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K2, "V1").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K2, "V1").countLiterally(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(K2, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K2, "V2").countLiterally(one));

        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").with(K2, "V2").count(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").with(K2, "V2").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").with(K2, "V1").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").with(K2, "V1").countLiterally(one));

        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).with(K1, "V1").count(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).with(K1, "V1").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).with(K2, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).with(K2, "V2").countLiterally(one));

        //Node one outgoing

        assertEquals(7 * factor, counterCreator.createCounter(ONE, OUTGOING).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).countLiterally(one));

        assertEquals(2 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 1).count(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 1).countLiterally(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 3).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 3).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 4).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 4).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 5).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 5).countLiterally(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 7).count(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 7).countLiterally(one));

        assertEquals(5 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").count(one));
        assertEquals(3 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V2").countLiterally(one));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K2, "V1").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K2, "V1").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K2, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K2, "V2").countLiterally(one));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").with(K2, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").with(K2, "V2").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").with(K2, "V1").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").with(K2, "V1").countLiterally(one));

        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).with(K1, "V1").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).with(K1, "V1").countLiterally(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 1).with(K1, "V1").count(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 1).with(K1, "V1").countLiterally(one));

        //Node one both

        assertEquals(10 * factor, counterCreator.createCounter(ONE, BOTH).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).countLiterally(one));

        assertEquals(2 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 1).count(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 1).countLiterally(one));
        assertEquals(2 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 3).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 3).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 4).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 4).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 5).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 5).countLiterally(one));
        assertEquals(2 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 7).count(one));
        assertEquals(2 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 7).countLiterally(one));

        assertEquals(7 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").count(one));
        assertEquals(3 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V2").countLiterally(one));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K2, "V1").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K2, "V1").countLiterally(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(K2, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K2, "V2").countLiterally(one));

        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").with(K2, "V2").count(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").with(K2, "V2").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").with(K2, "V1").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").with(K2, "V1").countLiterally(one));

        assertEquals(2 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).with(K1, "V1").count(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).with(K1, "V1").countLiterally(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 1).with(K1, "V1").count(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 1).with(K1, "V1").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).with(K2, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).with(K2, "V2").countLiterally(one));

        //Node two outgoing

        assertEquals(2 * factor, counterCreator.createCounter(ONE, OUTGOING).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).countLiterally(two));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 1).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 1).countLiterally(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 3).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 3).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 4).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 4).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 5).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 5).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 7).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 7).countLiterally(two));

        assertEquals(2 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V2").countLiterally(two));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K2, "V1").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K2, "V1").countLiterally(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K2, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K2, "V2").countLiterally(two));

        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").with(K2, "V2").count(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").with(K2, "V2").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").with(K2, "V1").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").with(K2, "V1").countLiterally(two));

        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).with(K1, "V1").count(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).with(K1, "V1").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).with(K2, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).with(K2, "V2").countLiterally(two));

        //Node two incoming

        assertEquals(6 * factor, counterCreator.createCounter(ONE, INCOMING).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).countLiterally(two));

        assertEquals(2 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 1).count(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 1).countLiterally(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 3).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 3).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 4).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 4).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 5).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 5).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 7).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 7).countLiterally(two));

        assertEquals(5 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").count(two));
        assertEquals(3 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V2").countLiterally(two));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K2, "V1").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K2, "V1").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K2, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K2, "V2").countLiterally(two));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").with(K2, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").with(K2, "V2").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").with(K2, "V1").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").with(K2, "V1").countLiterally(two));

        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).with(K1, "V1").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).with(K1, "V1").countLiterally(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 1).with(K1, "V1").count(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 1).with(K1, "V1").countLiterally(two));

        //Node two both

        assertEquals(8 * factor, counterCreator.createCounter(ONE, BOTH).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).countLiterally(two));

        assertEquals(2 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 1).count(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 1).countLiterally(two));
        assertEquals(2 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 3).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 3).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 4).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 4).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 5).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 5).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 7).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 7).countLiterally(two));

        assertEquals(7 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").count(two));
        assertEquals(3 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V2").countLiterally(two));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K2, "V1").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K2, "V1").countLiterally(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(K2, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K2, "V2").countLiterally(two));

        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").with(K2, "V2").count(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").with(K2, "V2").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").with(K2, "V1").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").with(K2, "V1").countLiterally(two));

        assertEquals(2 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).with(K1, "V1").count(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).with(K1, "V1").countLiterally(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 1).with(K1, "V1").count(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 1).with(K1, "V1").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).with(K2, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).with(K2, "V2").countLiterally(two));
    }

    private void verifyWeightedCounts(int factor, CounterCreator counterCreator) {
        Node one = database.getNodeById(1);
        Node two = database.getNodeById(2);

        //Node one incoming

        assertEquals(10 * factor, counterCreator.createCounter(ONE, INCOMING).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).countLiterally(one));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 1).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 1).countLiterally(one));
        assertEquals(2 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 3).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 3).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 4).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 4).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 5).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 5).countLiterally(one));
        assertEquals(7 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 7).count(one));
        assertEquals(7 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 7).countLiterally(one));

        assertEquals(3 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V2").countLiterally(one));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K2, "V1").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K2, "V1").countLiterally(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(K2, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K2, "V2").countLiterally(one));

        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").with(K2, "V2").count(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").with(K2, "V2").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").with(K2, "V1").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").with(K2, "V1").countLiterally(one));

        assertEquals(2 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).with(K1, "V1").count(one));
        assertEquals(2 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).with(K1, "V1").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).with(K2, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).with(K2, "V2").countLiterally(one));

        //Node one outgoing

        assertEquals(14 * factor, counterCreator.createCounter(ONE, OUTGOING).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).countLiterally(one));

        assertEquals(1 * factor, counterCreator.createCounter(TWO, OUTGOING).count(one));

        assertEquals(2 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 1).count(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 1).countLiterally(one));
        assertEquals(2 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 3).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 3).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 4).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 4).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 5).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 5).countLiterally(one));
        assertEquals(7 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 7).count(one));
        assertEquals(7 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 7).countLiterally(one));

        assertEquals(6 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").count(one));
        assertEquals(3 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V2").countLiterally(one));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K2, "V1").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K2, "V1").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K2, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K2, "V2").countLiterally(one));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").with(K2, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").with(K2, "V2").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").with(K2, "V1").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").with(K2, "V1").countLiterally(one));

        assertEquals(2 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).with(K1, "V1").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).with(K1, "V1").countLiterally(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 1).with(K1, "V1").count(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 1).with(K1, "V1").countLiterally(one));

        //Node one both

        assertEquals(24 * factor, counterCreator.createCounter(ONE, BOTH).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).countLiterally(one));

        assertEquals(2 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 1).count(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 1).countLiterally(one));
        assertEquals(4 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 3).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 3).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 4).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 4).countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 5).count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 5).countLiterally(one));
        assertEquals(14 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 7).count(one));
        assertEquals(14 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 7).countLiterally(one));

        assertEquals(9 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").count(one));
        assertEquals(3 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V2").countLiterally(one));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K2, "V1").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K2, "V1").countLiterally(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(K2, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K2, "V2").countLiterally(one));

        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").with(K2, "V2").count(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").with(K2, "V2").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").with(K2, "V1").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").with(K2, "V1").countLiterally(one));

        assertEquals(4 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).with(K1, "V1").count(one));
        assertEquals(2 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).with(K1, "V1").countLiterally(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 1).with(K1, "V1").count(one));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 1).with(K1, "V1").countLiterally(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).with(K2, "V2").count(one));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).with(K2, "V2").countLiterally(one));

        //Node two outgoing

        assertEquals(3 * factor, counterCreator.createCounter(ONE, OUTGOING).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).countLiterally(two));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 1).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 1).countLiterally(two));
        assertEquals(2 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 3).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 3).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 4).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 4).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 5).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 5).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 7).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 7).countLiterally(two));

        assertEquals(3 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V2").countLiterally(two));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K2, "V1").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K2, "V1").countLiterally(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K2, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K2, "V2").countLiterally(two));

        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").with(K2, "V2").count(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").with(K2, "V2").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").with(K2, "V1").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(K1, "V1").with(K2, "V1").countLiterally(two));

        assertEquals(2 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).with(K1, "V1").count(two));
        assertEquals(2 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).with(K1, "V1").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).with(K2, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, OUTGOING).with(WEIGHT, 2).with(K2, "V2").countLiterally(two));

        //Node two incoming

        assertEquals(7 * factor, counterCreator.createCounter(ONE, INCOMING).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).countLiterally(two));

        assertEquals(2 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 1).count(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 1).countLiterally(two));
        assertEquals(2 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 3).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 3).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 4).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 4).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 5).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 5).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 7).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 7).countLiterally(two));

        assertEquals(6 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").count(two));
        assertEquals(3 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V2").countLiterally(two));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K2, "V1").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K2, "V1").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K2, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K2, "V2").countLiterally(two));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").with(K2, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").with(K2, "V2").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").with(K2, "V1").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").with(K2, "V1").countLiterally(two));

        assertEquals(2 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).with(K1, "V1").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).with(K1, "V1").countLiterally(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 1).with(K1, "V1").count(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 1).with(K1, "V1").countLiterally(two));

        //Node two both

        assertEquals(10 * factor, counterCreator.createCounter(ONE, BOTH).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).countLiterally(two));

        assertEquals(2 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 1).count(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 1).countLiterally(two));
        assertEquals(4 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 3).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 3).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 4).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 4).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 5).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 5).countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 7).count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 7).countLiterally(two));

        assertEquals(9 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").count(two));
        assertEquals(3 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V2").countLiterally(two));

        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K2, "V1").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K2, "V1").countLiterally(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(K2, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K2, "V2").countLiterally(two));

        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").with(K2, "V2").count(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").with(K2, "V2").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").with(K2, "V1").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(K1, "V1").with(K2, "V1").countLiterally(two));

        assertEquals(4 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).with(K1, "V1").count(two));
        assertEquals(2 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).with(K1, "V1").countLiterally(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 1).with(K1, "V1").count(two));
        assertEquals(1 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 1).with(K1, "V1").countLiterally(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).with(K2, "V2").count(two));
        assertEquals(0 * factor, counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 2).with(K2, "V2").countLiterally(two));
    }

    private void verifyCompactedCounts(int factor, CounterCreator counterCreator) {
        Node one = database.getNodeById(1);
        Node two = database.getNodeById(2);

        //Node one incoming

        assertEquals(3 * factor, counterCreator.createCounter(ONE, INCOMING).count(one));

        try {
            counterCreator.createCounter(ONE, INCOMING).countLiterally(one);
            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 1).count(one);
            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 1).countLiterally(one);
            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).count(one);

            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).countLiterally(one);

            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 7).count(one);

            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 7).countLiterally(one);
            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").count(one);
            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(ONE, INCOMING).with(K1, "V1").countLiterally(one);
            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(ONE, INCOMING).with(K2, "V1").count(one);
            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(ONE, INCOMING).with(K2, "V1").countLiterally(one);
            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).with(K1, "V1").count(one);
            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).with(K1, "V1").countLiterally(one);
            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).with(K2, "V2").count(one);
            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(ONE, INCOMING).with(WEIGHT, 2).with(K2, "V2").countLiterally(one);
            fail();
        } catch (UnableToCountException e) {
        }

        //Node one both

        assertEquals(10 * factor, counterCreator.createCounter(ONE, BOTH).count(one));

        try {
            counterCreator.createCounter(ONE, BOTH).countLiterally(one);
            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 1).count(one);
            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 1).countLiterally(one);
            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 7).count(one);
            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(ONE, BOTH).with(WEIGHT, 7).countLiterally(one);
            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(ONE, BOTH).with(K1, "V1").count(one);
            fail();
        } catch (UnableToCountException e) {
        }

        try {
            counterCreator.createCounter(ONE, BOTH).with(K1, "V1").countLiterally(one);
            fail();
        } catch (UnableToCountException e) {
        }
    }

    interface CounterCreator {
        FullRelationshipCounter createCounter(RelationshipType type, Direction direction);
    }
}
