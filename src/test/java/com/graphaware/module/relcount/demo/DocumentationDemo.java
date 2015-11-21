/*
 * Copyright (c) 2013-2015 GraphAware
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

package com.graphaware.module.relcount.demo;

import com.graphaware.common.description.relationship.RelationshipDescription;
import com.graphaware.common.description.relationship.RelationshipDescriptionFactory;
import com.graphaware.common.policy.RelationshipInclusionPolicy;
import com.graphaware.common.policy.RelationshipPropertyInclusionPolicy;
import com.graphaware.module.relcount.RelationshipCountConfiguration;
import com.graphaware.module.relcount.RelationshipCountConfigurationImpl;
import com.graphaware.module.relcount.RelationshipCountModule;
import com.graphaware.module.relcount.compact.ThresholdBasedCompactionStrategy;
import com.graphaware.module.relcount.count.*;
import com.graphaware.runtime.GraphAwareRuntime;
import com.graphaware.runtime.GraphAwareRuntimeFactory;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import static com.graphaware.common.description.predicate.Predicates.equalTo;
import static com.graphaware.common.description.relationship.RelationshipDescriptionFactory.literal;
import static com.graphaware.common.description.relationship.RelationshipDescriptionFactory.wildcard;
import static com.graphaware.module.relcount.demo.BaseDocumentationDemo.Rels.FOLLOWS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

/**
 * Demonstration of different full relationship counting options.
 */
public class DocumentationDemo extends BaseDocumentationDemo {

    @Test
    public void demonstrateCachedRelationshipCounter() {
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);
        RelationshipCountModule module = new RelationshipCountModule();
        runtime.registerModule(module);
        runtime.start();

        populateDatabase();

        RelationshipCounter counter = new CachedRelationshipCounter(database);

        try (Transaction tx = database.beginTx()) {

            Node tracy = database.getNodeById(2);

            assertEquals(9, counter.count(tracy, wildcard(FOLLOWS, INCOMING)));
            assertEquals(9, counter.count(tracy, wildcard(FOLLOWS, OUTGOING)));
            assertEquals(1, counter.count(tracy, literal(FOLLOWS, INCOMING)));

            assertEquals(4, counter.count(tracy, wildcard(FOLLOWS, OUTGOING).with(STRENGTH, equalTo(1))));
            assertEquals(3, counter.count(tracy, wildcard(FOLLOWS, INCOMING).with(STRENGTH, equalTo(2))));

            tx.success();
        }
    }

    @Test
    public void demonstrateFullCachedRelationshipCounterWithCustomThreshold() {
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);

        RelationshipCountConfiguration relationshipCountConfiguration = RelationshipCountConfigurationImpl.defaultConfiguration().with(new ThresholdBasedCompactionStrategy(7));
        RelationshipCountModule module = new RelationshipCountModule(relationshipCountConfiguration);

        runtime.registerModule(module);
        runtime.start();

        populateDatabase();

        RelationshipCounter counter = new CachedRelationshipCounter(database);

        try (Transaction tx = database.beginTx()) {

            Node tracy = database.getNodeById(2);

            RelationshipDescription followers = RelationshipDescriptionFactory.wildcard(FOLLOWS, INCOMING);
            assertEquals(9, counter.count(tracy, followers));
            assertEquals(9, counter.count(tracy, wildcard(FOLLOWS, OUTGOING)));

            assertEquals(4, counter.count(tracy, wildcard(FOLLOWS, OUTGOING).with(STRENGTH, equalTo(1))));
            assertEquals(3, counter.count(tracy, wildcard(FOLLOWS, INCOMING).with(STRENGTH, equalTo(2))));

            tx.success();
        }
    }

    @Test
    public void demonstrateFullCachedRelationshipCounterWithCustomLowerThreshold() {
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);

        RelationshipCountConfiguration relationshipCountConfiguration = RelationshipCountConfigurationImpl.defaultConfiguration().with(new ThresholdBasedCompactionStrategy(3));
        RelationshipCountModule module = new RelationshipCountModule(relationshipCountConfiguration);

        runtime.registerModule(module);
        runtime.start();

        populateDatabase();

        try (Transaction tx = database.beginTx()) {

            Node tracy = database.getNodeById(2);

            RelationshipCounter counter = new CachedRelationshipCounter(database);
            assertEquals(9, counter.count(tracy, wildcard(FOLLOWS, INCOMING)));

            try {
                counter.count(tracy, wildcard(FOLLOWS, INCOMING).with(STRENGTH, equalTo(2)));
                fail();
            } catch (UnableToCountException e) {
                //ok
            }

            assertEquals(9, counter.count(tracy, wildcard(FOLLOWS, OUTGOING)));

            tx.success();
        }
    }

    @Test
    public void demonstrateFullCachedRelationshipCounterWithCustomThresholdAndWeighingStrategy() {
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);

        WeighingStrategy customWeighingStrategy = new WeighingStrategy() {
            @Override
            public int getRelationshipWeight(Relationship relationship, Node pointOfView) {
                return (int) relationship.getProperty(STRENGTH, 1);
            }
        };

        RelationshipCountConfiguration config = RelationshipCountConfigurationImpl.defaultConfiguration()
                .with(new ThresholdBasedCompactionStrategy(7))
                .with(customWeighingStrategy);

        RelationshipCountModule module = new RelationshipCountModule(config);

        runtime.registerModule(module);
        runtime.start();

        populateDatabase();

        try (Transaction tx = database.beginTx()) {

            Node tracy = database.getNodeById(2);

            RelationshipCounter counter = new CachedRelationshipCounter(database);

            assertEquals(12, counter.count(tracy, wildcard(FOLLOWS, INCOMING)));
            assertEquals(11, counter.count(tracy, wildcard(FOLLOWS, OUTGOING)));

            assertEquals(4, counter.count(tracy, wildcard(FOLLOWS, OUTGOING).with(STRENGTH, equalTo(1))));
            assertEquals(6, counter.count(tracy, wildcard(FOLLOWS, INCOMING).with(STRENGTH, equalTo(2))));


            tx.success();
        }
    }

    @Test
    public void demonstrateFullCachedRelationshipCounterWithCustomRelationshipInclusionPolicy() {
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);

        RelationshipInclusionPolicy customRelationshipInclusionPolicy = new RelationshipInclusionPolicy.Adapter() {
            @Override
            public boolean include(Relationship relationship) {
                return relationship.isType(FOLLOWS);
            }
        };

        RelationshipCountConfiguration config = RelationshipCountConfigurationImpl.defaultConfiguration()
                .with(customRelationshipInclusionPolicy);

        RelationshipCountModule module = new RelationshipCountModule(config);

        runtime.registerModule(module);
        runtime.start();

        populateDatabase();

        try (Transaction tx = database.beginTx()) {

            Node tracy = database.getNodeById(2);

            RelationshipCounter counter = new CachedRelationshipCounter(database);

            assertEquals(9, counter.count(tracy, wildcard(FOLLOWS, INCOMING)));
            assertEquals(9, counter.count(tracy, wildcard(FOLLOWS, OUTGOING)));

            assertEquals(4, counter.count(tracy, wildcard(FOLLOWS, OUTGOING).with(STRENGTH, equalTo(1))));
            assertEquals(3, counter.count(tracy, wildcard(FOLLOWS, INCOMING).with(STRENGTH, equalTo(2))));

            tx.success();
        }
    }

    @Test
    public void demonstrateFullCachedRelationshipCounterWithCustomRelationshipPropertyInclusionPolicy() {
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);

        RelationshipPropertyInclusionPolicy propertyInclusionPolicy = new RelationshipPropertyInclusionPolicy() {
            @Override
            public boolean include(String key, Relationship propertyContainer) {
                return !"timestamp".equals(key);
            }
        };

        RelationshipCountConfiguration config = RelationshipCountConfigurationImpl.defaultConfiguration()
                .with(propertyInclusionPolicy);

        RelationshipCountModule module = new RelationshipCountModule(config);

        runtime.registerModule(module);
        runtime.start();

        populateDatabase();

        try (Transaction tx = database.beginTx()) {

            Node tracy = database.getNodeById(2);

            RelationshipCounter counter = new CachedRelationshipCounter(database);

            assertEquals(9, counter.count(tracy, wildcard(FOLLOWS, INCOMING)));
            assertEquals(9, counter.count(tracy, wildcard(FOLLOWS, OUTGOING)));

            assertEquals(4, counter.count(tracy, wildcard(FOLLOWS, OUTGOING).with(STRENGTH, equalTo(1))));
            assertEquals(3, counter.count(tracy, wildcard(FOLLOWS, INCOMING).with(STRENGTH, equalTo(2))));

            tx.success();
        }
    }

    @Test
    public void demonstrateFullNaiveRelationshipCounter() {
        populateDatabase();

        try (Transaction tx = database.beginTx()) {

            Node tracy = database.getNodeById(2);

            RelationshipCounter counter = new NaiveRelationshipCounter();

            assertEquals(9, counter.count(tracy, wildcard(FOLLOWS, INCOMING)));
            assertEquals(9, counter.count(tracy, wildcard(FOLLOWS, OUTGOING)));

            assertEquals(4, counter.count(tracy, wildcard(FOLLOWS, OUTGOING).with(STRENGTH, equalTo(1))));
            assertEquals(3, counter.count(tracy, wildcard(FOLLOWS, INCOMING).with(STRENGTH, equalTo(2))));

            tx.success();
        }
    }

    @Test
    public void demonstrateFullFallingBackRelationshipCounterWithCustomLowerThreshold() {
        GraphAwareRuntime runtime = GraphAwareRuntimeFactory.createRuntime(database);

        RelationshipCountConfiguration relationshipCountConfiguration = RelationshipCountConfigurationImpl.defaultConfiguration().with(new ThresholdBasedCompactionStrategy(3));
        RelationshipCountModule module = new RelationshipCountModule(relationshipCountConfiguration);

        runtime.registerModule(module);
        runtime.start();

        populateDatabase();

        try (Transaction tx = database.beginTx()) {

            Node tracy = database.getNodeById(2);

            RelationshipCounter counter = new LegacyFallbackRelationshipCounter(database);

            assertEquals(9, counter.count(tracy, wildcard(FOLLOWS, INCOMING))); //uses cache
            assertEquals(3, counter.count(tracy, wildcard(FOLLOWS, INCOMING).with(STRENGTH, equalTo(2)))); //falls back to naive

            tx.success();
        }
    }
}
