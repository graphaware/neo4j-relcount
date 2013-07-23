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

package com.graphaware.neo4j.relcount.full.logic;

import com.graphaware.neo4j.tx.single.KeepCalmAndCarryOn;
import com.graphaware.neo4j.tx.single.SimpleTransactionExecutor;
import com.graphaware.neo4j.tx.single.VoidReturningCallback;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * {@link ThresholdBasedRelationshipCountCompactor} that performs compaction in a separate thread.
 */
public class AsyncThresholdBasedRelationshipCountCompactor extends ThresholdBasedRelationshipCountCompactor {

    private final ExecutorService executor;

    /**
     * Construct a new compactor.
     *
     * @param compactionThreshold compaction threshold, see class javadoc.
     * @param countCache          relationship count cache.
     */
    public AsyncThresholdBasedRelationshipCountCompactor(int compactionThreshold, FullRelationshipCountCache countCache) {
        super(compactionThreshold, countCache);
        executor = Executors.newFixedThreadPool(1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void compactRelationshipCounts(final Node node) {
        executor.submit(new Runnable() {
            @Override
            public void run() {
                new SimpleTransactionExecutor(node.getGraphDatabase()).executeInTransaction(new VoidReturningCallback() {
                    @Override
                    protected void doInTx(GraphDatabaseService database) {
                        AsyncThresholdBasedRelationshipCountCompactor.super.compactRelationshipCounts(node);
                    }
                }, KeepCalmAndCarryOn.getInstance());
            }
        });
    }
}
