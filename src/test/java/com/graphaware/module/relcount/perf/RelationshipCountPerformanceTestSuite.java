package com.graphaware.module.relcount.perf;

import com.graphaware.test.performance.PerformanceTest;
import com.graphaware.test.performance.PerformanceTestSuite;
import org.junit.Ignore;

/**
 * Performance test suite for relationship count module.
 */
@Ignore
public class RelationshipCountPerformanceTestSuite extends PerformanceTestSuite {

    @Override
    protected PerformanceTest[] getPerfTests() {
        return new PerformanceTest[]{
                new CreateRelationships(),
                new CountRelationships()
        };
    }
}
