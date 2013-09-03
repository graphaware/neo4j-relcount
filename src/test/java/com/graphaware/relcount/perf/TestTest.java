package com.graphaware.relcount.perf;

import org.junit.Test;

/**
 *
 */
public class TestTest {

    @Test
    public void test() {
        for (double i = 1; i <= 4; i += 0.25) {
            System.out.println((int) (Math.pow(10, i) * 100 / 2));
        }
    }
}
