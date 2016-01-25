package com.holdenkarau.spark.testing;

import org.junit.Test;

public class InvalidTest {

    @Test
    public void invalidTestCase() {
        throw new RuntimeException("Should not pass the tests");
    }

}
