package com.holdenkarau.spark.testing;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class SampleJavaDataFrameTest extends JavaDataFrameSuiteBase implements Serializable {
    @Test
    public void testEqualDataFrameWithItSelf() {
        List<BasicMagic> list = Arrays.asList(new BasicMagic("holden", 30),
                                         new BasicMagic("mahmoud", 23));

        DataFrame personsDataFrame = toDF(list);
        equalDataFrames(personsDataFrame, personsDataFrame);

        List<BasicMagic> emptyList = Arrays.asList();
        DataFrame emptyDataFrame = toDF(emptyList);
        equalDataFrames(emptyDataFrame, emptyDataFrame);
    }

    @Test
    public void testEqualDataFrames() {
        List<BasicMagic> magics1 = Arrays.asList(new BasicMagic("holden", 30),
                                            new BasicMagic("mahmoud", 23));

        List<BasicMagic> magics2 = Arrays.asList(new BasicMagic("holden", 30),
                                            new BasicMagic("mahmoud", 23));

        equalDataFrames(toDF(magics1), toDF(magics2));
    }

    @Test (expected = java.lang.AssertionError.class)
    public void testNotEqualInteger() {
        List<BasicMagic> magics1 = Arrays.asList(new BasicMagic("mahmoud", 20),
                                            new BasicMagic("Holden", 25));

        List<BasicMagic> magics2 = Arrays.asList(new BasicMagic("mahmoud", 40),
                                            new BasicMagic("Holden", 25));

        equalDataFrames(toDF(magics1), toDF(magics2));
    }

    @Test
    public void testApproximateEqual() {
        List<BasicMagic> magics1 = Arrays.asList(new BasicMagic("Holden", 10.0),
                                            new BasicMagic("Mahmoud", 9.9));

        List<BasicMagic> magics2 = Arrays.asList(new BasicMagic("Holden", 10.1),
                                            new BasicMagic("Mahmoud", 10.0));

        approxEqualDataFrames(toDF(magics1), toDF(magics2), 0.1);
    }

    @Test (expected = java.lang.AssertionError.class)
    public void testApproximateNotEqual() {
        List<BasicMagic> magics1 = Arrays.asList(new BasicMagic("Holden", 10.0),
                                            new BasicMagic("Mahmoud", 9.9));

        List<BasicMagic> magics2 = Arrays.asList(new BasicMagic("Holden", 10.2),
                                            new BasicMagic("Mahmoud", 10.0));

        approxEqualDataFrames(toDF(magics1), toDF(magics2), 0.1);
    }

    @Test
    public void testApproximateEqualRows() {
        List<BasicMagic> magics = Arrays.asList(new BasicMagic("Holden", 10.0),
                                           new BasicMagic("Mahmoud", 9.9));

        Row row1 = toDF(magics).collect()[0];
        Row row2 = toDF(magics).collect()[1];

        assertTrue(approxEquals(row1, row1, 0));
        assertFalse(approxEquals(row1, row2, 0));
    }

    private DataFrame toDF(List<BasicMagic> list) {
        JavaRDD<BasicMagic> rdd = jsc().parallelize(list);
        return sqlContext().createDataFrame(rdd, BasicMagic.class);
    }

}
