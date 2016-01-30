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
    private static final byte[] byteArray1 = {1, 2};
    private static final byte[] byteArray2 = {100, 120};

    @Test
    public void testEqualDataFrameWithItSelf() {
        List<Magic> list = Arrays.asList(new Magic("holden", 30, byteArray1),
                                         new Magic("mahmoud", 23, byteArray1));

        DataFrame personsDataFrame = toDF(list);
        equalDataFrames(personsDataFrame, personsDataFrame);

        List<Magic> emptyList = Arrays.asList();
        DataFrame emptyDataFrame = toDF(emptyList);
        equalDataFrames(emptyDataFrame, emptyDataFrame);
    }

    @Test
    public void testEqualDataFrames() {
        List<Magic> magics1 = Arrays.asList(new Magic("holden", 30, byteArray1),
                                            new Magic("mahmoud", 23, byteArray1));

        List<Magic> magics2 = Arrays.asList(new Magic("holden", 30, byteArray1),
                                            new Magic("mahmoud", 23, byteArray1));

        equalDataFrames(toDF(magics1), toDF(magics2));
    }

    @Test (expected = java.lang.AssertionError.class)
    public void testNotEqualInteger() {
        List<Magic> magics1 = Arrays.asList(new Magic("mahmoud", 20, byteArray1),
                                            new Magic("Holden", 25, byteArray1));

        List<Magic> magics2 = Arrays.asList(new Magic("mahmoud", 40, byteArray1),
                                            new Magic("Holden", 25, byteArray1));

        equalDataFrames(toDF(magics1), toDF(magics2));
    }

    @Test (expected = java.lang.AssertionError.class)
    public void testNotEqualByteArray() {
        List<Magic> magics1 = Arrays.asList(new Magic("Holden", 20, byteArray1));
        List<Magic> magics2 = Arrays.asList(new Magic("Holden", 20, byteArray2));

        equalDataFrames(toDF(magics1), toDF(magics2));
    }

    @Test
    public void testApproximateEqual() {
        List<Magic> magics1 = Arrays.asList(new Magic("Holden", 10.0, byteArray1),
                                            new Magic("Mahmoud", 9.9, byteArray1));

        List<Magic> magics2 = Arrays.asList(new Magic("Holden", 10.1, byteArray1),
                                            new Magic("Mahmoud", 10.0, byteArray1));

        approxEqualDataFrames(toDF(magics1), toDF(magics2), 0.1);
    }

    @Test (expected = java.lang.AssertionError.class)
    public void testApproximateNotEqual() {
        List<Magic> magics1 = Arrays.asList(new Magic("Holden", 10.0, byteArray1),
                                            new Magic("Mahmoud", 9.9, byteArray1));

        List<Magic> magics2 = Arrays.asList(new Magic("Holden", 10.2, byteArray1),
                                            new Magic("Mahmoud", 10.0, byteArray1));

        approxEqualDataFrames(toDF(magics1), toDF(magics2), 0.1);
    }

    @Test
    public void testApproximateEqualRows() {
        List<Magic> magics = Arrays.asList(new Magic("Holden", 10.0, byteArray1),
                                           new Magic("Mahmoud", 9.9, byteArray1));

        Row row1 = toDF(magics).collect()[0];
        Row row2 = toDF(magics).collect()[1];

        assertTrue(approxEquals(row1, row1, 0));
        assertFalse(approxEquals(row1, row2, 0));
    }

    private DataFrame toDF(List<Magic> list) {
        JavaRDD<Magic> rdd = jsc().parallelize(list);
        return sqlContext().createDataFrame(rdd, Magic.class);
    }

    public static class Magic implements Serializable {
        private String name;
        private double power;
        private byte[] byteArray;

        public Magic(String name, double power, byte[] byteArray) {
            this.name = name;
            this.power = power;
            this.byteArray = byteArray;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public double getPower() {
            return power;
        }

        public void setPower(double power) {
            this.power = power;
        }

        public void setByteArray(byte[] arr) {
            this.byteArray = arr;
        }

        public byte[] getByteArray() {
            return byteArray;
        }
    }
}
