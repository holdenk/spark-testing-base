package com.holdenkarau.spark.testing;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class SampleJavaDataFrameTest extends JavaDataFrameSuiteBase {
    @Test
    public void testEqualWithSameObject() {
        List<Person> persons = Arrays.asList(new Person("holden", 30), new Person("mahmoud", 23));
        JavaRDD<Person> personsRDD = jsc().parallelize(persons);
        DataFrame personsDataFrame = sqlContext().createDataFrame(personsRDD, Person.class);

        equalDataFrames(personsDataFrame, personsDataFrame);
    }

    @Test
    public void testEqualWithDifferentObjects() {
        List<Person> persons1 = Arrays.asList(new Person("holden", 30), new Person("mahmoud", 23));
        List<Person> persons2 = Arrays.asList(new Person("holden", 30), new Person("mahmoud", 23));

        JavaRDD<Person> persons1RDD = jsc().parallelize(persons1);
        JavaRDD<Person> persons2RDD = jsc().parallelize(persons2);

        DataFrame persons1DataFrame = sqlContext().createDataFrame(persons1RDD, Person.class);
        DataFrame persons2DataFrame = sqlContext().createDataFrame(persons2RDD, Person.class);

        equalDataFrames(persons1DataFrame, persons2DataFrame);
    }

    public static class Person implements Serializable {
        private String name;
        private int age;

        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }
    }
}
