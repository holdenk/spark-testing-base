package com.holdenkarau.spark.testing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.catalyst.encoders.OuterScopes;
import org.junit.Test;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SampleJavaDatasetTest extends JavaDatasetSuiteBase implements Serializable {

    @Test
    public void equalEmptyDataset() {
        OuterScopes.addOuterScope(this);
        List<JavaTestPerson> list = new ArrayList<>();
        Dataset<JavaTestPerson> dataset = sqlContext().createDataset(list, Encoders.bean(JavaTestPerson.class));

        assertDatasetEquals(dataset, dataset);
    }

    @Test
    public void datasetEqualItself() {
        OuterScopes.addOuterScope(this);

        JavaTestPerson person = createPerson("Hanafy", 23, 80.0);
        List<JavaTestPerson> list = Arrays.asList(person);
        Dataset<JavaTestPerson> dataset = sqlContext().createDataset(list, Encoders.bean(JavaTestPerson.class));

        assertDatasetEquals(dataset, dataset);
    }

    @Test (expected = AssertionError.class)
    public void notEqualDataset() {
        OuterScopes.addOuterScope(this);

        JavaTestPerson person1 = createPerson("Hanafy", 23, 80.0);
        List<JavaTestPerson> list1 = Arrays.asList(person1);
        Dataset<JavaTestPerson> dataset1 = sqlContext().createDataset(list1, Encoders.bean(JavaTestPerson.class));

        JavaTestPerson person2 = createPerson("Hanofy", 23, 80.0);
        List<JavaTestPerson> list2 = Arrays.asList(person2);
        Dataset<JavaTestPerson> dataset2 = sqlContext().createDataset(list2, Encoders.bean(JavaTestPerson.class));

        assertDatasetEquals(dataset1, dataset2);
    }

    @Test
    public void approximateEqualEmptyDataset() {
        OuterScopes.addOuterScope(this);
        List<JavaTestPerson> list = new ArrayList<>();
        Dataset<JavaTestPerson> dataset = sqlContext().createDataset(list, Encoders.bean(JavaTestPerson.class));

        assertDatasetApproximateEquals(dataset, dataset, 0.0);
    }

    @Test
    public void approximateEqualDatasetItself() {
        OuterScopes.addOuterScope(this);

        JavaTestPerson person = createPerson("Hanafy", 23, 80.0);
        List<JavaTestPerson> list = Arrays.asList(person);
        Dataset<JavaTestPerson> dataset = sqlContext().createDataset(list, Encoders.bean(JavaTestPerson.class));

        assertDatasetApproximateEquals(dataset, dataset, 0.0);
    }

    @Test
    public void approximateEqualAcceptableTolerance() {
        OuterScopes.addOuterScope(this);

        JavaTestPerson person1 = createPerson("Hanafy", 23, 80.0);
        List<JavaTestPerson> list1 = Arrays.asList(person1);
        Dataset<JavaTestPerson> dataset1 = sqlContext().createDataset(list1, Encoders.bean(JavaTestPerson.class));

        JavaTestPerson person2 = createPerson("Hanafy", 23, 80.2);
        List<JavaTestPerson> list2 = Arrays.asList(person2);
        Dataset<JavaTestPerson> dataset2 = sqlContext().createDataset(list2, Encoders.bean(Person.class));

        assertDatasetApproximateEquals(dataset1, dataset2, 0.201);
    }

    @Test (expected = AssertionError.class)
    public void approximateEqualLowTolerance() {
        OuterScopes.addOuterScope(this);

        JavaTestPerson person1 = createPerson("Hanafy", 23, 80.0);
        List<JavaTestPerson> list1 = Arrays.asList(person1);
        Dataset<JavaTestPerson> dataset1 = sqlContext().createDataset(list1, Encoders.bean(JavaTestPerson.class));

        JavaTestPerson person2 = createPerson("Hanafy", 23, 80.5);
        List<JavaTestPerson> list2 = Arrays.asList(person2);
        Dataset<JavaTestPerson> dataset2 = sqlContext().createDataset(list2, Encoders.bean(JavaTestPerson.class));

        assertDatasetApproximateEquals(dataset1, dataset2, 0.2);
    }

    @Test
    public void approximateEqualTime() {
        OuterScopes.addOuterScope(this);

        Time time1 = createTime("Holden", Timestamp.valueOf("2018-01-12 22:21:23"));
        List<Time> list1 = Arrays.asList(time1);
        Dataset<Time> dataset1 = sqlContext().createDataset(list1, Encoders.bean(Time.class));

        Time time2 = createTime("Holden", Timestamp.valueOf("2018-01-12 22:21:23"));
        List<Time> list2 = Arrays.asList(time2);
        Dataset<Time> dataset2 = sqlContext().createDataset(list2, Encoders.bean(Time.class));

        assertDatasetApproximateEquals(dataset1, dataset2, 0);
    }

    @Test
    public void approximateEqualTimeAcceptableTolerance() {
        OuterScopes.addOuterScope(this);

        Time time1 = createTime("Shakanti", Timestamp.valueOf("2018-01-12 22:21:23"));
        List<Time> list1 = Arrays.asList(time1);
        Dataset<Time> dataset1 = sqlContext().createDataset(list1, Encoders.bean(Time.class));

        Time time2 = createTime("Shakanti", Timestamp.valueOf("2018-01-12 22:21:43"));
        List<Time> list2 = Arrays.asList(time2);
        Dataset<Time> dataset2 = sqlContext().createDataset(list2, Encoders.bean(Time.class));

        assertDatasetApproximateEquals(dataset1, dataset2, 30000);
    }

    @Test (expected = AssertionError.class)
    public void approximateEqualTimeLowTolerance() {
        OuterScopes.addOuterScope(this);

        Time time1 = createTime("Shakanti", Timestamp.valueOf("2018-01-12 22:21:23"));
        List<Time> list1 = Arrays.asList(time1);
        Dataset<Time> dataset1 = sqlContext().createDataset(list1, Encoders.bean(Time.class));

        Time time2 = createTime("Shakanti", Timestamp.valueOf("2018-01-12 22:22:43"));
        List<Time> list2 = Arrays.asList(time2);
        Dataset<Time> dataset2 = sqlContext().createDataset(list2, Encoders.bean(Time.class));

        assertDatasetApproximateEquals(dataset1, dataset2, 60000);
    }

    private JavaTestPerson createPerson(String name, int age, double weight) {
        JavaTestPerson person = new JavaTestPerson();
        person.setName(name);
        person.setAge(age);
        person.setWeight(weight);

        return person;
    }

    private JavaTestTime createTime(String name, Timestamp time) {
        JavaTestTime t = new JavaTestTime();
        t.setName(name);
        t.setTime(time);

        return t;
    }
}
