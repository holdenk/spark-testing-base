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
        List<Person> list = new ArrayList<>();
        Dataset<Person> dataset = sqlContext().createDataset(list, Encoders.bean(Person.class));

        assertDatasetEquals(dataset, dataset);
    }

    @Test
    public void datasetEqualItself() {
        OuterScopes.addOuterScope(this);

        Person person = createPerson("Hanafy", 23, 80.0);
        List<Person> list = Arrays.asList(person);
        Dataset<Person> dataset = sqlContext().createDataset(list, Encoders.bean(Person.class));

        assertDatasetEquals(dataset, dataset);
    }

    @Test (expected = AssertionError.class)
    public void notEqualDataset() {
        OuterScopes.addOuterScope(this);

        Person person1 = createPerson("Hanafy", 23, 80.0);
        List<Person> list1 = Arrays.asList(person1);
        Dataset<Person> dataset1 = sqlContext().createDataset(list1, Encoders.bean(Person.class));

        Person person2 = createPerson("Hanofy", 23, 80.0);
        List<Person> list2 = Arrays.asList(person2);
        Dataset<Person> dataset2 = sqlContext().createDataset(list2, Encoders.bean(Person.class));

        assertDatasetEquals(dataset1, dataset2);
    }

    @Test
    public void approximateEqualEmptyDataset() {
        OuterScopes.addOuterScope(this);
        List<Person> list = new ArrayList<>();
        Dataset<Person> dataset = sqlContext().createDataset(list, Encoders.bean(Person.class));

        assertDatasetApproximateEquals(dataset, dataset, 0.0);
    }

    @Test
    public void approximateEqualDatasetItself() {
        OuterScopes.addOuterScope(this);

        Person person = createPerson("Hanafy", 23, 80.0);
        List<Person> list = Arrays.asList(person);
        Dataset<Person> dataset = sqlContext().createDataset(list, Encoders.bean(Person.class));

        assertDatasetApproximateEquals(dataset, dataset, 0.0);
    }

    @Test
    public void approximateEqualAcceptableTolerance() {
        OuterScopes.addOuterScope(this);

        Person person1 = createPerson("Hanafy", 23, 80.0);
        List<Person> list1 = Arrays.asList(person1);
        Dataset<Person> dataset1 = sqlContext().createDataset(list1, Encoders.bean(Person.class));

        Person person2 = createPerson("Hanafy", 23, 80.2);
        List<Person> list2 = Arrays.asList(person2);
        Dataset<Person> dataset2 = sqlContext().createDataset(list2, Encoders.bean(Person.class));

        assertDatasetApproximateEquals(dataset1, dataset2, 0.201);
    }

    @Test (expected = AssertionError.class)
    public void approximateEqualLowTolerance() {
        OuterScopes.addOuterScope(this);

        Person person1 = createPerson("Hanafy", 23, 80.0);
        List<Person> list1 = Arrays.asList(person1);
        Dataset<Person> dataset1 = sqlContext().createDataset(list1, Encoders.bean(Person.class));

        Person person2 = createPerson("Hanafy", 23, 80.5);
        List<Person> list2 = Arrays.asList(person2);
        Dataset<Person> dataset2 = sqlContext().createDataset(list2, Encoders.bean(Person.class));

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

    private Person createPerson(String name, int age, double weight) {
        Person person = new Person();
        person.setName(name);
        person.setAge(age);
        person.setWeight(weight);

        return person;
    }

    private Time createTime(String name, Timestamp time) {
        Time t = new Time();
        t.setName(name);
        t.setTime(time);

        return t;
    }

    public class Person implements Serializable {
        private String name;
        private int age;
        private double weight;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public double getWeight() {
            return weight;
        }

        public void setWeight(double weight) {
            this.weight = weight;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Person) {
                Person person = (Person) obj;
                return name.equals(person.name) && age == person.age && weight == person.weight;
            }

            return false;
        }

        @Override
        public int hashCode() {
            return name.hashCode() + age + (int)(weight);
        }
    }

    public class Time implements Serializable {
        private String name;
        private Timestamp time;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Timestamp getTime() {
            return time;
        }

        public void setTime(Timestamp time) {
            this.time = time;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Time) {
                Time objTime = (Time) obj;
                return objTime.getName().equals(this.name) && objTime.getTime().equals(this.time);
            }

            return false;
        }

        @Override
        public int hashCode() {
            return name.hashCode() + time.hashCode();
        }
    }
}
