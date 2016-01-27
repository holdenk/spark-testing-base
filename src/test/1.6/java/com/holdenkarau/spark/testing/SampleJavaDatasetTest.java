package com.holdenkarau.spark.testing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.catalyst.encoders.OuterScopes;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SampleJavaDatasetTest extends JavaDatasetSuiteBase implements Serializable {

    @Test
    public void equalEmptyDataset() {
        OuterScopes.addOuterScope(this);
        List<Person> list = new ArrayList<>();
        Dataset<Person> dataset = sqlContext().createDataset(list, Encoders.bean(Person.class));

        equalDatasets(dataset, dataset);
    }

    @Test
    public void datasetEqualItself() {
        OuterScopes.addOuterScope(this);

        Person person = createPerson("Hanafy", 23, 80.0);
        List<Person> list = Arrays.asList(person);
        Dataset<Person> dataset = sqlContext().createDataset(list, Encoders.bean(Person.class));

        equalDatasets(dataset, dataset);
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

        equalDatasets(dataset1, dataset2);
    }

    @Test
    public void approximateEqualEmptyDataset() {
        OuterScopes.addOuterScope(this);
        List<Person> list = new ArrayList<>();
        Dataset<Person> dataset = sqlContext().createDataset(list, Encoders.bean(Person.class));

        approxEqualDatasets(dataset, dataset, 0.0);
    }

    @Test
    public void approximateEqualDatasetItself() {
        OuterScopes.addOuterScope(this);

        Person person = createPerson("Hanafy", 23, 80.0);
        List<Person> list = Arrays.asList(person);
        Dataset<Person> dataset = sqlContext().createDataset(list, Encoders.bean(Person.class));

        approxEqualDatasets(dataset, dataset, 0.0);
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

        approxEqualDatasets(dataset1, dataset2, 0.201);
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

        approxEqualDatasets(dataset1, dataset2, 0.2);
    }

    private Person createPerson(String name, int age, double weight) {
        Person person = new Person();
        person.setName(name);
        person.setAge(age);
        person.setWeight(weight);

        return person;
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

}
