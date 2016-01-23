package com.holdenkarau.spark.testing;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.catalyst.encoders.OuterScopes;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

// extending JavaDataFrameSuiteBase !!
// which is already running in test "SampleJavaDataFrameTest"
public class SampleJavaDatasetTest extends JavaDataFrameSuiteBase implements Serializable {

    @Test
    public void equalDataset() {
        OuterScopes.addOuterScope(this);

        Human human = new Human();
        human.setName("Hanafy");
        human.setAge(10);
        human.setWeight(100);
        List<Human> list = Arrays.asList(human);

        Dataset<Human> dataset = sqlContext().createDataset(list, Encoders.bean(Human.class));
        dataset.show(); // this works

        Dataset<Human> mapped = dataset.map(new MapFunction<Human, Human>() {
            @Override
            public Human call(Human v) throws Exception {
                v.setName("hello");
                return v;
            }
        }, Encoders.bean(Human.class));

        mapped.show(); // this gives exception

        throw new RuntimeException("failed");
    }

    public class Human implements Serializable {
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
            return name.equals(name) && age == age && weight == weight;
        }

        @Override
        public int hashCode() {
            return name.hashCode() + age + (int)(weight);
        }
    }
}