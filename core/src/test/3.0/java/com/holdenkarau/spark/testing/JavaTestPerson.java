package com.holdenkarau.spark.testing;

import java.io.Serializable;

public class JavaTestPerson implements Serializable {
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
