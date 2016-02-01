package com.holdenkarau.spark.testing;

import java.io.Serializable;

public class BasicMagic implements Serializable {
  private String name;
  private double power;

  public BasicMagic(String name, double power) {
    this.name = name;
    this.power = power;
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
}
