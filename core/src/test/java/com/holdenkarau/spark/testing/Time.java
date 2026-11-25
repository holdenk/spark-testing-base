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
