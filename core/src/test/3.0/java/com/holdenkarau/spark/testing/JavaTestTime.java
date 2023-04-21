package com.holdenkarau.spark.testing;

import java.io.Serializable;
import java.sql.Timestamp;

public class JavaTestTime implements Serializable {
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
    if (obj instanceof JavaTestTime) {
      JavaTestTime objTime = (JavaTestTime) obj;
      return objTime.getName().equals(this.name) && objTime.getTime().equals(this.time);
    }

    return false;
  }

  @Override
  public int hashCode() {
    return name.hashCode() + time.hashCode();
  }
}
