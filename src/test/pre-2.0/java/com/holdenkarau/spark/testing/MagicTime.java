package com.holdenkarau.spark.testing;

import java.io.Serializable;

import java.sql.Timestamp;

public class MagicTime implements Serializable {
  private String name;
  private Timestamp t;

  public String getName() {
    return this.name;
  }
  public Timestamp getT() {
    return this.t;
  }

  public MagicTime() {
  }

  public MagicTime(String newName, Timestamp nt) {
    this.name = newName;
    this.t = nt;
  }
  
  public void setName(String newName) {
    this.name = newName;
  }

  public void setT(Timestamp nt) {
    this.t = nt;
  }
}
