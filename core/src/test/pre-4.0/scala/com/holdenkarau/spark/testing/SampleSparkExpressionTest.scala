/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.holdenkarau.spark.testing

import org.apache.spark.sql.{Column => SColumn, SparkSession, DataFrame}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.types._
import org.scalatest.Suite

/**
 * To run this test yous must set SPARK_TESTING=yes (or other non-null value).
 */
class SampleSparkExpressionTest extends ScalaDataFrameSuiteBase {
  val inputList = List(
    FakeMagic("panda"),
    FakeMagic("coffee"))

  testNonCodegen("non-codegen paths!") {
    val session = SparkSession.builder().getOrCreate()
    import session.implicits._
    val input = sc.parallelize(inputList).toDF
    val result_working = input.select(WorkingCodegenExpression.work(input("name")) + 1)
    val result_failing = input.select(FailingCodegenExpression.fail(input("name")) + 1)
    assert(result_working.collect()(0)(0) === 2)
    assert(result_failing.collect()(0)(0) === 2)
  }

  testCodegenOnly("verify codegen tests are run with codegen.") {
    import sqlContext.implicits._
    val input = sc.parallelize(inputList).toDF
    val result_working = input.select(WorkingCodegenExpression.work(input("name")))
    val result_failing = input.select(FailingCodegenExpression.fail(input("name")))
    assert(result_working.collect()(0)(0) === 1)
    assert(result_failing.collect()(0)(0) === 3)
  }
}

object WorkingCodegenExpression {
  private def withExpr(expr: Expression): SColumn = new SColumn(expr)

  def work(col: SColumn): SColumn = withExpr {
    WorkingCodegenExpression(col.expr)
  }
}


//tag::unary[]
case class WorkingCodegenExpression(child: Expression) extends UnaryExpression {
  override def prettyName = "workingCodegen"

  override def nullSafeEval(input: Any): Any = {
    if (input == null) {
      return null
    }
    return 1
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // Code to serialize.
    val input = child.genCode(ctx)
    val javaType = CodeGenerator.javaType(dataType)
    val code = input.code + code"""
        final $javaType ${ev.value} =
          ${input.isNull} ? ${CodeGenerator.defaultValue(dataType)} : 1;
     """
    ev.copy(code = code, isNull = input.isNull)
  }

  override def dataType: DataType = IntegerType

  // New in 3.2
  def withNewChildInternal(newChild: Expression) = {
    copy(child = newChild)
  }
}
//end::unary[]

object FailingCodegenExpression {
  private def withExpr(expr: Expression): SColumn = new SColumn(expr)

  def fail(col: SColumn): SColumn = withExpr {
    FailingCodegenExpression(col.expr)
  }
}

case class FailingCodegenExpression(child: Expression) extends UnaryExpression {
  override def prettyName = "failingCodegen"

  override def nullSafeEval(input: Any): Any = {
    if (input == null) {
      return null
    }
    return 1
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // Code to serialize.
    val input = child.genCode(ctx)
    val javaType = CodeGenerator.javaType(dataType)
    val code = input.code + code"""
        final $javaType ${ev.value} = 3;
     """
    ev.copy(code = code, isNull = input.isNull)
  }
  override def dataType: DataType = IntegerType

  // New in 3.2
  def withNewChildInternal(newChild: Expression) = {
    copy(child = newChild)
  }
}

case class FakeMagic(name: String)
