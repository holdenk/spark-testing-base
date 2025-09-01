package org.apache.spark.sql.internal

import org.apache.spark.sql._
// Switched between 4 preview and 4
import org.apache.spark.sql.classic._
import org.apache.spark.sql.internal._
import org.apache.spark.sql.catalyst.expressions._


object EvilExpressionColumnNode {
  def getExpr(node: ColumnNode): Expression = {
    ColumnNodeToExpressionConverter.apply(node)
  }
  def toColumnNode(expr: Expression): ColumnNode = {
    ExpressionColumnNode(expr)
  }
}
