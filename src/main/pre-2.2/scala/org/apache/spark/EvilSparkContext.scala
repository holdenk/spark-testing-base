package org.apache.spark

import java.util.concurrent.atomic.AtomicReference

/**
 * Access Spark internals.
 */
object EvilSparkContext {
  def stopActiveSparkContext(): Unit = {
    val declaredFields = classOf[SparkContext$].getDeclaredFields()
    declaredFields.foreach{field => field.setAccessible(true) }
    val activeContextField =  declaredFields.filter(_.getName.contains("active"))
    val activeContextValue = activeContextField.map(field => field.get(SparkContext$.MODULE$))
    val activeContextRef = activeContextValue.filter(ctx => ctx != null && ctx.isInstanceOf[AtomicReference[_]])
    activeContextRef.foreach(ctx => Option(ctx.asInstanceOf[AtomicReference[SparkContext]].get()).foreach(_.stop()))
  }
}
