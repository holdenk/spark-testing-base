package com.holdenkarau.spark.testing

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import java.util.concurrent.atomic.AtomicReference

/**
 * Utility companion object to provide getOrCreate on SQLContext
 */
object SQLContext {
    private val INSTANTIATION_LOCK = new Object()

  /**
   * Reference to the last created SQLContext.
   */
  @transient private val lastInstantiatedContext = new AtomicReference[org.apache.spark.sql.SQLContext]()

  /**
   * Get the singleton SQLContext if it exists or create a new one using the given SparkContext.
   * This function can be used to create a singleton SQLContext object that can be shared across
   * the JVM.
   * This is a bit of a hack over the regular Spark version, we re-create if the Spark Context
   * differs.
   */
  def getOrCreate(sparkContext: SparkContext): SQLContext = {
    INSTANTIATION_LOCK.synchronized {
      lastInstantiatedContext.get() match {
        case null =>
          lastInstantiatedContext.set(new SQLContext(sparkContext))
        case _ if lastInstantiatedContext.get().sparkContext != sparkContext =>
          clearLastInstantiatedContext()
          lastInstantiatedContext.set(new SQLContext(sparkContext))
      }
      lastInstantiatedContext.get()
    }
  }

  private def clearLastInstantiatedContext(): Unit = {
    INSTANTIATION_LOCK.synchronized {
      lastInstantiatedContext.set(null)
    }
  }

  private def setLastInstantiatedContext(sqlContext: SQLContext): Unit = {
    INSTANTIATION_LOCK.synchronized {
      lastInstantiatedContext.set(sqlContext)
    }
  }
}
