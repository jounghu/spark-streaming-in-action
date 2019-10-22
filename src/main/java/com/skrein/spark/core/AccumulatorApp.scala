package com.skrein.spark.core

import org.apache.spark.sql.SparkSession

/**
 *
 *
 * @author :hujiansong  
 * @date :2019/10/22 13:59
 * @since :1.8
 *
 */
object AccumulatorApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL basic example")
      .getOrCreate()

    val sc = spark.sparkContext

    val acc = sc.longAccumulator("acc-long")

    sc.parallelize(Array(1, 2, 3, 4, 56, 2342, 234234, 234), 2).foreach(acc.add(_))

    println(acc.value)
  }
}
