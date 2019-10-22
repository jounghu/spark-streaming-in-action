package com.skrein.spark.sql

import org.apache.spark.sql.SparkSession

/**
 *
 *
 * @author :hujiansong  
 * @date :2019/10/22 11:26
 * @since :1.8
 *
 */
object QuickStartApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL basic example")
      .getOrCreate()

    val sc = spark.sparkContext

    val df = spark.read.json("C:\\ad-dev\\spark-streaming-in-action\\src\\main\\resources\\people.json")

    df.show()
    df.printSchema()

    import spark.implicits._
    df.select($"name").show()

    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("select * from people")
    sqlDF.show()

  }
}
