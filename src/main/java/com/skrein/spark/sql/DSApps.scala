package com.skrein.spark.sql

import org.apache.spark.sql.SparkSession

/**
 *
 *
 * @author :hujiansong  
 * @date :2019/10/22 14:34
 * @since :1.8
 *
 */
case class Person(name: String, age: Long)

object DSApps {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL basic example")
      .getOrCreate()

    import spark.implicits._

    val peopleDS = spark.read.json("C:\\ad-dev\\spark-streaming-in-action\\src\\main\\resources\\people.json").as[Person]

    peopleDS.show()

  }
}
