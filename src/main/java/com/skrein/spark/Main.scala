package com.skrein.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 *
 * @author :hujiansong  
 * @date :2019/10/22 11:25
 * @since :1.8
 *
 */
object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.textFile(args(0)).flatMap(line => line.split(" "))
      .map((_, 1)).reduceByKey(_ + _).saveAsTextFile(args(1))
    sc.stop()
  }

  class Person(name: String) {
    val this.name = name;
  }

}
