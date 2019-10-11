package com.skrein

import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wc")

    val sc = new SparkContext(conf)
    sc.textFile(args(0)).flatMap(line => line.split(" "))
      .map((_, 1)).reduceByKey(_ + _).saveAsTextFile(args(1))
    sc.stop()
  }
}
