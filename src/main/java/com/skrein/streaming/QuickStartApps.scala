package com.skrein.streaming

import org.apache.spark._
import org.apache.spark.streaming._

/**
 *
 *
 * @author :hujiansong  
 * @date :2019/10/18 16:18
 * @since :1.8
 *
 */
object QuickStartApps {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))

    val lines = ssc.socketTextStream("localhost", 9999)

    val words = lines.flatMap(_.split(" "))

    val pairs = words.map(word => (word, 1))

    val wordCount = pairs.reduceByKey((x, y) => x + y)

    wordCount.foreachRDD(rdd => rdd.foreachPartition(p => p.foreach(println)))

    ssc.start()
    ssc.awaitTermination()
  }
}
