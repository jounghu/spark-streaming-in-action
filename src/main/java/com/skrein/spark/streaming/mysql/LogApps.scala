package com.skrein.spark.streaming.mysql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *
 *
 * @author :hujiansong  
 * @date :2019/10/22 15:33
 * @since :1.8
 *
 */

case class Record(log_level: String, method: String, content: String)

object LogApps {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("LogAnalysis").setMaster("local[*]")
      .set("spark.local.dir", "./tmp")

    val spark = SparkSession.builder()
      .appName("LogAnalysis")
      .config(sparkConf)
      .getOrCreate()

    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))

    // Mysql配置
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "Qwe123###")

    val dStream = ssc.textFileStream("C:\\ad-dev\\spark-streaming-in-action\\spark-streaming-dir\\")
    dStream.foreachRDD(rdd=>{
      rdd.foreach(println)
    })
    dStream.print()
    dStream.foreachRDD(rdd => {
      import spark.implicits._
      val data = rdd.map(line => {
        val tokens = line.split("\t")
        Record(tokens(0), tokens(1), tokens(2))
      }).toDS()

      data.createOrReplaceTempView("logapps")

      val logData = spark.sql("select * from logapps where log_level='[error]' or log_level='[warn]'")
      logData.show()

      logData.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/spark?useSSL=false","logs",properties)

    })


    ssc.start()
    ssc.awaitTermination()

  }
}
