//package com.skrein.spark.streaming.kafka
//
//import org.apache.kafka.common.TopicPartition
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}
//import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
///**
// *
// *
// * @author :hujiansong
// * @date :2019/10/21 17:54
// * @since :1.8
// *
// */
//object KafkaOffsetApps {
//  def main(args: Array[String]): Unit = {
//
//
//    val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaOffsetApps")
//    val ssc = new StreamingContext(conf, Seconds(1))
//
//    val fromOffsets = Map().map { resultSet =>
//      new TopicPartition(resultSet.string("topic"), resultSet.int("partition")) -> resultSet.long("offset")
//    }.toMap
//
//
//
//    val stream = KafkaUtils.createDirectStream[String, String](
//      ssc,
//      PreferConsistent,
//      Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
//    )
//
//    stream.foreachRDD { rdd =>
//      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//
//      val results = yourCalculation(rdd)
//
//      // begin your transaction
//
//      // update results
//      // update offsets where the end of existing offsets matches the beginning of this batch of offsets
//      // assert that offsets were updated correctly
//
//      // end your transaction
//    }
//  }
//}
