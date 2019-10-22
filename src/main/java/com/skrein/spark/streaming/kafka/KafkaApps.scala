package com.skrein.spark.streaming.kafka

import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.{ZkClient, ZkConnection}
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.{Assign, Subscribe}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 *
 *
 * @author :hujiansong  
 * @date :2019/10/21 10:29
 * @since :1.8
 *
 */
object KafkaApps {
  def main(args: Array[String]): Unit = {
    val logger: Log = LogFactory.getLog(getClass)
    val spark = SparkSession
      .builder()
      .appName("KafkaApps")
      .config("spark.some.config.option", "some-value")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(1))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-kafka-group",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    val topicsSet = Array("spark-kafka")
    val groupId = "spark-kafka-group"
    val zkUrls = "localhost:2181"
    val multipleTopicDirs: mutable.Map[String, ZKGroupTopicDirs] = mutable.Map[String, ZKGroupTopicDirs]()
    topicsSet.foreach(topic => {
      multipleTopicDirs.put(topic, new ZKGroupTopicDirs(groupId, topic))
    })
    val zkConnection = new ZkConnection(zkUrls, 60000)
    val zkClient = new ZkClient(zkConnection)

    var fromOffsets: Map[TopicPartition, Long] = Map() //如果
    logger.warn("start read start offset...")
    multipleTopicDirs.foreach(topicDirs => {
      val partitions = zkClient.countChildren(topicDirs._2.consumerOffsetDir)
      if (partitions > 0) {
        for (i <- 0 until partitions) {
          val offsetPath = s"${topicDirs._2.consumerOffsetDir}/$i"
          val offset = zkClient.readData(offsetPath)
          val tp = new TopicPartition(topicDirs._1, offset._1.)
          logger.warn(s"$offsetPath $offset")
          fromOffsets += (tp -> offset)
        }
      }
    })


    fromOffsets.foreach(tpo => {
      logger.warn("fromOffsets topic=" + tpo._1.topic() + " partition=" + tpo._1.partition() + " offset=" + tpo._2)
    })

    logger.warn("fromOffsets.keys= " + fromOffsets.keys)
    val stream = if (fromOffsets.keys.isEmpty) {
      KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topicsSet, kafkaParams)
      )
    } else {
      KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Assign[String, String](fromOffsets.keys, kafkaParams, fromOffsets)
      )
    }

    var offsetRanges: Array[OffsetRange] = null
    stream.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }).foreachRDD(rdd => {
      val words = rdd.map(consumerRecord => consumerRecord.value())
        .flatMap(_.split(" "))
        .map((_, 1))
        .reduceByKey(_ + _)
      words.foreach(println)
      if (offsetRanges != null) {

        for (o <- offsetRanges) {
          val zkPath = s"${multipleTopicDirs(o.topic).consumerOffsetDir}/${o.partition}"
          logger.warn(s"$zkPath offset=${o.fromOffset}")
          new ZkUtils(zkClient, zkConnection, false).updatePersistentPath(zkPath, o.fromOffset.toString)
        }
      }
    })


    ssc.start()
    ssc.awaitTermination()

  }


}
