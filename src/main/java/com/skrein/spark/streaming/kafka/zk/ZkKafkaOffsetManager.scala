package com.skrein.spark.streaming.kafka.zk

import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 *
 *
 * @author :hujiansong  
 * @date :2019/10/22 19:08
 * @since :1.8
 *
 */
class ZkKafkaOffsetManager(zkUrl: String) {
  private val logger = LoggerFactory.getLogger(classOf[ZkKafkaOffsetManager])

  private val zkClientAndConn = ZkUtils.createZkClientAndConnection(zkUrl, 30000, 30000);
  private val zkUtils = new ZkUtils(zkClientAndConn._1, zkClientAndConn._2, false)

  def readOffsets(topics: Seq[String], groupId: String): Map[TopicPartition, Long] = {
    val offsets = mutable.HashMap.empty[TopicPartition, Long]
    val partitionsForTopics = zkUtils.getPartitionsForTopics(topics)

    // /consumers/<groupId>/offsets/<topic>/<partition>
    partitionsForTopics.foreach(partitions => {
      val topic = partitions._1
      val groupTopicDirs = new ZKGroupTopicDirs(groupId, topic)

      partitions._2.foreach(partition => {
        val path = groupTopicDirs.consumerOffsetDir + "/" + partition
        try {
          val data = zkUtils.readData(path)
          if (data != null) {
            offsets.put(new TopicPartition(topic, partition), data._1.toLong)
            logger.info(
              "Read offset - topic={}, partition={}, offset={}, path={}",
              Seq[AnyRef](topic, partition.toString, data._1, path)
            )
          }
        } catch {
          case ex: Exception =>
            offsets.put(new TopicPartition(topic, partition), 0L)
            logger.info(
              "Read offset - not exist: {}, topic={}, partition={}, path={}",
              Seq[AnyRef](ex.getMessage, topic, partition.toString, path)
            )
        }
      })
    })
    offsets.toMap
  }

  def saveOffsets(offsetRanges: Seq[OffsetRange], groupId: String): Unit = {
    offsetRanges.foreach(range => {
      val groupTopicDirs = new ZKGroupTopicDirs(groupId, range.topic)
      val path = groupTopicDirs.consumerOffsetDir + "/" + range.partition
      zkUtils.updatePersistentPath(path, range.untilOffset.toString)
      logger.info(
        "Save offset - topic={}, partition={}, offset={}, path={}",
        Seq[AnyRef](range.topic, range.partition.toString, range.untilOffset.toString, path)
      )
    })
  }
}
