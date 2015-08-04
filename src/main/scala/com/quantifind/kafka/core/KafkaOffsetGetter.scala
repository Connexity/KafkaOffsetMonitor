package com.quantifind.kafka.core

import com.quantifind.kafka.OffsetGetter
import OffsetGetter.OffsetInfo
import com.twitter.util.Time
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.TopicAndPartition
import kafka.consumer.ConsumerConnector
import kafka.consumer.KafkaStream
import kafka.server.OffsetManager
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.zookeeper.data.Stat

import scala.collection._
import scala.util.control.NonFatal

class KafkaOffsetGetter(theZkClient: ZkClient, kafkaConsumerConnector: ConsumerConnector) extends OffsetGetter {

  override val zkClient = theZkClient

//  new Runnable {
//    override def run() = {
//      val ks: KafkaStream = _
//      val it = ks.iterator()
//      while (it.hasNext())
//        it.next().message()
//
//    }
//  }.run()
//

  override def processPartition(group: String, topic: String, pid: Int): Option[OffsetInfo] = {
    try {
      val (offset, stat: Stat) = ZkUtils.readData(zkClient, s"${ZkUtils.ConsumersPath}/$group/offsets/$topic/$pid")
      val (owner, _) = ZkUtils.readDataMaybeNull(zkClient, s"${ZkUtils.ConsumersPath}/$group/owners/$topic/$pid")

      ZkUtils.getLeaderForPartition(zkClient, topic, pid) match {
        case Some(bid) =>
          val consumerOpt = consumerMap.getOrElseUpdate(bid, getConsumer(bid))
          consumerOpt map {
            consumer =>
              val topicAndPartition = TopicAndPartition(topic, pid)
              val request =
                OffsetRequest(immutable.Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)))
              val logSize = consumer.getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition).offsets.head

              OffsetInfo(group = group,
                topic = topic,
                partition = pid,
                offset = offset.toLong,
                logSize = logSize,
                owner = owner,
                creation = Time.fromMilliseconds(stat.getCtime),
                modified = Time.fromMilliseconds(stat.getMtime))
          }
        case None =>
          error("No broker for partition %s - %s".format(topic, pid))
          None
      }
    } catch {
      case NonFatal(t) =>
        error(s"Could not parse partition info. group: [$group] topic: [$topic]", t)
        None
    }
  }

  override def getGroups: Seq[String] = {
    try {
      OffsetManager
      ZkUtils.getChildren(zkClient, ZkUtils.ConsumersPath)
    } catch {
      case NonFatal(t) =>
        error(s"could not get groups because of ${t.getMessage}", t)
        Seq()
    }
  }

  override def getActiveTopicMap: Map[String, Seq[String]] = {
    try {
      ZkUtils.getChildren(zkClient, ZkUtils.ConsumersPath).flatMap {
        group =>
          try {
            ZkUtils.getConsumersPerTopic(zkClient, group, true).keySet.map {
              key =>
                key -> group
            }
          } catch {
            case NonFatal(t) =>
              error(s"could not get consumers for group $group", t)
              Seq()
          }
      }.groupBy(_._1).mapValues {
        _.unzip._2
      }
    } catch {
      case NonFatal(t) =>
        error(s"could not get topic maps because of ${t.getMessage}", t)
        Map()
    }
  }

}
