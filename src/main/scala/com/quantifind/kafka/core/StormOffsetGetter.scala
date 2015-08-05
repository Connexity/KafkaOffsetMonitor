package com.quantifind.kafka.core

import com.quantifind.kafka.OffsetGetter
import com.quantifind.kafka.OffsetGetter.OffsetInfo
import com.twitter.util.Time
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.TopicAndPartition
import kafka.utils.{Json, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.apache.zookeeper.data.Stat

import scala.collection._
import scala.util.control.NonFatal

/**
 * a version that manages offsets saved by Storm Kafka Spout
 * User: hsun
 * Date: 8/1/15
 */
class StormOffsetGetter(theZkClient: ZkClient) extends OffsetGetter {

  import StormOffsetGetter._

  override val zkClient = theZkClient

  override def processPartition(group: String, topic: String, pid: Int): Option[OffsetInfo] = {
    try {
      val (stateJson, stat: Stat) = ZkUtils.readData(zkClient, s"$StormConsumerPath/$group/partition_$pid")
      // val (owner, _) = ZkUtils.readDataMaybeNull(zkClient, s"${ZkUtils.ConsumersPath}/$group/owners/$topic/$pid")

      val offset: String = Json.parseFull(stateJson) match {
        case Some(m) =>
          val spoutState = m.asInstanceOf[Map[String, Any]]
          spoutState.get("offset").getOrElse("-1").toString
        case None =>
          "-1"
      }

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
                owner = Some("NA"),
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
      ZkUtils.getChildren(zkClient, StormConsumerPath)
    } catch {
      case NonFatal(t) =>
        error(s"could not get groups because of ${t.getMessage}", t)
        Seq()
    }
  }

  // find all topics for this group, for Kafka Spout there is only one
  override def getTopicList(group: String): List[String] = {
    try {
      // assume there should be partition 0
      val (stateJson, _) = ZkUtils.readData(zkClient, s"$StormConsumerPath/$group/partition_0")
      Json.parseFull(stateJson) match {
        case Some(m) =>
          val spoutState = m.asInstanceOf[Map[String, Any]]
          List(spoutState.get("topic").getOrElse("Unknown Topic").toString)
        case None =>
          List()
      }
    } catch {
      case _: ZkNoNodeException => List()
    }
  }

  /**
   * Returns a map of topics -> list of consumers, including non-active
   */
  override def getTopicMap: Map[String, Seq[String]] = {
    try {
      ZkUtils.getChildren(zkClient, StormConsumerPath).flatMap {
        group => {
          getTopicList(group).map(topic => topic -> group)
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

  override def getActiveTopicMap: Map[String, Seq[String]] = {
    // not really have a way to determine which consumer is active now, so return all
    getTopicMap
  }

}

object StormOffsetGetter {
  val StormConsumerPath = "/stormconsumers"
}