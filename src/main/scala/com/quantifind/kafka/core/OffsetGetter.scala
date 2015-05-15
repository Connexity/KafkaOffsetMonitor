package com.quantifind.kafka.core

import com.quantifind.kafka.core.OffsetGetter.KafkaInfo
import com.twitter.util.Time
import kafka.consumer.SimpleConsumer

import scala.collection._

case class Node(name: String, children: Seq[Node] = Seq())

case class TopicDetails(consumers: Seq[ConsumerDetail])

case class ConsumerDetail(name: String)

trait OffsetGetter {

  val consumerMap: mutable.Map[Int, Option[SimpleConsumer]] = mutable.Map()

  def getInfo(group: String, topics: Seq[String] = Seq()): KafkaInfo

  def getGroups: Seq[String]

  def getTopics: Seq[String]

  def getActiveTopicMap: Map[String, Seq[String]]

  def getClusterViz: Node

  /**
   * returns details for a given topic such as the active consumers pulling off of it
   * @param topic
   * @return
   */
  def getTopicDetail(topic: String): TopicDetails = {
    val topicMap = getActiveTopicMap

    if (topicMap.contains(topic)) {
      TopicDetails(topicMap(topic).map(consumer => {
        ConsumerDetail(consumer.toString)
      }).toSeq)
    } else {
      TopicDetails(Seq(ConsumerDetail("Unable to find Active Consumers")))
    }
  }

  def getActiveTopics: Node = {
    val topicMap = getActiveTopicMap

    Node("ActiveTopics", topicMap.map {
      case (s: String, ss: Seq[String]) => {
        Node(s, ss.map(consumer => Node(consumer)))

      }
    }.toSeq)
  }

  def close() {
    for (consumerOpt <- consumerMap.values) {
      consumerOpt match {
        case Some(consumer) => consumer.close()
        case None => // ignore
      }
    }
  }
}

object OffsetGetter {

  case class KafkaInfo(brokers: Seq[BrokerInfo], offsets: Seq[OffsetInfo])

  case class BrokerInfo(id: Int, host: String, port: Int)

  case class OffsetInfo(group: String,
                        topic: String,
                        partition: Int,
                        offset: Long,
                        logSize: Long,
                        owner: Option[String],
                        creation: Time,
                        modified: Time) {
    val lag = logSize - offset
  }
}