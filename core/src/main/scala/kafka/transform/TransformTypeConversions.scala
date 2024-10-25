package kafka.transform

import kafka.server.{KafkaConfig, MetadataCache}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.MemoryRecords

import java.util.Collection
import scala.jdk.CollectionConverters._

object TransformTypeConversions {

  def topicsMeta(topics: Collection[String],
                 metadataCache: MetadataCache, config: KafkaConfig) = {
    metadataCache.getTopicMetadata(topics.asScala.toSet, config.interBrokerListenerName)
      .map(meta => meta.name() -> meta).toMap.asJava
  }

  def asScala(m: java.util.Map[TopicPartition, MemoryRecords]) = {
    m.asScala.toSeq
  }

}
