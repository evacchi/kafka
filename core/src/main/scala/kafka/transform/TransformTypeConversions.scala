package kafka.transform

import org.apache.kafka.common.message.MetadataResponseData

import scala.jdk.CollectionConverters._

object TransformTypeConversions {
  def metadataSeqToJavaMap(x: collection.Seq[MetadataResponseData.MetadataResponseTopic]) = {
    x.map(meta => meta.name() -> meta).toMap.asJava
  }

  def asScalaSet[T](s: java.util.Set[T]) = s.asScala

  def asScalaMap[T, U](m: java.util.Map[T, U]) = m.asScala

}
