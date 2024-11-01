package kafka.server.intercept

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.record.{MemoryRecords, SimpleRecord}

import java.util.concurrent.{ConcurrentLinkedQueue, Executors}
import scala.collection.immutable
import scala.jdk.CollectionConverters.IterableHasAsScala

class ProduceRequestInterceptorManager(interceptor: ProduceRequestInterceptor) {
  def intercept(request: scala.collection.Map[TopicPartition, MemoryRecords]): immutable.Map[TopicPartition, MemoryRecords] = {
    val svc = Executors.newSingleThreadExecutor()
    val results = new ConcurrentLinkedQueue[ProduceRequestInterceptor.Record]()

    try {
      request.toSeq.flatMap { case (tp, records) =>
        records.records().asScala
          .flatMap(r => interceptor.intercept(new RecordImpl(tp.topic(), tp.partition(), r)).asScala)
          .map(r => r.topicPartition() -> Conversions.asSimpleRecord(r))
          .groupBy { case (tp, records) => tp }
          .map { case (tp, coll) => tp ->
            MemoryRecords.withRecords(Compression.NONE, coll.map { case (k, v) => v }.toArray[SimpleRecord]: _*)
          }
      }.toMap
    } finally {
      svc.shutdown();
    }
  }
}
