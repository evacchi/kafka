package kafka.server.intercept

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.record.{MemoryRecords, SimpleRecord}

import java.util.concurrent.{ConcurrentLinkedQueue, Executors, TimeUnit}
import scala.collection.immutable
import scala.jdk.CollectionConverters.IterableHasAsScala

class ProduceRequestInterceptorManager(interceptor: ProduceRequestInterceptor) {
  def intercept(request: scala.collection.Map[TopicPartition, MemoryRecords]): immutable.Map[TopicPartition, MemoryRecords] = {
    val svc = Executors.newSingleThreadExecutor()
    val results = new ConcurrentLinkedQueue[ProduceRequestInterceptor.Record]()

    try {
      for (
        (tp,v) <- request;
        r <- v.records().asScala) {
        svc.submit({ () =>
          val records = interceptor.intercept(new RecordImpl(tp, r))
          results.addAll(records)
        }: Runnable)
      }

      svc.shutdown()
      svc.awaitTermination(1, TimeUnit.SECONDS)

      results.asScala
      .map(r => r.topicPartition() -> Conversions.asSimpleRecord(r))
      .groupBy { case (tp, _) => tp }
      .map { case (tp, coll) => tp ->
        MemoryRecords.withRecords(Compression.NONE,
          coll.map { case (k, v) => v }.toArray[SimpleRecord]: _*)
      }
    } finally {
      svc.shutdown();
    }
  }
}
