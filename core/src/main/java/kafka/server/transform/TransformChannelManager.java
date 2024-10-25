package kafka.server.transform;

import kafka.server.KafkaConfig;
import kafka.server.MetadataCache;
import kafka.server.transform.TransformStore.MaybeLocalTopicPartition;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.ManualMetadataUpdater;
import org.apache.kafka.clients.MetadataRecoveryStrategy;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.InterBrokerSendThread;
import org.apache.kafka.server.util.RequestAndCompletionHandler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;


public class TransformChannelManager extends InterBrokerSendThread {

    public static TransformChannelManager create(
            KafkaConfig config, Metrics metrics, MetadataCache metadataCache, Time time) {

        LogContext logContext = new LogContext("transform-channel-manager");

        var channelBuilder = ChannelBuilders.clientChannelBuilder(
                config.interBrokerSecurityProtocol(),
                JaasContext.Type.SERVER,
                config,
                config.interBrokerListenerName(),
                config.saslMechanismInterBrokerProtocol(),
                time,
                config.saslInterBrokerHandshakeRequestEnable(),
                logContext
        );
        if (channelBuilder instanceof Reconfigurable) {
            config.addReconfigurable((Reconfigurable) channelBuilder);
        }
        var selector = new Selector(
                NetworkReceive.UNLIMITED,
                config.connectionsMaxIdleMs(),
                metrics,
                time,
                "transform-sender-channel",
                Map.of(),
                false,
                channelBuilder,
                logContext
        );
        var networkClient = new NetworkClient(
                selector,
                new ManualMetadataUpdater(),
                String.format("broker-%s-transform-sender", config.brokerId()),
                1,
                50,
                50,
                Selectable.USE_DEFAULT_BUFFER_SIZE,
                config.socketReceiveBufferBytes(),
                config.requestTimeoutMs(),
                config.connectionSetupTimeoutMs(),
                config.connectionSetupTimeoutMaxMs(),
                time,
                false,
                new ApiVersions(),
                logContext,
                MetadataRecoveryStrategy.NONE
        );
        return new TransformChannelManager(
                config,
                "transform-send-thread",
                networkClient,
                config.requestTimeoutMs(),
                time,
                true);
    }


    private final LinkedBlockingDeque<RequestAndCompletionHandler> requestQueue = new LinkedBlockingDeque<>();
    private final KafkaConfig config;
    private final Time time;

    public TransformChannelManager(KafkaConfig config, String name, KafkaClient networkClient, int requestTimeoutMs, Time time, boolean isInterruptible) {
        super(name, networkClient, requestTimeoutMs, time, isInterruptible);
        this.config = config;
        this.time = time;
    }

    @Override
    public void doWork() {
        super.doWork();
    }


    @Override
    public Collection<RequestAndCompletionHandler> generateRequests() {
        var r = new ArrayList<RequestAndCompletionHandler>();
        requestQueue.drainTo(r);
        return r;
    }

    public void enqueue(MaybeLocalTopicPartition tp, MemoryRecords memoryRecords) {
        log.info("Enqueuing {}", tp.topicPartition());
        TopicPartition topicPartition = tp.topicPartition();
        ProduceRequest.Builder requestBuilder = ProduceRequest.forMagic(RecordBatch.CURRENT_MAGIC_VALUE,
                new ProduceRequestData()
                        .setTopicData(new ProduceRequestData.TopicProduceDataCollection(Collections.singletonList(
                                        new ProduceRequestData.TopicProduceData()
                                                .setName(topicPartition.topic())
                                                .setPartitionData(Collections.singletonList(
                                                        new ProduceRequestData.PartitionProduceData()
                                                                .setIndex(topicPartition.partition()).setRecords(memoryRecords))))
                                .iterator()))
                        .setAcks((short) 1)
                        .setTimeoutMs(5000));

        var h = new RequestAndCompletionHandler(
                time.milliseconds(), tp.node().get(), requestBuilder, makeCompletionHandler());
        requestQueue.add(h);
    }

    private RequestCompletionHandler makeCompletionHandler() {
        return resp -> {
            log.info("Received response: {}", resp);
        };
    }


}
