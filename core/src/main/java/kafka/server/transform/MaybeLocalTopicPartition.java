package kafka.server.transform;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import scala.Option;

import java.util.Objects;

public class MaybeLocalTopicPartition {
    private final Option<Node> maybeNode;
    private final TopicPartition topicPartition;

    public MaybeLocalTopicPartition(Option<Node> maybeNode, TopicPartition topicPartition) {
        this.maybeNode = maybeNode;
        this.topicPartition = topicPartition;
    }

    public boolean isLocal() {
        return maybeNode.isEmpty();
    }

    public Option<Node> node() {
        return maybeNode;
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MaybeLocalTopicPartition)) return false;
        MaybeLocalTopicPartition that = (MaybeLocalTopicPartition) o;
        return Objects.equals(maybeNode, that.maybeNode) && Objects.equals(topicPartition, that.topicPartition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maybeNode, topicPartition);
    }

    @Override
    public String toString() {
        return "MaybeLocalTopicPartition{" +
                "maybeNode=" + maybeNode +
                ", topicPartition=" + topicPartition +
                '}';
    }
}