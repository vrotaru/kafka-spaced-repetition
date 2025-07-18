package spaced_repetition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import spaced_repetition.model.Message;
import spaced_repetition.model.MessageSerde.MessageDeserializer;
import spaced_repetition.model.MessageSerde.MessageSerializer;

public class Spaced_List {
    private final static String APPLICATION_ID = Spaced_List.class.getName();

    private final static String TOPIC = "srs-topic";
    private final static String BOOTSTRAP_SERVER = "localhost:9092";

    private final static int MAX_ITEMS = 14;
    private final static long MAX_WAIT_MS = 200L;

    public static void main(String[] args) {
        var console = System.console();

        Callback callback = (metadata, exception) -> {
            if (exception != null) {
                console.format("Error sending message: %s", exception.getMessage());
                return;
            }
        };

        // Get the questions to review
        var toReview = new ArrayList<Message>();
        try (var consumer = new KafkaConsumer<String, Message>(consumerConfig())) {
            consumer.subscribe(List.of(TOPIC));

            var now = System.currentTimeMillis();
            var offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
            REVIEW: while (true) {
                var records = consumer.poll(Duration.ofMillis(1000));

                for (var entry : records) {
                    var message = entry.value();
                    if (message.correctAnswers() <= 2) {
                        toReview.add(message);
                    }

                    var partition = new TopicPartition(entry.topic(), entry.partition());
                    var offset = new OffsetAndMetadata(entry.offset() + 1);
                    offsets.put(partition, offset);

                    if (toReview.size() >= MAX_ITEMS) {
                        break REVIEW;
                    }
                }
                if (System.currentTimeMillis() - now >= MAX_WAIT_MS) {
                    break;
                }
            }
            if (!offsets.isEmpty()) {
                consumer.commitSync(offsets);
            }
        }

        // The review proper
        try (var producer = new KafkaProducer<String, Message>(producerConfig())) {
            for (var message : toReview) {
                console.readLine("Question: %s%n  (press <Enter> to continue)", message.question());
                var input = console.readLine("Answer  : %s%n  (Press 'y' and <Enter> if you knew the answer) ",
                        message.answer())
                        .toLowerCase();

                ProducerRecord<String, Message> producerRecord = null;
                if (!input.isBlank() & input.strip().startsWith("y")) {
                    producerRecord = new ProducerRecord<>(TOPIC, message.id(), message.up());
                }
                else {
                    producerRecord = new ProducerRecord<>(TOPIC, message.id(), message);
                }
                producer.send(producerRecord, callback);
            }

        }
    }

    private static Properties producerConfig() {
        Properties props = new Properties();
        // Producer configuration
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MessageSerializer.class.getName());

        // Optional configuration
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);

        return props;
    }

    private static Properties consumerConfig() {
        var props = new Properties();

        // Streams configuration
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MessageDeserializer.class.getName());

        props.put(ConsumerConfig.GROUP_ID_CONFIG, APPLICATION_ID);
        // Do not auto-commit
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return props;
    }

}
