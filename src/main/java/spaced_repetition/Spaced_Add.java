package spaced_repetition;

import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import spaced_repetition.model.Message;
import spaced_repetition.model.MessageSerde.MessageSerializer;

public class Spaced_Add {
    private final static String TOPIC = "srs-topic";
    private final static String BOOTSTRAP_SERVER = "localhost:9092";

    public static void main(String[] args) {
        var console = System.console();

        var question = console.readLine("Enter the question and press <Enter>%n : ");
        var answer = console.readLine("Enter the answer and press <Enter>%n : ");

        var props = new Properties();
        // Producer configuration
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MessageSerializer.class.getName());

        // Optional configuration
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);

        Callback callback = (metadata, exception) -> {
            if (exception != null) {
                console.printf("Error sending message: %s%n", exception.getMessage());
                return;
            }
            console.printf("Question & Answer added%n");
        };

        try (var producer = new KafkaProducer<String, Message>(props)) {
            var id = UUID.randomUUID().toString();
            var message = new Message(id, question, 0, answer);
            var record = new ProducerRecord<>(TOPIC, id, message);
            producer.send(record, callback);
        }
    }
}
