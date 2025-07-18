package spaced_repetition.model;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class MessageSerde implements Serde<Message> {

    private static final String SEPARATOR = "```";
    private static final Serde<String> serdeString = Serdes.String();

    @Override
    public Serializer<Message> serializer() {
        return new MessageSerializer();
    }

    @Override
    public Deserializer<Message> deserializer() {
        return new MessageDeserializer();
    }

    public static class MessageSerializer implements Serializer<Message> {

        @Override
        public byte[] serialize(String topic, Message data) {
            if (data == null) {
                return null;
            }

            String asString = data.id() + SEPARATOR + data.question() + SEPARATOR + data.correctAnswers() + SEPARATOR
                    + data.answer();
            return serdeString.serializer().serialize(topic, asString);
        }
    }

    public static class MessageDeserializer implements Deserializer<Message> {
        @Override
        public Message deserialize(String topic, byte[] data) {
            String asString = serdeString.deserializer().deserialize(topic, data);
            if (asString == null) {
                return null;
            }

            try {
                String[] parts = asString.split(SEPARATOR);
                return new Message(parts[0], parts[1], Integer.parseInt(parts[2]), parts[3]);
            }
            catch (Exception e) {
                throw new SerializationException("Error decoding a Message from: " + asString);
            }
        }
    }

}
