import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaDispatcher<T> {

    private final KafkaProducer<String, Message<T>> producer;

    KafkaDispatcher() {
        this.producer = new KafkaProducer<>(properties());
    }

    private static Properties properties() {
        var props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
//        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        return props;
    }

    public void send(String topic, String key, CorrelationId correlationId, T payload) throws ExecutionException, InterruptedException {
        Future<RecordMetadata> future = sendAsync(topic, key, correlationId, payload);
        future.get();
    }

    public Future<RecordMetadata> sendAsync(String topic, String key, CorrelationId correlationId, T payload) {
        var value = new Message<>(correlationId, payload);
        var record = new ProducerRecord<>(topic, key, value);

        return producer.send(record, onSent());
    }


    private static Callback onSent() {
        return (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }

            System.out.println(data.topic() + ":::partition " + data.partition() + "/ offset " + data.offset() + "/ timestamp " + data.timestamp());
        };
    }
}
