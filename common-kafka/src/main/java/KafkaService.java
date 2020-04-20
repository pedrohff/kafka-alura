import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class KafkaService<T> {
    private final KafkaConsumer<String, Message<T>> consumer;
    private final ConsumerFunction parse;

    KafkaService(String groupId, String topic, ConsumerFunction<T> parse, Map<String, String> extraProps) {
        this(parse, groupId, extraProps);
        consumer.subscribe(Collections.singletonList(topic));
    }

    KafkaService(String groupId, Pattern pattern, ConsumerFunction<T> parse, Map<String, String> extraProps) {
        this(parse, groupId, extraProps);
        consumer.subscribe(pattern);
    }

    KafkaService(ConsumerFunction<T> parse, String groupId, Map<String, String> extraProps) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(properties(groupId, extraProps));
    }

    public void run() throws ExecutionException, InterruptedException {
        var deadLetter = new KafkaDispatcher<>();

        while (true) {

            var records = consumer.poll(Duration.ofMillis(100));

            if (!records.isEmpty()) {
                for (ConsumerRecord<String, Message<T>> record : records) {
                    try {
                        parse.consume(record);
                    } catch (Exception e) {
                        e.printStackTrace();
                        var message = record.value();
                        deadLetter.send(
                                "ECOMMERCE_DEADLETTER",
                                message.getId().toString(),
                                message.getId().continueWith("DeadLetter"),
                                new GsonSerializer<>().serialize("", message)
                        );
                    }
                }
            }
        }

    }

    private Properties properties(String groupId, Map<String, String> extraProps) {
        var props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        props.putAll(extraProps);
        return props;
    }
}
