import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class LogService {
    public static void main(String[] args) {
        LogService logService = new LogService();
        var service = new KafkaService<String>(LogService.class.getSimpleName(), Pattern.compile("ECOMMERCE.*"), logService::parse, Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()));
        service.run();
    }

    private void parse(ConsumerRecord<String, Message<String>> record) {
//        System.out.println("----------");
        System.out.println("LOG: " + record.topic());
        System.out.println(record.key());
//        System.out.println(record.value());
//        System.out.println(record.partition());
//        System.out.println(record.offset());
    }
}