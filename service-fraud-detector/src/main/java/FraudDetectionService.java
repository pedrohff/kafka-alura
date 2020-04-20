import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

public class FraudDetectionService {
    public static void main(String[] args) {
        var fraudService = new FraudDetectionService();
        var service = new KafkaService(FraudDetectionService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER", fraudService::parse, new HashMap<>());
        service.run();

    }

    private void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
//        System.out.println("----------");
//        System.out.println("Processing new order, checking for fraud");
//        System.out.println(record.key());
//        System.out.println(record.value());
//        System.out.println(record.partition());
//        System.out.println(record.offset());
//        System.out.println("Order processed");

        var message = record.value();
        var order = message.getPayload();
        if (isFraud(order)) {
            System.out.println("Order is a fraud!");
            orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(), message.getId().continueWith(FraudDetectionService.class.getSimpleName()), order);
        } else {
            System.out.println("Approved!");
            orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(), message.getId().continueWith(FraudDetectionService.class.getSimpleName()), order);
        }
        System.out.println(order);

    }

    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal(4500)) >= 0;
    }

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();
}
