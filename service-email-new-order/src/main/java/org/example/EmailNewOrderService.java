package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.consumer.KafkaService;
import org.example.dispatcher.KafkaDispatcher;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

public class EmailNewOrderService {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var fraudService = new EmailNewOrderService();
        var service = new KafkaService(
                EmailNewOrderService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudService::parse,
                new HashMap<>()
        );
        service.run();

    }

    private void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
        System.out.println("----------");
        System.out.println("Processing new order, preparing email");

        var emailCode = "Thank you for your order! We are processing your order!";
        Order order = record.value().getPayload();
        CorrelationId corrId = record.value().getId();
        emailDispatcher.send(
                "ECOMMERCE_SEND_EMAIL",
                order.getEmail(),
                corrId.continueWith(EmailNewOrderService.class.getSimpleName()),
                emailCode
        );


    }

    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal(4500)) >= 0;
    }

    private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>();
}
