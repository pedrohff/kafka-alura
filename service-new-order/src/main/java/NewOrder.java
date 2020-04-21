import org.example.CorrelationId;
import org.example.dispatcher.KafkaDispatcher;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrder {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var orderDispatcher = new KafkaDispatcher<Order>();


        var usermail = Math.random() + "@email.com";
        for (int i = 0; i < 10; i++) {
            var orderId = UUID.randomUUID().toString();
            var amount = Math.random() * 5000 + 1;
            var order = new Order(orderId, new BigDecimal(amount), usermail);
            orderDispatcher.send(
                    "ECOMMERCE_NEW_ORDER",
                    usermail,
                    new CorrelationId(NewOrder.class.getSimpleName()),
                    order
            );

        }

    }


}
