import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.Message;
import org.example.consumer.ConsumerService;
import org.example.consumer.ServiceRunner;

public class EmailService implements ConsumerService<String> {
    public static void main(String[] args) {
        new ServiceRunner(EmailService::new).start(5);
    }

    public String getConsumerGroup() {
        return EmailService.class.getSimpleName();
    }

    public String getTopic() {
        return "ECOMMERCE_SEND_MAIL";
    }

    public void parse(ConsumerRecord<String, Message<String>> record) {
        System.out.println("----------");
        System.out.println("Sending e-mail");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try {
            Thread.sleep(5000);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Email sent");
    }


}
