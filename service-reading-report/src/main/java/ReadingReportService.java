import org.example.Message;
import org.example.consumer.ConsumerService;
import org.example.consumer.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.consumer.ServiceRunner;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

public class ReadingReportService implements ConsumerService<User> {
    private final Path SOURCE = new File("src/main/resources/report.txt").toPath();

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        new ServiceRunner(ReadingReportService::new).start(5);
    }


    @Override
    public String getConsumerGroup() {
        return ReadingReportService.class.getSimpleName();
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_USER_GENERATE_READING_REPORT";
    }

    @Override
    public void parse(ConsumerRecord<String, Message<User>> record) {
        System.out.println("----------");
        System.out.println("Processing report for " + record.value());

        var message = record.value();
        var user = message.getPayload();
        var target = new File(user.getReportPath());
        try {
            IO.copyTo(SOURCE, target);
            IO.append(target, "Created for " + user.getUuid());
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("File created: " + target.getAbsolutePath());
    }
}
