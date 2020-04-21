import org.example.Message;
import org.example.consumer.KafkaService;
import org.example.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class BatchSendMessageService {
    private Connection connection;
    private final KafkaDispatcher userDispatcher = new KafkaDispatcher<User>();

    BatchSendMessageService() throws SQLException {
        String url = "jdbc:sqlite:target/users_database.db";
        try {
            this.connection = DriverManager.getConnection(url);
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
        if (this.connection != null) {

            try {
                connection.createStatement().execute("create table Users ( " +
                        "uuid varchar(200) primary key, " +
                        "email varchar(200))");

            } catch (java.sql.SQLException exception) {
                if (exception.getMessage().contains("table Users already exists")) {
                    exception.printStackTrace();
                } else {
                    throw exception;

                }

            }
        }
    }

    public static void main(String[] args) throws SQLException, ExecutionException, InterruptedException {
        var batchService = new BatchSendMessageService();
        var service = new KafkaService(
                BatchSendMessageService.class.getSimpleName(),
                "ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS",
                batchService::parse,
                new HashMap<>()
        );
        service.run();

    }


    private void parse(ConsumerRecord<String, Message<String>> record) {
        System.out.println("----------");
        System.out.println("Processing new batch");
        var message = record.value();
        var payload = message.getPayload();
        System.out.println("Topic: " + payload);

        try {
            for (User user : getAllUsers()) {
                userDispatcher.sendAsync(
                        payload, user.getUuid(),
                        message.getId().continueWith(BatchSendMessageService.class.getSimpleName()), user
                );
            }
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
    }

    private List<User> getAllUsers() throws SQLException {
        var results = connection.prepareStatement("select uuid from Users").executeQuery();
        List<User> users = new ArrayList<User>();
        while (results.next()) {
            users.add(new User(results.getString(1)));
        }
        return users;
    }


}

