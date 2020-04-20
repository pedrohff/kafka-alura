import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class CreateUserService {
    private final Connection connection;

    public CreateUserService() throws SQLException {
        String url = "jdbc:sqlite:target/users_database.db";
        this.connection = DriverManager.getConnection(url);
        try {
            connection.createStatement().execute("create table Users ( " +
                    "uuid varchar(200) primary key, " +
                    "email varchar(200))");

        } catch (SQLException exception) {
            exception.printStackTrace();
        }
    }

    public static void main(String[] args) throws SQLException {
        var userService = new CreateUserService();
        var service = new KafkaService(CreateUserService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER", userService::parse, new HashMap<>());
        service.run();

    }

    private void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException, SQLException {
//        System.out.println("----------");
//        System.out.println("Processing new order, checking for new user");
        System.out.println(record.value());
        var order = record.value().getPayload();
        if (isNewUser(order.getEmail())) {
            insertNewUser(order.getEmail());
        }
    }

    private void insertNewUser(String email) throws SQLException {
        var statement = connection.prepareStatement("insert into Users(uuid, email) values (?,?)");
        statement.setString(1, UUID.randomUUID().toString());
        statement.setString(2, email);
        statement.execute();
        System.out.println("Usuário uuid " + email + " criado");
    }

    private boolean isNewUser(String email) throws SQLException {
        var statement = connection.prepareStatement("select uuid from Users where email = ? limit 1");
        statement.setString(1, email);
        ResultSet resultSet = statement.executeQuery();
        return !resultSet.next();
    }

}
