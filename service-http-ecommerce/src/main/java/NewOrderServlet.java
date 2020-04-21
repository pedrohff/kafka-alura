import org.example.CorrelationId;
import org.example.dispatcher.KafkaDispatcher;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        var orderDispatcher = new KafkaDispatcher<Order>();

        var usermail = req.getParameter("email");
        var orderId = UUID.randomUUID().toString();
        var amount = req.getParameter("amount");
        var order = new Order(orderId, new BigDecimal(amount), usermail);
        try {
            orderDispatcher.send(
                    "ECOMMERCE_NEW_ORDER",
                    usermail,
                    new CorrelationId(NewOrderServlet.class.getSimpleName()),
                    order
            );
            System.out.println("New order successfully sent.");
            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println("New order successfully sent");
        } catch (ExecutionException e) {
            e.printStackTrace();
            throw new ServletException(e);
        } catch (InterruptedException e) {
            throw new ServletException(e);
        }


    }
}
