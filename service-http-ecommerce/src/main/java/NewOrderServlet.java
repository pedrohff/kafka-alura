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
        var emailDispatcher = new KafkaDispatcher<String>();


        var usermail = req.getParameter("email");
        var orderId = UUID.randomUUID().toString();
        var amount = req.getParameter("amount");
        var order = new Order(orderId, new BigDecimal(amount), usermail);
        var email = "Thank you for your order! We are processing your order!";
        try {
            orderDispatcher.send("ECOMMERCE_NEW_ORDER", usermail, new CorrelationId(NewOrderServlet.class.getSimpleName()), order);
            emailDispatcher.send("ECOMMERCE_SEND_MAIL", email, new CorrelationId(NewOrderServlet.class.getSimpleName()), email);
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
