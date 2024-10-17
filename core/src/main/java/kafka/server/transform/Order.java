package kafka.server.transform;

import java.time.ZonedDateTime;

public class Order {
    ZonedDateTime date; double price; long volume;

    public Order(ZonedDateTime date, double price, long volume) {
        this.date = date;
        this.price = price;
        this.volume = volume;
    }
}
