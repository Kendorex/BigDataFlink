package lab;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Map;

public class Sale implements Serializable {
    public String eventId;
    public String sourceFile;
    public int sourceRow;
    public int sourceId;
    public Map<String, String> customer;
    public Map<String, String> seller;
    public Map<String, String> product;
    public Map<String, String> store;
    public Map<String, String> supplier;
    public String saleDate;
    public int quantity;
    public BigDecimal totalPrice;

    public Sale() {}
}
