package lab;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.OpenContext;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;
import java.util.LinkedHashMap;
import java.util.Map;

public class SaleMapper extends RichMapFunction<String, Sale> {
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("M/d/uuuu")
            .withResolverStyle(ResolverStyle.STRICT);
    private transient ObjectMapper mapper;

    @Override
    public void open(OpenContext context) {
        mapper = new ObjectMapper();
    }

    @Override
    public Sale map(String message) throws Exception {
        JsonNode row = mapper.readTree(message);
        Sale sale = new Sale();
        sale.sourceFile = required(row, "source_file");
        sale.sourceRow = Integer.parseInt(required(row, "source_row"));
        sale.eventId = required(row, "event_id");
        if (sale.sourceRow < 1 || !sale.eventId.equals(sale.sourceFile + ":" + sale.sourceRow)) {
            throw new IllegalArgumentException("Invalid event identity: " + sale.eventId);
        }
        sale.sourceId = Integer.parseInt(required(row, "id"));
        sale.saleDate = LocalDate.parse(required(row, "sale_date"), DATE).toString();
        sale.quantity = Integer.parseInt(required(row, "sale_quantity"));
        sale.totalPrice = new BigDecimal(required(row, "sale_total_price"))
                .setScale(2, RoundingMode.UNNECESSARY);
        if (sale.quantity <= 0 || sale.totalPrice.signum() < 0) {
            throw new IllegalArgumentException("Invalid sale measures: " + sale.eventId);
        }
        sale.customer = dimension(row, "customer", true,
                "first_name,last_name,age,email,country,postal_code,pet_type,pet_name,pet_breed");
        sale.seller = dimension(row, "seller", true,
                "first_name,last_name,email,country,postal_code");
        sale.product = dimension(row, "product", false,
                "name,category,price,quantity,weight,color,size,brand,material,description,rating,reviews,release_date,expiry_date");
        sale.product.put("source_product_id", text(row, "sale_product_id"));
        sale.product.put("pet_category", text(row, "pet_category"));
        for (String field : new String[]{"product_release_date", "product_expiry_date"}) {
            String value = sale.product.get(field);
            if (value != null) {
                sale.product.put(field, LocalDate.parse(value, DATE).toString());
            }
        }
        sale.store = dimension(row, "store", false, "name,location,city,state,country,phone,email");
        sale.supplier = dimension(row, "supplier", false, "name,contact,email,phone,address,city,country");
        return sale;
    }

    private static Map<String, String> dimension(JsonNode row, String entity,
                                                  boolean stripPrefix, String fields) {
        Map<String, String> values = new LinkedHashMap<>();
        if (stripPrefix) {
            values.put("source_" + entity + "_id", text(row, "sale_" + entity + "_id"));
        }
        for (String field : fields.split(",")) {
            String source = entity + "_" + field;
            values.put(stripPrefix ? field : source, text(row, source));
        }
        return values;
    }

    private static String text(JsonNode row, String field) {
        if (!row.has(field)) {
            throw new IllegalArgumentException("Missing field: " + field);
        }
        JsonNode value = row.get(field);
        if (value.isNull() || value.asText().isEmpty()) {
            return null;
        }
        if (!value.isValueNode()) {
            throw new IllegalArgumentException("Expected scalar field: " + field);
        }
        return value.asText();
    }

    private static String required(JsonNode row, String field) {
        String value = text(row, field);
        if (value == null) {
            throw new IllegalArgumentException("Empty required field: " + field);
        }
        return value;
    }
}
