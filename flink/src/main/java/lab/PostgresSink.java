package lab;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Collections;

public class PostgresSink extends RichSinkFunction<Sale> {
    private transient Connection connection;
    private transient Map<String, PreparedStatement> statements;

    @Override
    public void open(OpenContext context) throws Exception {
        connection = DriverManager.getConnection(
                System.getenv().getOrDefault("PG_URL", "jdbc:postgresql://postgres:5432/lab_db"),
                System.getenv().getOrDefault("PG_USER", "postgres"),
                System.getenv().getOrDefault("PG_PASSWORD", "postgres"));
        connection.setAutoCommit(false);
        statements = new HashMap<>();
    }

    @Override
    public void invoke(Sale sale, Context context) throws Exception {
        try {
            long customer = dimension("customer", sale.customer, sale.eventId);
            long seller = dimension("seller", sale.seller, sale.eventId);
            long product = dimension("product", sale.product, sale.eventId);
            long store = dimension("store", sale.store, null);
            long supplier = dimension("supplier", sale.supplier, null);
            LocalDate date = LocalDate.parse(sale.saleDate);
            int dateKey = date.getYear() * 10000 + date.getMonthValue() * 100 + date.getDayOfMonth();
            PreparedStatement dateStatement = statement("date", """
                    INSERT INTO dim_date (date_key, full_date, year, quarter, month, day, day_of_week)
                    VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (date_key) DO NOTHING
                    """);
            dateStatement.setInt(1, dateKey);
            dateStatement.setObject(2, date);
            dateStatement.setInt(3, date.getYear());
            dateStatement.setInt(4, (date.getMonthValue() - 1) / 3 + 1);
            dateStatement.setInt(5, date.getMonthValue());
            dateStatement.setInt(6, date.getDayOfMonth());
            dateStatement.setInt(7, date.getDayOfWeek().getValue());
            dateStatement.executeUpdate();
            PreparedStatement fact = statement("fact", """
                    INSERT INTO fact_sales (source_row_id, source_file, source_row, source_id,
                        customer_key, seller_key, product_key, store_key, supplier_key, date_key,
                        sale_quantity, sale_total_price)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (source_row_id) DO UPDATE SET
                        customer_key = EXCLUDED.customer_key, seller_key = EXCLUDED.seller_key,
                        product_key = EXCLUDED.product_key, store_key = EXCLUDED.store_key,
                        supplier_key = EXCLUDED.supplier_key, date_key = EXCLUDED.date_key,
                        sale_quantity = EXCLUDED.sale_quantity, sale_total_price = EXCLUDED.sale_total_price
                    """);
            fact.setString(1, sale.eventId);
            fact.setString(2, sale.sourceFile);
            fact.setInt(3, sale.sourceRow);
            fact.setInt(4, sale.sourceId);
            fact.setLong(5, customer);
            fact.setLong(6, seller);
            fact.setLong(7, product);
            fact.setLong(8, store);
            fact.setLong(9, supplier);
            fact.setInt(10, dateKey);
            fact.setInt(11, sale.quantity);
            fact.setBigDecimal(12, sale.totalPrice);
            fact.executeUpdate();
            connection.commit();
        } catch (Exception error) {
            try {
                connection.rollback();
            } catch (SQLException rollbackError) {
                error.addSuppressed(rollbackError);
            }
            throw error;
        }
    }

    private long dimension(String entity, Map<String, String> attributes, String eventId) throws Exception {
        Map<String, String> values = new LinkedHashMap<>();
        if (eventId != null) {
            values.put("source_row_id", eventId);
        }
        values.putAll(attributes);
        List<String> columns = new ArrayList<>(values.keySet());
        String conflict = eventId == null ? String.join(", ", columns) : "source_row_id";
        List<String> updates = new ArrayList<>();
        for (String column : attributes.keySet()) {
            updates.add(column + " = EXCLUDED." + column);
        }
        String sql = "INSERT INTO dim_" + entity + " (" + String.join(", ", columns)
                + ") VALUES (" + String.join(", ", Collections.nCopies(columns.size(), "?"))
                + ") ON CONFLICT (" + conflict + ") DO UPDATE SET " + String.join(", ", updates)
                + " RETURNING " + entity + "_key";
        PreparedStatement insert = statement(entity, sql);
        int index = 1;
        for (String value : values.values()) {
            insert.setObject(index++, value, Types.OTHER);
        }
        try (ResultSet result = insert.executeQuery()) {
            if (!result.next()) {
                throw new SQLException("Missing dimension key: " + entity);
            }
            return result.getLong(1);
        }
    }

    private PreparedStatement statement(String name, String sql) throws SQLException {
        if (!statements.containsKey(name)) {
            statements.put(name, connection.prepareStatement(sql));
        }
        return statements.get(name);
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            try {
                connection.rollback();
            } finally {
                connection.close();
            }
        }
    }
}
