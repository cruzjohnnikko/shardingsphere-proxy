package org.apache.shardingsphere.example.proxy.cdc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;

public final class CDCMain {
    
    private static final String URL = "jdbc:postgresql://localhost:5432/migration_ds_0";
    private static final String USER = System.getenv("CDC_DB_USER") != null ? System.getenv("CDC_DB_USER") : "postgres";
    private static final String PASSWORD = System.getenv("CDC_DB_PASSWORD") != null ? System.getenv("CDC_DB_PASSWORD") : "root";
    
    public static void main(final String[] args) {
        try (Connection connection = DriverManager.getConnection(URL, USER, PASSWORD)) {
            System.out.println("Successfully connected to the source database.");
            int orderId = 7; // Start from an ID that doesn't conflict with initial data
            while (true) {
                try {
                    String sql = "INSERT INTO t_order (order_id, user_id, status) VALUES (?, ?, ?)";
                    try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                        preparedStatement.setInt(1, orderId);
                        preparedStatement.setInt(2, ThreadLocalRandom.current().nextInt(1, 101));
                        preparedStatement.setString(3, "new");
                        preparedStatement.executeUpdate();
                    }
                    System.out.println("Inserted new order with order_id: " + orderId);
                    orderId++;
                    Thread.sleep(1000); // Wait for 1 second before inserting the next record
                } catch (SQLException | InterruptedException ex) {
                    System.err.println("An error occurred: " + ex.getMessage());
                    // Wait a bit before retrying
                    Thread.sleep(5000);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}