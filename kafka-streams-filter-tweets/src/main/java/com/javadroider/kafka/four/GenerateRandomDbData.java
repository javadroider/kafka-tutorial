package com.javadroider.kafka.four;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

public class GenerateRandomDbData {

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        new GenerateRandomDbData().start();
    }

    // JDBC driver name and database URL
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_URL = "jdbc:mysql://localhost/test";

    //  Database credentials
    private static final String USER = "root";
    private static final String PASS = "root";

    private void start() throws SQLException, ClassNotFoundException {

        Class.forName("com.mysql.jdbc.Driver");
        createTable();

        for (int i = 0; i < 3000000; ) {
            final int start = i + 1;
            final int end = i + 100000;
            System.out.println(start + " " + end + " " + (end - start));
            new Thread(() -> start(start, end)).start();
            i = i + 100000;
        }
    }

    private void createTable() {
        String sql = "CREATE TABLE orders \n" +
                "(\n" +
                "  id bigint(20) NOT NULL AUTO_INCREMENT,\n" +
                "  total int(11) DEFAULT NULL,\n" +
                "  user_id int(11) DEFAULT NULL,\n" +
                "  created_at   timestamp  NULL DEFAULT CURRENT_TIMESTAMP,\n" +
                "  PRIMARY KEY (id)\n" +
                ")\n";

        Connection conn = null;
        Statement stmt = null;
        try {
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            stmt = conn.createStatement();
            stmt.executeUpdate(sql);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close(conn);
            close(stmt);

        }
    }

    private void close(Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (Exception e) {

            }
        }
    }

    private void close(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {

            }
        }
    }

    private void start(int idStart, int idEnd) {

        Connection conn = null;
        Statement stmt = null;
        try {
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            stmt = conn.createStatement();

            for (int i = idStart; i <= idEnd; i++) {
                int total = getRandomNumberInRange(1, 3000);
                int userId = getRandomNumberInRange(1, 200);
                int year = getRandomNumberInRange(2015, 2019);
                int month = getRandomNumberInRange(1, 12);
                int day = getRandomNumberInRange(1, 28);
                int hour = getRandomNumberInRange(1, 23);
                int minute = getRandomNumberInRange(1, 59);
                int secs = getRandomNumberInRange(1, 59);

                StringBuilder sb = new StringBuilder();
                sb.append("insert into orders values(")
                        .append(i)
                        .append(",")
                        .append(total)
                        .append(",")
                        .append(userId)
                        .append(",")
                        .append("'")
                        .append(year)
                        .append("-")
                        .append(month)
                        .append("-")
                        .append(day)
                        .append(" ")
                        .append(hour)
                        .append(":")
                        .append(minute)
                        .append(":")
                        .append(secs)
                        .append("');");
                System.out.println(Thread.currentThread().getName() + " ->> " + sb);
                stmt.executeUpdate(sb.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close(conn);
            close(stmt);
        }
    }

    private int getRandomNumberInRange(int min, int max) {
        return new Random().nextInt((max - min) + 1) + min;
    }

}
