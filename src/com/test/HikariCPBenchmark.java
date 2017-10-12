package com.test;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.UUID;

public class HikariCPBenchmark {

    private static DataSource datasource;

    public static DataSource getDataSource() {
        if (datasource == null) {
            HikariConfig config = new HikariConfig();
            //config.setDriverClassName("");
            config.setJdbcUrl("jdbc:postgresql://localhost:5432/oic");
            config.setUsername("oic");
            config.setPassword("oic");

            config.setMaximumPoolSize(10);
            config.setAutoCommit(false);
            //config.addDataSourceProperty("cachePrepStmts", "true");
            //config.addDataSourceProperty("prepStmtCacheSize", "250");
            //config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

            datasource = new HikariDataSource(config);
        }
        return datasource;
    }

    public static void runReadStaments(Connection connection) throws SQLException {
        PreparedStatement pstmt = connection.prepareStatement("SELECT * FROM users");
        int countResults = 0;
        ResultSet resultSet = pstmt.executeQuery();
        while (resultSet.next() && countResults < 10) {
            String username = resultSet.getString(1);
            countResults--;
        }
    }

    public static void runInsertStaments(Connection connection) throws SQLException {
        String query = "INSERT INTO users(username, password, enabled) VALUES (?, ?, ?);";
        PreparedStatement pstmt = connection.prepareStatement(query);
        pstmt.setString(1, UUID.randomUUID().toString());
        pstmt.setString(2, "test");
        pstmt.setBoolean(3, true);
        pstmt.executeUpdate();
        pstmt.close();

        connection.commit();
    }

    public static void clean(Connection connection) throws SQLException {
        Date start = new Date();
        String query = "DELETE FROM users WHERE password = ?;";
        PreparedStatement pstmt = connection.prepareStatement(query);
        pstmt.setString(1, "test");
        pstmt.executeUpdate();
        pstmt.close();

        connection.commit();
        Date end = new Date();
        System.out.println("Clean finished in " + Long.toString(end.getTime() - start.getTime()) + "milliseconds");
    }

    public static void main(String[] args) {

        Connection connection = null;
        long mean = 0;
        long totalTime = 0;

        try {
            DataSource dataSource = HikariCPBenchmark.getDataSource();
            for (int k = 0; k < 50; k++) {
                //System.out.println("Launching 100 selects staments and 100 inserts staments");
                connection = dataSource.getConnection();
                Date start = new Date();
                for (int i = 0; i < 100; i++) {
                    runReadStaments(connection);
                    runInsertStaments(connection);
                }
                Date end = new Date();
                long ellapsedTime = end.getTime() - start.getTime();
                if (mean == 0) {
                    mean = ellapsedTime;
                }
                totalTime += ellapsedTime;
                mean = (mean + ellapsedTime) / 2;
                //System.out.println("Finished in [" + Long.toString(end.getTime() - start.getTime()) + "] milliseconds");

                if (k % 10 == 0) {
                    System.out.println("Cleaning");
                    clean(connection);
                }
                connection.close();
            }
            System.out.println("The block mean time is " + mean);
            System.out.println("The total time is " + totalTime);
            System.out.println("Run 5000 select statements and 5000 insert statements");
            System.out.println((10000.0/totalTime) * 1000 + " statements per second");

        } catch (Exception e) {
            try {
                connection.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
            e.printStackTrace();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }

}
