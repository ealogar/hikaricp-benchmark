package com.test;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class HikariCPBenchmark {

    public static final Map<String, String> users;
    private static DataSource datasource;

    static {
        users = new HashMap<String, String>();
        users.put("monkey_user", "monkey_pass");
        users.put("user1", "pass");
        users.put("user2", "pass");
    }

    public static DataSource getDataSource(String user, int maxPool) {

        if (datasource == null) {
            HikariConfig config = new HikariConfig();
            //config.setDriverClassName("");
            config.setJdbcUrl("jdbc:postgresql://localhost:5430/oic");
            config.setUsername(user);
            config.setPassword(users.get(user));

            config.setMaximumPoolSize(maxPool);
            config.setAutoCommit(false);
            config.setConnectionTestQuery("select 1;");
            config.setConnectionTimeout(500);
            config.setValidationTimeout(500);
            //config.addDataSourceProperty("cachePrepStmts", "true");
            //config.addDataSourceProperty("prepStmtCacheSize", "250");
            //config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

            datasource = new HikariDataSource(config);
        }
        return datasource;
    }

    public static void runReads(DataSource dataSource, boolean verbose, int maxPool) {
        try {
            int counter = 0;
            System.out.println("Ask all connections from pool an run");
            Connection[] connections = new Connection[maxPool];
            while (true) {
                Date start = new Date();

                for (int i = 0; i < maxPool; i++) {
                    connections[i] = dataSource.getConnection();
                    for (int k = 0; k < 2000; k++) {
                        readStatment(connections[i], verbose);
                    }

                }
                Date end = new Date();
                System.out.println(maxPool * 2000 + "Reads finished in " + Long.toString(end.getTime() - start.getTime()) + "milliseconds.");
                System.out.println("Returning all connections to the pool and sleeping 2 seconds");
                for (int i = 0; i < maxPool; i++) {
                    connections[i].close();
                }
                Thread.currentThread().sleep(2000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void runInserts(DataSource dataSource) {
        try {
            int counter = 0;
            System.out.println("Ask a connection from pool an run");
            Connection connection = dataSource.getConnection();
            while (true) {

                if (counter > 10) {
                    System.out.println("Returning connection to pool and sleeping 10 seconds");
                    connection.close();
                    Thread.currentThread().sleep(200);
                    System.out.println("Ask a connection from pool an run inserts");
                    connection = dataSource.getConnection();
                    counter = 0;
                }
                Date start = new Date();
                insertStament(connection);
                Date end = new Date();
                counter++;
                System.out.println("Insert finished in " + Long.toString(end.getTime() - start.getTime()) + "milliseconds.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void readStatment(Connection connection, boolean verbose) throws SQLException {
        PreparedStatement pstmt = connection.prepareStatement("SELECT * FROM users");
        int countResults = 0;
        ResultSet resultSet = pstmt.executeQuery();
        while (resultSet.next() && countResults < 10) {
            String username = resultSet.getString(1);
            countResults--;
        }
    }

    public static void insertStament(Connection connection) throws SQLException {
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

    public static void runBenchmark(DataSource dataSource) {
        try {
            Connection connection = null;
            long mean = 0;
            long totalTime = 0;
            for (int k = 0; k < 50; k++) {
                //System.out.println("Launching 100 selects staments and 100 inserts staments");
                connection = dataSource.getConnection();
                Date start = new Date();
                for (int i = 0; i < 100; i++) {
                    readStatment(connection, false);
                    insertStament(connection);
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
                System.out.println("Returning connection to pool and asking new one");
                connection.close();
            }
            System.out.println("The block mean time is " + mean);
            System.out.println("The total time is " + totalTime);
            System.out.println("Run 5000 select statements and 5000 insert statements");
            System.out.println((10000.0 / totalTime) * 1000 + " statements per second");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        //if something happens the program will fail, not interested in anything else from now
        if (args.length != 3) {
            System.out.println("Usage: java -cp target/com.test-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.test.HikariCPBenchmark <mode> <user> <maxPool>");
            System.out.println("<mode> read|write|bench <user> monkey_user|user1|user2 <maxPool> int");
            System.exit(1);
        }
        String mode = args[0];
        String user = args[1];
        int maxPool = Integer.parseInt(args[2]);
        if (!users.containsKey(user)) {
            System.out.println("Invalid user: " + user + " monkey_user|user1|user2");
            System.exit(1);
        }
        DataSource dataSource = HikariCPBenchmark.getDataSource(user, maxPool);
        if (mode.equals("read")) {
            runReads(dataSource, true, maxPool);
        } else if (mode.equals("write")) {
            runInserts(dataSource);
        } else if (mode.equals("bench")) {
            runBenchmark(dataSource);
        } else {
            System.out.println("Invalid mode " + mode + " . read|write|bench allowed");
        }
        //runReads(dataSource, true);

    }

}
