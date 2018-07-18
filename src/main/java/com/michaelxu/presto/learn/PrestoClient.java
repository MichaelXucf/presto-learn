package com.michaelxu.presto.learn;

import com.facebook.presto.jdbc.PrestoDriver;

import java.sql.*;
import java.util.Properties;

public class PrestoClient {
    private static PrestoClient client;

    private static Connection conn;

    public static PrestoClient getInstance(String url, String user, String password) throws SQLException {
        if (client == null) {
            synchronized (PrestoClient.class){
                if (client == null) {
                    conn = DriverManager.getConnection(url, user, password);
                    client = new PrestoClient();
                }
            }
        }

        return client;
    }

    public static PrestoClient getInstance2(String url, String user, String password) throws SQLException {
        if (client == null) {
            synchronized (PrestoClient.class){
                if (client == null) {
                    PrestoDriver driver = new PrestoDriver();
                    Properties info = new Properties();
                    info.setProperty("user", user);
                    info.setProperty("password", "");
                    conn = driver.connect(url, info);
                    client = new PrestoClient();
                }
            }
        }

        return client;
    }

    public ResultSet executeQuery(String sql) throws SQLException {
        Statement statement = conn.createStatement();
        ResultSet result = statement.executeQuery(sql);
        return result;
    }

    public void close(){
        if(conn != null){
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws SQLException {
        test1();
    }

    public static void test1() throws SQLException {
        String url = "jdbc:presto://192.168.51.42:18080/hive/default?httpProxy=172.20.51.196:80";
        //PrestoClient client = PrestoClient.getInstance(url, "root", null);

        PrestoClient client = PrestoClient.getInstance2(url, "root", null);

        /*String sql = "select * from kylin_sales limit 10";
        ResultSet rs = client.executeQuery(sql);
        while(rs.next()){
            int id = rs.getInt(1);
            Date date = rs.getDate(2);
            String lstg_format_name = rs.getString(3);
            System.out.println(String.format("id:%d, date:%s, name:%s", id, date, lstg_format_name ));
        }*/

        String sql = "show tables";
        ResultSet rs = client.executeQuery(sql);
        while(rs.next()){
            String tableName = rs.getString(1);
            System.out.println(tableName);
        }

        rs.close();
        client.close();
    }





}
